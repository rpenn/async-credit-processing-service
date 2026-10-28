import logging

import pymongo
from celery import Task
from celery.exceptions import Reject
from pymongo.client_session import ClientSession
from pymongo.errors import ConnectionFailure, PyMongoError

from credit_service.messaging import APPLY_CREDIT_TASK
from credit_service.models import CreditStatus
from credit_service.money import from_decimal128
from credit_service.mongo import utc_now
from credit_service.worker import repository
from credit_service.worker.app import app
from credit_service.worker.database import get_client, get_database

logger = logging.getLogger(__name__)

# Bounds one attempt, including with_transaction's own retries (otherwise up to 120 s).
ATTEMPT_TIMEOUT_SECONDS = 10
# Retries after transient errors, waiting 1, 2, 4, 8, then 16 seconds (31 s in total).
MAX_RETRIES = 5


class UnknownCreditError(Exception):
    pass


@app.task(name=APPLY_CREDIT_TASK, bind=True, max_retries=MAX_RETRIES)
# "Task[...]" is quoted because Task is generic only in the type stubs, not at runtime.
def apply_credit(self: "Task[[str], None]", credit_id: str) -> None:
    """Add one pending credit to the user's balance, exactly once.

    The message carries only the credit_id; MongoDB is the source of truth for the amount.
    A redelivered or re-enqueued message finds the credit no longer pending and does nothing.
    """
    if not isinstance(credit_id, str):
        raise Reject("credit_id must be a string", requeue=False)  # to the dead-letter queue
    try:
        _finish(credit_id, CreditStatus.APPLIED)
    except UnknownCreditError:
        raise Reject(f"unknown credit {credit_id}", requeue=False) from None
    except PyMongoError as exc:
        if not _is_transient(exc):
            raise  # rejected to the dead-letter queue (task_acks_on_failure_or_timeout=False)
        if self.request.retries < MAX_RETRIES:
            # Whole seconds: native delayed delivery truncates fractions.
            raise self.retry(exc=exc, countdown=2**self.request.retries) from exc
        logger.error("credit %s failed after %d retries", credit_id, self.request.retries)
        _finish(credit_id, CreditStatus.FAILED)


def _finish(credit_id: str, status: CreditStatus) -> None:
    """In one transaction, move the credit out of pending and release the user's reservation.

    Only an applied credit changes the balance. Either way the user's pending_credits goes
    down by one, and the user becomes `processed` when it reaches zero.
    """
    db = get_database()

    # with_transaction reruns this on transient errors, so it only touches the database.
    def finish(session: ClientSession) -> None:
        credit = repository.finish_pending_credit(db, session, credit_id, status, utc_now())
        if credit is None:
            if repository.find_credit(db, session, credit_id) is None:
                raise UnknownCreditError(credit_id)
            return  # already applied or failed by an earlier delivery
        amount = from_decimal128(credit["amount"]) if status is CreditStatus.APPLIED else None
        repository.release_user(db, session, credit["user_id"], amount)

    with pymongo.timeout(ATTEMPT_TIMEOUT_SECONDS), get_client().start_session() as session:
        session.with_transaction(finish)


def _is_transient(exc: PyMongoError) -> bool:
    """Errors worth retrying later: MongoDB unreachable, too slow, or a write conflict."""
    return (
        isinstance(exc, ConnectionFailure)
        or exc.timeout
        or exc.has_error_label("TransientTransactionError")
    )
