"""Sync data access for the worker.

Writes that touch more than one document run inside the caller's transaction (they take its
session). The reconciler's single-document writes do not need one.
"""

from datetime import datetime
from decimal import Decimal

from pymongo import ASCENDING, ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.database import Database

from credit_service.models import CreditStatus, UserStatus
from credit_service.money import to_decimal128
from credit_service.mongo import CREDITS, USERS, Document

# Bounds one reconciler run; whatever is left waits for the next run.
RECONCILE_BATCH_SIZE = 500


def find_stale_pending_credits(db: Database[Document], before: datetime) -> list[Document]:
    """Pending credits last touched before `before`, oldest first (uses status_updated_at)."""
    return (
        db[CREDITS]
        .find(
            {"status": CreditStatus.PENDING, "updated_at": {"$lt": before}},
            {"_id": 1, "updated_at": 1, "attempts": 1},
        )
        .sort("updated_at", ASCENDING)
        .limit(RECONCILE_BATCH_SIZE)
        .to_list()
    )


def claim_for_retry(db: Database[Document], credit: Document, now: datetime) -> bool:
    """Count one more reconciler attempt. False if something touched the credit meanwhile.

    Matching on the old updated_at makes this a compare-and-set: if two reconciler runs
    overlap, only one claims the credit, so `attempts` is never counted twice.
    """
    result = db[CREDITS].update_one(
        {"_id": credit["_id"], "status": CreditStatus.PENDING, "updated_at": credit["updated_at"]},
        {"$inc": {"attempts": 1}, "$set": {"updated_at": now}},
    )
    return result.modified_count == 1


def find_credit(db: Database[Document], session: ClientSession, credit_id: str) -> Document | None:
    return db[CREDITS].find_one({"_id": credit_id}, session=session)


def finish_pending_credit(
    db: Database[Document],
    session: ClientSession,
    credit_id: str,
    status: CreditStatus,
    now: datetime,
) -> Document | None:
    """Move a credit from pending to `status` (applied or failed).

    Returns the credit, or None if it was not pending, i.e. another attempt already finished
    it. The status condition in the filter is what makes a redelivered task a no-op.
    """
    return db[CREDITS].find_one_and_update(
        {"_id": credit_id, "status": CreditStatus.PENDING},
        {"$set": {"status": status, "updated_at": now}},
        session=session,
    )


def release_user(
    db: Database[Document],
    session: ClientSession,
    user_id: str,
    balance_change: Decimal | None,
) -> None:
    """One in-flight credit finished: add it to the balance (if applied) and derive status.

    `$inc` creates `balance` if it does not exist yet. The user is `processed` only when no
    other credit is still in flight.
    """
    increments: Document = {"pending_credits": -1}
    if balance_change is not None:
        increments["balance"] = to_decimal128(balance_change)

    user = db[USERS].find_one_and_update(
        # pending_credits > 0 guards the counter: it can never go negative.
        {"user_id": user_id, "pending_credits": {"$gt": 0}},
        {"$inc": increments},
        return_document=ReturnDocument.AFTER,
        session=session,
    )
    if user is None:
        raise RuntimeError(f"user {user_id} has no pending credits to release")

    status = UserStatus.PROCESSED if user["pending_credits"] == 0 else UserStatus.PROCESSING
    db[USERS].update_one({"user_id": user_id}, {"$set": {"status": status}}, session=session)
