"""Sync data access for the worker. Every write runs inside the caller's transaction."""

from datetime import datetime
from decimal import Decimal

from pymongo import ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.database import Database

from credit_service.models import CreditStatus, UserStatus
from credit_service.money import to_decimal128
from credit_service.mongo import CREDITS, USERS, Document


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
