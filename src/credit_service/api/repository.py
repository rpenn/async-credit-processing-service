"""Async data access for the API. Functions that write take the transaction's session."""

import uuid
from datetime import datetime
from decimal import Decimal

from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.database import AsyncDatabase

from credit_service.models import CreditResponse, CreditStatus, UserStatus
from credit_service.money import from_decimal128, to_decimal128
from credit_service.mongo import CREDITS, USERS, Document


def new_credit(user_id: str, amount: Decimal, idempotency_key: str, now: datetime) -> Document:
    return {
        "_id": str(uuid.uuid4()),  # the credit_id returned to the client
        "user_id": user_id,
        "amount": to_decimal128(amount),
        "status": CreditStatus.PENDING,
        "idempotency_key": idempotency_key,
        "attempts": 0,
        "created_at": now,
        "updated_at": now,
    }


async def reserve_user(
    db: AsyncDatabase[Document], session: AsyncClientSession, user_id: str
) -> bool:
    """Count one more in-flight credit for the user. False if the user does not exist."""
    result = await db[USERS].update_one(
        {"user_id": user_id},
        {"$inc": {"pending_credits": 1}, "$set": {"status": UserStatus.PROCESSING}},
        session=session,
    )
    return result.matched_count == 1


async def insert_credit(
    db: AsyncDatabase[Document], session: AsyncClientSession, credit: Document
) -> None:
    """Raises DuplicateKeyError if the Idempotency-Key was already used."""
    await db[CREDITS].insert_one(credit, session=session)


async def find_credit(db: AsyncDatabase[Document], credit_id: str) -> Document | None:
    return await db[CREDITS].find_one({"_id": credit_id})


async def find_credit_by_idempotency_key(
    db: AsyncDatabase[Document], idempotency_key: str
) -> Document | None:
    return await db[CREDITS].find_one({"idempotency_key": idempotency_key})


def to_response(credit: Document) -> CreditResponse:
    return CreditResponse(
        credit_id=credit["_id"],
        user_id=credit["user_id"],
        amount=from_decimal128(credit["amount"]),
        status=credit["status"],
        created_at=credit["created_at"],
        updated_at=credit["updated_at"],
    )
