"""Guarantees the database enforces that no API or worker test exercises directly.

Applying credits, the transactional write, and status derivation are proven end to end by
the API (Step 3) and worker (Step 4) tests.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from credit_service.api import repository as api_repo
from credit_service.mongo import CREDITS, USERS, Document, ensure_indexes
from credit_service.worker import repository as worker_repo

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def test_idempotency_key_is_unique(db: Database[Document]) -> None:
    ensure_indexes(db)
    db[CREDITS].insert_one(api_repo.new_credit("u_1", Decimal("1.00"), "key-1", NOW))

    with pytest.raises(DuplicateKeyError):
        db[CREDITS].insert_one(api_repo.new_credit("u_1", Decimal("1.00"), "key-1", NOW))


def test_release_never_drives_pending_credits_negative(
    mongo_client: MongoClient[Document], db: Database[Document]
) -> None:
    db[USERS].insert_one({"user_id": "u_1", "pending_credits": 0})

    with mongo_client.start_session() as session, pytest.raises(RuntimeError):
        worker_repo.release_user(db, session, "u_1", Decimal("1.00"), NOW)
