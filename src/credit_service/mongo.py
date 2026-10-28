"""MongoDB clients and indexes shared by the API (async) and the worker (sync).

The database name comes from the connection string path, e.g. mongodb://host/credit_service.
"""

from typing import Any

from pymongo import ASCENDING, AsyncMongoClient, IndexModel, MongoClient
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.database import Database

from credit_service.settings import Settings

type Document = dict[str, Any]

USERS = "users"
CREDITS = "credits"

USER_INDEXES = [IndexModel("user_id", unique=True, name="user_id_unique")]

CREDIT_INDEXES = [
    # Partial (not sparse, per the MongoDB docs): only credits sent with an Idempotency-Key
    # are indexed, so any number of credits without a key can coexist.
    IndexModel(
        "idempotency_key",
        unique=True,
        partialFilterExpression={"idempotency_key": {"$type": "string"}},
        name="idempotency_key_unique",
    ),
    # The reconciler looks for credits still pending after a threshold.
    IndexModel([("status", ASCENDING), ("updated_at", ASCENDING)], name="status_updated_at"),
]


def create_async_client(settings: Settings) -> AsyncMongoClient[Document]:
    return AsyncMongoClient(settings.mongo_uri.get_secret_value(), tz_aware=True)


def create_sync_client(settings: Settings) -> MongoClient[Document]:
    return MongoClient(settings.mongo_uri.get_secret_value(), tz_aware=True)


async def ensure_indexes_async(db: AsyncDatabase[Document]) -> None:
    await db[USERS].create_indexes(USER_INDEXES)
    await db[CREDITS].create_indexes(CREDIT_INDEXES)


def ensure_indexes(db: Database[Document]) -> None:
    db[USERS].create_indexes(USER_INDEXES)
    db[CREDITS].create_indexes(CREDIT_INDEXES)
