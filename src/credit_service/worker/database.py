"""One MongoClient per worker process.

Celery's prefork pool forks child processes, and a MongoClient must not be shared across a
fork. Tasks only run in the children, so creating the client lazily on first use means each
child builds its own, after the fork.
"""

from functools import cache
from typing import Any

from celery.signals import worker_process_shutdown
from pymongo import MongoClient
from pymongo.database import Database

from credit_service.mongo import Document, create_sync_client
from credit_service.settings import Settings


@cache
def _client() -> MongoClient[Document]:
    return create_sync_client(Settings())


def get_client() -> MongoClient[Document]:
    return _client()


def get_database() -> Database[Document]:
    return _client().get_default_database()


@worker_process_shutdown.connect
def _close_client(**_: Any) -> None:
    if _client.cache_info().currsize:
        _client().close()
