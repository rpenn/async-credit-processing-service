"""Chaos tests run against a real Compose stack, started here and removed afterwards.

Run them with `uv run pytest -m chaos` (the default run skips them). Needs Docker and a .env
file (cp .env.example .env).
"""

import uuid
from collections.abc import Iterator

import httpx2
import pytest
from pymongo import MongoClient
from pymongo.database import Database

from credit_service.mongo import USERS, Document
from tests.chaos.harness import (
    API_URL,
    MONGO_URI,
    RABBITMQ_API_URL,
    TOXIPROXY_URL,
    RabbitMQ,
    RabbitMQLogin,
    Toxiproxy,
    compose,
)


@pytest.fixture(scope="session", autouse=True)
def stack() -> Iterator[None]:
    # Beat is left out: tests run the reconciler themselves (harness.run_reconciler).
    compose("up", "--detach", "--build", "--wait", "api", "worker")
    yield
    compose("down", "--volumes")


@pytest.fixture(scope="session")
def toxiproxy() -> Iterator[Toxiproxy]:
    with httpx2.Client(base_url=TOXIPROXY_URL) as client:
        yield Toxiproxy(client)


@pytest.fixture(autouse=True)
def _recover(toxiproxy: Toxiproxy) -> Iterator[None]:
    """Each test starts with every link healthy and the worker running, whatever came before."""
    toxiproxy.heal()
    compose("start", "worker")
    yield
    toxiproxy.heal()


@pytest.fixture(scope="session")
def rabbitmq() -> Iterator[RabbitMQ]:
    login = RabbitMQLogin()
    auth = (login.rabbitmq_user, login.rabbitmq_password)
    with httpx2.Client(base_url=RABBITMQ_API_URL, auth=auth) as client:
        yield RabbitMQ(client)


@pytest.fixture
def api() -> Iterator[httpx2.Client]:
    with httpx2.Client(base_url=API_URL, timeout=60) as client:
        yield client


@pytest.fixture(scope="session")
def db() -> Iterator[Database[Document]]:
    with MongoClient[Document](MONGO_URI, tz_aware=True) as client:
        yield client.get_default_database()


@pytest.fixture
def user_id(db: Database[Document]) -> str:
    """A new user per test, so tests never see each other's balances."""
    user_id = f"u_{uuid.uuid4().hex[:8]}"
    db[USERS].insert_one({"user_id": user_id, "pending_credits": 0})
    return user_id
