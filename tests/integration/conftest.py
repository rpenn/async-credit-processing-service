"""Real MongoDB (single-node replica set) and RabbitMQ, started once per test run.

Uses testcontainers' generic DockerContainer: its MongoDB module cannot start a replica set
(needed for transactions), and its RabbitMQ module would add `pika` just for a readiness check.
"""

import time
import uuid
from collections.abc import Iterator

import pytest
from pydantic import SecretStr
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from credit_service.mongo import Document
from credit_service.settings import Settings

# Keep in step with compose.yaml.
MONGO_IMAGE = "mongo:8.0.34"
RABBITMQ_IMAGE = "rabbitmq:4.3.7-management"


@pytest.fixture(scope="session")
def mongo_server_uri() -> Iterator[str]:
    container = (
        DockerContainer(MONGO_IMAGE)
        .with_command("--replSet rs0 --bind_ip_all")
        .with_exposed_ports(27017)
        .waiting_for(LogMessageWaitStrategy("Waiting for connections"))
    )
    with container:
        container.exec(
            [
                "mongosh",
                "--quiet",
                "--eval",
                "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})",
            ]
        )
        host = container.get_container_host_ip()
        port = container.get_exposed_port(27017)
        uri = f"mongodb://{host}:{port}"
        _wait_until_primary(uri)
        yield uri


def _wait_until_primary(uri: str, timeout_seconds: float = 30) -> None:
    deadline = time.monotonic() + timeout_seconds
    with MongoClient[Document](uri, directConnection=True) as client:
        while time.monotonic() < deadline:
            try:
                if client.admin.command("hello").get("isWritablePrimary"):
                    return
            except PyMongoError:
                pass
            time.sleep(0.2)
    raise TimeoutError("MongoDB replica set did not elect a primary")


@pytest.fixture(scope="session")
def broker_url() -> Iterator[str]:
    container = (
        DockerContainer(RABBITMQ_IMAGE)
        # The default guest user may only connect from localhost inside the container.
        .with_env("RABBITMQ_DEFAULT_USER", "test")
        .with_env("RABBITMQ_DEFAULT_PASS", "test")
        .with_exposed_ports(5672, 15672)
        .waiting_for(LogMessageWaitStrategy("Server startup complete"))
    )
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        yield f"amqp://test:test@{host}:{port}//"


@pytest.fixture
def mongo_uri(mongo_server_uri: str) -> Iterator[str]:
    """A fresh, uniquely named database per test, dropped afterwards."""
    name = f"test_{uuid.uuid4().hex}"
    yield f"{mongo_server_uri}/{name}?directConnection=true"
    with MongoClient[Document](mongo_server_uri, directConnection=True) as client:
        client.drop_database(name)


@pytest.fixture
def settings(mongo_uri: str, broker_url: str) -> Settings:
    return Settings(mongo_uri=SecretStr(mongo_uri), broker_url=SecretStr(broker_url))


@pytest.fixture
def mongo_client(mongo_uri: str) -> Iterator[MongoClient[Document]]:
    with MongoClient[Document](mongo_uri, tz_aware=True) as client:
        yield client


@pytest.fixture
def db(mongo_client: MongoClient[Document]) -> Database[Document]:
    return mongo_client.get_default_database()
