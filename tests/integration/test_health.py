from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from credit_service.api.main import create_app
from credit_service.settings import Settings

# Port 1 is never listening, so both dependencies are unreachable.
UNREACHABLE = Settings(
    mongo_uri=SecretStr("mongodb://127.0.0.1:1/"),
    broker_url=SecretStr("amqp://guest:guest@127.0.0.1:1//"),
)


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(create_app(UNREACHABLE)) as client:
        yield client


def test_live_does_not_depend_on_mongodb_or_broker(client: TestClient) -> None:
    response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_returns_503_when_dependencies_are_down(client: TestClient) -> None:
    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "unavailable",
        "checks": {"mongodb": "error", "broker": "error"},
    }
