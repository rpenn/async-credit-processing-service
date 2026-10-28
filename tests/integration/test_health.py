from fastapi.testclient import TestClient
from pydantic import SecretStr

from credit_service.api.main import create_app
from credit_service.settings import Settings


def test_live_and_ready_with_real_dependencies(settings: Settings) -> None:
    with TestClient(create_app(settings)) as client:
        live = client.get("/health/live")
        ready = client.get("/health/ready")

    assert live.status_code == 200
    assert ready.status_code == 200
    assert ready.json() == {"status": "ok", "checks": {"mongodb": "ok", "broker": "ok"}}


def test_ready_returns_503_when_broker_is_unreachable(settings: Settings) -> None:
    # Port 1 is never listening.
    broken = settings.model_copy(update={"broker_url": SecretStr("amqp://test:test@127.0.0.1:1//")})

    with TestClient(create_app(broken)) as client:
        live = client.get("/health/live")
        ready = client.get("/health/ready")

    assert live.status_code == 200
    assert ready.status_code == 503
    assert ready.json() == {
        "status": "unavailable",
        "checks": {"mongodb": "ok", "broker": "error"},
    }
