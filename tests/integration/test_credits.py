import logging
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from bson.decimal128 import Decimal128
from fastapi.testclient import TestClient
from pydantic import SecretStr
from pymongo.database import Database

from credit_service.api.credits import IDEMPOTENCY_KEY_REUSED
from credit_service.api.main import create_app
from credit_service.messaging import APPLY_CREDIT_TASK, create_celery_app
from credit_service.mongo import CREDITS, USERS, Document
from credit_service.settings import Settings

USER = "u_1"
PROBLEM_JSON = "application/problem+json"

type TakeTasks = Callable[[], list[tuple[str, list[Any]]]]


@pytest.fixture
def user(db: Database[Document]) -> None:
    db[USERS].insert_one({"user_id": USER, "pending_credits": 0})


@pytest.fixture
def client(settings: Settings, user: None) -> Iterator[TestClient]:
    with TestClient(create_app(settings)) as client:
        yield client


@pytest.fixture
def published_tasks(settings: Settings) -> TakeTasks:
    """Returns a function that takes every task off the queue the API publishes to.

    The queue is emptied first, because all tests share one broker.
    """
    celery = create_celery_app(settings)
    queue = celery.amqp.queues[celery.conf.task_default_queue]

    def take_all() -> list[tuple[str, list[Any]]]:
        tasks = []
        with celery.connection_for_read() as connection:
            bound = queue(connection.default_channel)
            bound.declare()
            while (message := bound.get(no_ack=True)) is not None:
                args, _kwargs, _embed = message.payload
                tasks.append((message.headers["task"], args))
        return tasks

    take_all()
    return take_all


def _user(db: Database[Document]) -> Document | None:
    return db[USERS].find_one({"user_id": USER}, {"_id": 0})


def test_credit_is_stored_reserved_and_published(
    client: TestClient, db: Database[Document], published_tasks: TakeTasks
) -> None:
    response = client.post("/credits", json={"user_id": USER, "amount": "150.00"})

    assert response.status_code == 202
    body = response.json()
    assert body | {"credit_id": "", "created_at": "", "updated_at": ""} == {
        "credit_id": "",
        "user_id": USER,
        "amount": "150.00",
        "status": "pending",
        "created_at": "",
        "updated_at": "",
    }
    assert response.headers["location"] == f"/credits/{body['credit_id']}"
    assert client.get(response.headers["location"]).json() == body
    credit = db[CREDITS].find_one({"_id": body["credit_id"]})
    assert credit is not None
    assert credit["amount"] == Decimal128("150.00")
    assert _user(db) == {"user_id": USER, "pending_credits": 1, "status": "processing"}
    assert published_tasks() == [(APPLY_CREDIT_TASK, [body["credit_id"]])]


@pytest.mark.parametrize(
    ("content", "headers", "error_type", "echoed_input"),
    [
        # Above the configurable maximum (default 1000000.00).
        ('{"user_id": "u_1", "amount": "1000000.01"}', {}, "less_than_equal", "1000000.01"),
        # Parsed as a float this would become 10.0 and pass. As a Decimal it is rejected,
        # and the error echoes the exact number back.
        (
            '{"user_id": "u_1", "amount": 10.000000000000000001}',
            {},
            "decimal_max_digits",
            "10.000000000000000001",
        ),
        ('{"user_id": "u_1", "amount": "1.00"}', {"Idempotency-Key": '"a'}, None, '"a'),
    ],
    ids=["above-maximum", "no-float-rounding", "malformed-idempotency-key"],
)
def test_invalid_requests_are_rejected_before_anything_is_stored(
    client: TestClient,
    db: Database[Document],
    published_tasks: TakeTasks,
    content: str,
    headers: dict[str, str],
    error_type: str | None,
    echoed_input: str,
) -> None:
    response = client.post(
        "/credits", content=content, headers={"Content-Type": "application/json", **headers}
    )

    assert response.status_code == 422
    [error] = response.json()["detail"]
    assert error["type"] == (error_type or "string_pattern_mismatch")
    assert error["input"] == echoed_input
    assert db[CREDITS].count_documents({}) == 0
    assert _user(db) == {"user_id": USER, "pending_credits": 0}
    assert published_tasks() == []


def test_unknown_user_returns_404_and_stores_nothing(
    client: TestClient, db: Database[Document], published_tasks: TakeTasks
) -> None:
    response = client.post("/credits", json={"user_id": "u_missing", "amount": "1.00"})

    assert response.status_code == 404
    assert response.headers["content-type"] == PROBLEM_JSON
    assert response.json()["type"] == "about:blank"
    assert db[CREDITS].count_documents({}) == 0
    assert published_tasks() == []


def test_unknown_credit_returns_404(client: TestClient) -> None:
    response = client.get(f"/credits/{uuid.uuid4()}")

    assert response.status_code == 404
    assert response.headers["content-type"] == PROBLEM_JSON


def test_same_key_and_body_replays_the_original_credit(
    client: TestClient, db: Database[Document], published_tasks: TakeTasks
) -> None:
    # Bare and quoted keys are the same key; 10 and "10.00" are the same amount.
    first = client.post(
        "/credits", json={"user_id": USER, "amount": 10}, headers={"Idempotency-Key": "k-1"}
    )
    replay = client.post(
        "/credits",
        json={"user_id": USER, "amount": "10.00"},
        headers={"Idempotency-Key": '"k-1"'},
    )

    assert first.status_code == replay.status_code == 202
    assert replay.json() == first.json()
    assert replay.headers["location"] == first.headers["location"]
    assert db[CREDITS].count_documents({}) == 1
    assert _user(db) == {"user_id": USER, "pending_credits": 1, "status": "processing"}
    assert len(published_tasks()) == 1


def test_same_key_with_different_body_returns_problem_details(
    client: TestClient, db: Database[Document]
) -> None:
    headers = {"Idempotency-Key": "k-1"}
    client.post("/credits", json={"user_id": USER, "amount": "10.00"}, headers=headers)

    response = client.post("/credits", json={"user_id": USER, "amount": "20.00"}, headers=headers)

    assert response.status_code == 422
    assert response.headers["content-type"] == PROBLEM_JSON
    assert response.json()["type"] == IDEMPOTENCY_KEY_REUSED
    assert db[CREDITS].count_documents({}) == 1
    assert _user(db) == {"user_id": USER, "pending_credits": 1, "status": "processing"}


def test_concurrent_requests_with_one_key_create_one_credit(
    client: TestClient, db: Database[Document], published_tasks: TakeTasks
) -> None:
    """Shows the draft's 409 ("still in flight") cannot happen: every request gets 202."""

    def send(_: int) -> tuple[int, str]:
        response = client.post(
            "/credits", json={"user_id": USER, "amount": "5.00"}, headers={"Idempotency-Key": "k"}
        )
        return response.status_code, response.json()["credit_id"]

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = set(pool.map(send, range(10)))

    assert len(results) == 1
    [(status, _credit_id)] = results
    assert status == 202
    assert db[CREDITS].count_documents({}) == 1
    assert _user(db) == {"user_id": USER, "pending_credits": 1, "status": "processing"}
    assert len(published_tasks()) == 1


def test_publish_failure_still_accepts_a_durable_pending_credit(
    settings: Settings,
    user: None,
    db: Database[Document],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Port 1 is never listening.
    broken = settings.model_copy(update={"broker_url": SecretStr("amqp://test:test@127.0.0.1:1//")})

    with TestClient(create_app(broken)) as client, caplog.at_level(logging.WARNING):
        response = client.post("/credits", json={"user_id": USER, "amount": "1.00"})

    assert response.status_code == 202
    credit_id = response.json()["credit_id"]
    credit = db[CREDITS].find_one({"_id": credit_id})
    assert credit is not None
    assert credit["status"] == "pending"
    assert _user(db) == {"user_id": USER, "pending_credits": 1, "status": "processing"}
    assert f"publish failed for credit {credit_id}" in caplog.text
    assert "test:test" not in caplog.text  # no broker credentials in logs
