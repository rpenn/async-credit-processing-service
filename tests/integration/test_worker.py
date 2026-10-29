"""End to end: credits go in through the API and a real Celery worker applies them.

The worker runs in a background thread of this process (Celery's test worker), so tests
can inject database errors into it with monkeypatch. It runs for the whole module against
one database, so each test uses its own user instead of its own database: a delayed retry
from one test must never land in another test's database. Where a test must control when a
task runs, it calls the task directly with `apply_credit.apply()`.

The retry tests come last: after a retried message, this in-thread test worker takes about
10 s to fetch the next message. The production prefork worker does not (measured: 0.03 s).
"""

import importlib
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from types import ModuleType
from typing import Any

import pytest
from bson.decimal128 import Decimal128
from celery.contrib.testing.worker import start_worker
from fastapi.testclient import TestClient
from kombu import Queue
from pydantic import SecretStr
from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.errors import AutoReconnect

from credit_service.api.main import create_app
from credit_service.logs import correlation_id
from credit_service.messaging import APPLY_CREDIT_TASK, TASK_QUEUE
from credit_service.models import CreditStatus
from credit_service.mongo import CREDITS, USERS, Document, utc_now
from credit_service.settings import Settings
from credit_service.worker import database, repository

DEAD_LETTER_QUEUE = "credits.dead-letter"  # declared in rabbitmq/definitions.json


@pytest.fixture(scope="module")
def mongo_uri(mongo_server_uri: str) -> Iterator[str]:
    """Overrides the per-test database from conftest: one database for this module."""
    name = f"test_worker_{uuid.uuid4().hex}"
    yield f"{mongo_server_uri}/{name}?directConnection=true"
    with MongoClient[Document](mongo_server_uri, directConnection=True) as client:
        client.drop_database(name)


@pytest.fixture(scope="module")
def tasks(broker_url: str, mongo_uri: str) -> Iterator[ModuleType]:
    """The worker's tasks module, with a worker consuming the task queue."""
    with pytest.MonkeyPatch.context() as env:
        # The worker reads its settings from the environment, like in production.
        env.setenv("BROKER_URL", broker_url)
        env.setenv("MONGO_URI", mongo_uri)
        tasks = importlib.import_module("credit_service.worker.tasks")
        # Tasks left over from earlier test modules refer to databases that are gone.
        with tasks.app.connection_for_write() as connection:
            queue = tasks.app.amqp.queues[TASK_QUEUE](connection.default_channel)
            queue.declare()
            queue.purge()
        # The solo pool runs tasks in the consumer thread, so acks and rejects go out at once
        # (the threads pool defers them by seconds). No ping check: it needs a result backend.
        with start_worker(tasks.app, pool="solo", perform_ping_check=False):
            yield tasks
        database.get_client().close()
        database._client.cache_clear()


@pytest.fixture
def user_id(tasks: ModuleType, db: Database[Document]) -> str:
    user_id = f"u_{uuid.uuid4().hex[:8]}"
    db[USERS].insert_one({"user_id": user_id, "pending_credits": 0})
    return user_id


@pytest.fixture
def client(settings: Settings, user_id: str) -> Iterator[TestClient]:
    with TestClient(create_app(settings)) as client:
        yield client


def _without_broker(settings: Settings) -> Settings:
    """The API accepts credits but cannot queue them, so the test decides when each runs."""
    return settings.model_copy(update={"broker_url": SecretStr("amqp://test:test@127.0.0.1:1//")})


def _post(client: TestClient, user_id: str, amount: str) -> str:
    response = client.post(
        "/credits",
        json={"user_id": user_id, "amount": amount},
        headers={"Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 202
    credit_id: str = response.json()["credit_id"]
    return credit_id


def _wait_until_finished(client: TestClient, credit_id: str) -> str:
    """Returns the credit's final status."""
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        status: str = client.get(f"/credits/{credit_id}").json()["status"]
        if status != "pending":
            return status
        time.sleep(0.05)
    raise AssertionError(f"credit {credit_id} is still pending")


def _user(db: Database[Document], user_id: str) -> Document | None:
    return db[USERS].find_one({"user_id": user_id}, {"_id": 0, "user_id": 0})


def test_credit_is_applied_and_creates_the_balance(
    client: TestClient, db: Database[Document], user_id: str
) -> None:
    credit_id = _post(client, user_id, "150.00")

    assert _wait_until_finished(client, credit_id) == "applied"
    assert _user(db, user_id) == {
        "pending_credits": 0,
        "status": "processed",
        "balance": Decimal128("150.00"),
    }


def test_running_the_task_again_changes_nothing(
    client: TestClient, db: Database[Document], user_id: str, tasks: ModuleType
) -> None:
    """A redelivered message, or one the reconciler re-enqueued, must not credit twice."""
    credit_id = _post(client, user_id, "150.00")
    _wait_until_finished(client, credit_id)

    tasks.apply_credit.apply(args=[credit_id])  # runs the task again, here and now

    assert _user(db, user_id) == {
        "pending_credits": 0,
        "status": "processed",
        "balance": Decimal128("150.00"),
    }


def test_concurrent_credits_add_up_exactly(
    settings: Settings, db: Database[Document], user_id: str, tasks: ModuleType
) -> None:
    """Many credits for one user, applied at the same moment, conflict on the user document;
    the transactions retry and the total is still exact."""
    # With floats, 0.10 + 0.20 is 0.30000000000000004.
    amounts = ["0.10", "0.20"] * 10
    with ThreadPoolExecutor(max_workers=20) as pool:
        with TestClient(create_app(_without_broker(settings))) as client:
            credit_ids = list(pool.map(lambda amount: _post(client, user_id, amount), amounts))
        list(pool.map(lambda credit_id: tasks.apply_credit.apply(args=[credit_id]), credit_ids))

    assert _user(db, user_id) == {
        "pending_credits": 0,
        "status": "processed",
        "balance": Decimal128("3.00"),
    }


def test_user_stays_processing_until_the_last_credit_finishes(
    settings: Settings, db: Database[Document], user_id: str, tasks: ModuleType
) -> None:
    with TestClient(create_app(_without_broker(settings))) as client:
        first, second = _post(client, user_id, "1.00"), _post(client, user_id, "2.00")

    tasks.apply_credit.apply(args=[first])
    assert _user(db, user_id) == {
        "pending_credits": 1,
        "status": "processing",
        "balance": Decimal128("1.00"),
    }

    tasks.apply_credit.apply(args=[second])
    assert _user(db, user_id) == {
        "pending_credits": 0,
        "status": "processed",
        "balance": Decimal128("3.00"),
    }


@pytest.mark.parametrize(
    "args",
    [[str(uuid.uuid4())], [123], []],
    ids=["unknown-credit-id", "credit-id-not-a-string", "credit-id-missing"],
)
def test_unprocessable_messages_go_to_the_dead_letter_queue(
    tasks: ModuleType, args: list[Any]
) -> None:
    with tasks.app.connection_for_read() as connection:
        dead_letters = Queue(DEAD_LETTER_QUEUE, no_declare=True)(connection.default_channel)
        dead_letters.purge()

        task_id = str(uuid.uuid4())
        tasks.app.send_task(APPLY_CREDIT_TASK, args=args, task_id=task_id)

        deadline = time.monotonic() + 15
        while (message := dead_letters.get(no_ack=True)) is None:
            assert time.monotonic() < deadline, "nothing reached the dead-letter queue"
            time.sleep(0.05)

    assert message.headers["id"] == task_id
    assert message.headers["x-first-death-reason"] == "rejected"


def test_the_request_correlation_id_reaches_the_worker(
    client: TestClient, user_id: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Through the task message header, so the worker's log lines carry the request's ID."""
    seen = []
    original = repository.finish_pending_credit

    def record_correlation_id(*args: Any) -> Document | None:
        seen.append(correlation_id.get())
        return original(*args)

    monkeypatch.setattr(repository, "finish_pending_credit", record_correlation_id)

    response = client.post(
        "/credits",
        json={"user_id": user_id, "amount": "1.00"},
        headers={"Idempotency-Key": str(uuid.uuid4()), "X-Correlation-ID": "trace-me"},
    )

    assert _wait_until_finished(client, response.json()["credit_id"]) == "applied"
    assert seen == ["trace-me"]


def _make_stale(db: Database[Document], credit_id: str, **fields: Any) -> None:
    """As if the credit had been pending for an hour, well past the reconciler's threshold."""
    db[CREDITS].update_one(
        {"_id": credit_id}, {"$set": {"updated_at": utc_now() - timedelta(hours=1), **fields}}
    )


def _credit(db: Database[Document], credit_id: str) -> Document | None:
    return db[CREDITS].find_one({"_id": credit_id}, {"_id": 0, "status": 1, "attempts": 1})


def test_reconciler_re_enqueues_a_stale_credit_and_it_is_applied_once(
    settings: Settings,
    client: TestClient,
    db: Database[Document],
    user_id: str,
    tasks: ModuleType,
) -> None:
    with TestClient(create_app(_without_broker(settings))) as unqueued:
        credit_id = _post(unqueued, user_id, "7.00")
    _make_stale(db, credit_id)

    tasks.reconcile_credits.apply()
    assert _wait_until_finished(client, credit_id) == "applied"
    tasks.reconcile_credits.apply()  # the credit is no longer pending, so nothing happens

    assert _credit(db, credit_id) == {"status": "applied", "attempts": 1}
    assert _user(db, user_id) == {
        "pending_credits": 0,
        "status": "processed",
        "balance": Decimal128("7.00"),
    }


def test_overlapping_reconciler_runs_claim_a_credit_once(
    settings: Settings, db: Database[Document], user_id: str
) -> None:
    with TestClient(create_app(_without_broker(settings))) as unqueued:
        credit_id = _post(unqueued, user_id, "7.00")
    _make_stale(db, credit_id)
    # Both runs read the same stale credit before either claims it.
    [credit] = [
        c for c in repository.find_stale_pending_credits(db, utc_now()) if c["_id"] == credit_id
    ]

    assert repository.claim_for_retry(db, credit, utc_now()) is True
    assert repository.claim_for_retry(db, credit, utc_now()) is False
    assert _credit(db, credit_id) == {"status": "pending", "attempts": 1}


def test_reconciler_leaves_fresh_pending_credits_alone(
    settings: Settings, db: Database[Document], user_id: str, tasks: ModuleType
) -> None:
    with TestClient(create_app(_without_broker(settings))) as unqueued:
        credit_id = _post(unqueued, user_id, "7.00")

    tasks.reconcile_credits.apply()

    assert _credit(db, credit_id) == {"status": "pending", "attempts": 0}


def test_reconciler_fails_a_credit_at_the_attempt_limit(
    settings: Settings,
    db: Database[Document],
    user_id: str,
    tasks: ModuleType,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with TestClient(create_app(_without_broker(settings))) as unqueued:
        credit_id = _post(unqueued, user_id, "7.00")
    max_attempts = tasks.settings.reconcile_max_attempts
    _make_stale(db, credit_id, attempts=max_attempts)

    tasks.reconcile_credits.apply()

    assert _credit(db, credit_id) == {"status": "failed", "attempts": max_attempts}
    assert _user(db, user_id) == {"pending_credits": 0, "status": "processed"}  # no balance
    [failed] = [r for r in caplog.records if vars(r).get("event") == "credit_failed"]
    assert vars(failed)["credit_id"] == credit_id


def test_reconciler_survives_a_database_error_and_the_next_run_continues(
    settings: Settings,
    client: TestClient,
    db: Database[Document],
    user_id: str,
    tasks: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed run must not dead-letter its own message: the credit is still pending, so
    the next scheduled run picks it up."""
    with TestClient(create_app(_without_broker(settings))) as unqueued:
        credit_id = _post(unqueued, user_id, "7.00")
    _make_stale(db, credit_id)

    def unreachable(*_: Any) -> bool:
        raise AutoReconnect("injected: MongoDB unreachable")

    monkeypatch.setattr(repository, "claim_for_retry", unreachable)
    assert tasks.reconcile_credits.apply().successful()
    assert _credit(db, credit_id) == {"status": "pending", "attempts": 0}

    monkeypatch.undo()
    tasks.reconcile_credits.apply()
    assert _wait_until_finished(client, credit_id) == "applied"


def test_transient_error_is_retried_after_a_delay(
    client: TestClient, db: Database[Document], user_id: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The retry goes back through RabbitMQ with a countdown (native delayed delivery)."""
    original = repository.finish_pending_credit
    calls = 0

    def fail_once(*args: Any) -> Document | None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise AutoReconnect("injected: MongoDB unreachable")
        return original(*args)

    monkeypatch.setattr(repository, "finish_pending_credit", fail_once)
    started = time.monotonic()

    credit_id = _post(client, user_id, "5.00")

    assert _wait_until_finished(client, credit_id) == "applied"
    assert calls == 2
    assert time.monotonic() - started >= 1  # the first retry waits 1 second
    assert _user(db, user_id) == {
        "pending_credits": 0,
        "status": "processed",
        "balance": Decimal128("5.00"),
    }


def test_exhausted_retries_mark_the_credit_failed(
    client: TestClient,
    db: Database[Document],
    user_id: str,
    tasks: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = repository.finish_pending_credit

    def never_apply(
        db: Database[Document],
        session: ClientSession,
        credit_id: str,
        status: CreditStatus,
        now: datetime,
    ) -> Document | None:
        if status is CreditStatus.APPLIED:
            raise AutoReconnect("injected: MongoDB unreachable")
        return original(db, session, credit_id, status, now)

    monkeypatch.setattr(repository, "finish_pending_credit", never_apply)
    monkeypatch.setattr(tasks, "MAX_RETRIES", 1)  # one 1-second retry instead of 31 seconds

    credit_id = _post(client, user_id, "5.00")

    assert _wait_until_finished(client, credit_id) == "failed"
    assert _user(db, user_id) == {"pending_credits": 0, "status": "processed"}  # no balance
