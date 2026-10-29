"""Real failures injected into the running stack: no mocks, and no test code in the app.

Each test breaks one thing, then checks the money: the credit ends applied exactly once, or
failed with no balance change, and the user's pending count and status match.
"""

import time
import uuid
from collections.abc import Callable

import httpx2
import pytest
from bson.decimal128 import Decimal128
from pymongo.database import Database

from credit_service.mongo import CREDITS, USERS, Document
from tests.chaos.harness import (
    API_MONGO,
    API_RABBITMQ,
    DEAD_LETTER_QUEUE,
    WORKER_MONGO,
    WORKER_RABBITMQ,
    RabbitMQ,
    Toxiproxy,
    compose,
    deliveries,
    events,
    open_transactions,
    run_reconciler,
    wait_until,
)

pytestmark = pytest.mark.chaos

# compose.chaos.yaml sets RECONCILE_STALE_AFTER_SECONDS=1 and RECONCILE_MAX_ATTEMPTS=1.
STALE_AFTER_SECONDS = 1
# rabbitmq/definitions.json: the queue's delivery limit is 5, and RabbitMQ dead-letters a
# message once it has been returned more times than that.
DELIVERIES_BEFORE_DEAD_LETTER = 6


def _breaker(toxiproxy: Toxiproxy, failure: str) -> Callable[[str], None]:
    return toxiproxy.drop if failure == "dropped" else toxiproxy.black_hole


def _post(
    api: httpx2.Client, user_id: str, amount: str = "10.00", key: str | None = None
) -> httpx2.Response:
    headers = {"Idempotency-Key": key or str(uuid.uuid4())}
    return api.post("/credits", json={"user_id": user_id, "amount": amount}, headers=headers)


def _accept(api: httpx2.Client, user_id: str, amount: str = "10.00") -> str:
    response = _post(api, user_id, amount)
    assert response.status_code == 202, response.text
    credit_id: str = response.json()["credit_id"]
    return credit_id


def _credit(db: Database[Document], credit_id: str) -> Document:
    credit = db[CREDITS].find_one({"_id": credit_id})
    assert credit is not None
    return credit


def _wait_for_status(db: Database[Document], credit_id: str, status: str) -> None:
    wait_until(lambda: _credit(db, credit_id)["status"] == status, 60, f"credit {status}")


def _user(db: Database[Document], user_id: str) -> Document | None:
    return db[USERS].find_one({"user_id": user_id}, {"_id": 0, "user_id": 0})


def _credited_once(amount: str) -> Document:
    return {"pending_credits": 0, "status": "processed", "balance": Decimal128(amount)}


def test_worker_killed_mid_transaction_applies_the_credit_exactly_once(
    api: httpx2.Client, db: Database[Document], user_id: str, toxiproxy: Toxiproxy
) -> None:
    """The killed worker's transaction stays open in MongoDB: closing the connection does not
    abort it. The redelivered task meets write conflicts and retries until MongoDB aborts the
    old transaction (transactionLifetimeLimitSeconds, 5 s here, 60 s by default)."""
    wait_until(lambda: not open_transactions(db), 15, "earlier transactions to end")
    toxiproxy.slow_down(WORKER_MONGO, 1000)
    credit_id = _accept(api, user_id)
    wait_until(lambda: open_transactions(db), 15, "the worker to open its transaction")

    compose("kill", "worker")
    toxiproxy.heal()
    assert open_transactions(db), "the killed worker's transaction is gone already"

    compose("start", "worker")
    _wait_for_status(db, credit_id, "applied")
    assert _user(db, user_id) == _credited_once("10.00")
    assert events("worker", credit_id).count("credit_applied") == 1


@pytest.mark.parametrize("failure", ["dropped", "half-open"])
def test_broker_unreachable_on_publish_then_the_reconciler_completes_the_credit(
    api: httpx2.Client,
    db: Database[Document],
    user_id: str,
    toxiproxy: Toxiproxy,
    failure: str,
) -> None:
    # Publish once first, so "half-open" breaks a connection the API already holds.
    _wait_for_status(db, _accept(api, user_id, "1.00"), "applied")
    _breaker(toxiproxy, failure)(API_RABBITMQ)

    started = time.monotonic()
    credit_id = _accept(api, user_id)  # still 202: the credit is stored before publishing
    # Bounded by the publish confirm timeout (5 s) plus Celery's 3 publish retries.
    assert time.monotonic() - started < 30
    assert _credit(db, credit_id)["status"] == "pending"

    toxiproxy.heal()
    time.sleep(STALE_AFTER_SECONDS)
    run_reconciler()

    _wait_for_status(db, credit_id, "applied")
    assert _user(db, user_id) == _credited_once("11.00")


@pytest.mark.parametrize("failure", ["dropped", "half-open"])
def test_database_unreachable_from_the_api_returns_503_and_stores_nothing(
    api: httpx2.Client,
    db: Database[Document],
    user_id: str,
    toxiproxy: Toxiproxy,
    failure: str,
) -> None:
    key = str(uuid.uuid4())
    _breaker(toxiproxy, failure)(API_MONGO)

    started = time.monotonic()
    response = _post(api, user_id, key=key)

    assert response.status_code == 503
    assert response.headers["content-type"] == "application/problem+json"
    assert time.monotonic() - started < 10  # the API's 5 s MongoDB deadline
    toxiproxy.heal()
    assert db[CREDITS].count_documents({"user_id": user_id}) == 0
    assert _user(db, user_id) == {"pending_credits": 0}

    # The retry the 503 asks for: the same Idempotency-Key, until MongoDB answers again.
    # A half-open link can leave an open transaction that holds the user for a few seconds.
    responses = [_post(api, user_id, key=key)]
    while responses[-1].status_code == 503 and len(responses) < 20:
        time.sleep(1)
        responses.append(_post(api, user_id, key=key))
    assert responses[-1].status_code == 202
    _wait_for_status(db, responses[-1].json()["credit_id"], "applied")
    assert _user(db, user_id) == _credited_once("10.00")


@pytest.mark.parametrize("failure", ["dropped", "half-open"])
def test_database_unreachable_from_the_worker_is_retried_then_applied_once(
    api: httpx2.Client,
    db: Database[Document],
    user_id: str,
    toxiproxy: Toxiproxy,
    failure: str,
) -> None:
    _breaker(toxiproxy, failure)(WORKER_MONGO)

    credit_id = _accept(api, user_id)
    wait_until(
        lambda: "credit_retry_scheduled" in events("worker", credit_id),
        30,
        "the worker to schedule a retry",
    )
    assert _credit(db, credit_id)["status"] == "pending"

    toxiproxy.heal()
    _wait_for_status(db, credit_id, "applied")
    assert _user(db, user_id) == _credited_once("10.00")


def test_worker_loses_the_broker_mid_task_and_the_credit_is_applied_once(
    api: httpx2.Client, db: Database[Document], user_id: str, toxiproxy: Toxiproxy
) -> None:
    """The worker cannot ack after the connection drops, so RabbitMQ redelivers the task."""
    toxiproxy.slow_down(WORKER_MONGO, 1000)
    credit_id = _accept(api, user_id)
    wait_until(lambda: deliveries(credit_id) == 1, 15, "the worker to receive the task")

    toxiproxy.drop(WORKER_RABBITMQ)
    time.sleep(3)
    toxiproxy.heal()

    _wait_for_status(db, credit_id, "applied")
    assert deliveries(credit_id) >= 2
    assert _user(db, user_id) == _credited_once("10.00")


def test_a_retried_request_after_a_lost_response_creates_one_credit(
    api: httpx2.Client, db: Database[Document], user_id: str, toxiproxy: Toxiproxy
) -> None:
    """The client gives up before the response arrives and retries with the same key. The
    original request commits anyway; the retry waits for it and gets that credit back."""
    key = str(uuid.uuid4())
    toxiproxy.slow_down(API_MONGO, 1000)
    with pytest.raises(httpx2.ReadTimeout):
        api.post(
            "/credits",
            json={"user_id": user_id, "amount": "10.00"},
            headers={"Idempotency-Key": key},
            timeout=1,
        )
    toxiproxy.heal()

    response = _post(api, user_id, key=key)

    assert response.status_code == 202
    [credit] = db[CREDITS].find({"user_id": user_id}).to_list()
    assert response.json()["credit_id"] == credit["_id"]
    # The original request (whose response the client never saw) and the retry. The retry
    # replays as soon as the original commits, so it can log before the original publishes.
    assert sorted(events("api", credit["_id"])) == [
        "credit_accepted",
        "credit_published",
        "credit_replayed",
    ]
    _wait_for_status(db, credit["_id"], "applied")
    assert _user(db, user_id) == _credited_once("10.00")


def _kill_the_worker_on_each_delivery(credit_id: str, handled: int) -> int:
    """Kill the worker whenever it receives the credit's task, until RabbitMQ stops
    delivering it. Returns the number of deliveries so far."""
    while _delivered_again(credit_id, handled):
        compose("kill", "worker")
        handled = deliveries(credit_id)
        compose("start", "worker")
    return handled


def _delivered_again(credit_id: str, handled: int) -> bool:
    """Waits up to 10 s (ample for a restarted worker) for one more delivery."""
    try:
        wait_until(lambda: deliveries(credit_id) > handled, 10, "another delivery")
    except AssertionError:
        return False
    return True


def test_a_message_that_keeps_crashing_the_worker_is_dead_lettered_then_failed(
    api: httpx2.Client,
    db: Database[Document],
    user_id: str,
    toxiproxy: Toxiproxy,
    rabbitmq: RabbitMQ,
) -> None:
    """A poison message: whatever it is, the worker dies each time it holds it. RabbitMQ
    dead-letters it at the delivery limit, and the credit stays pending. The reconciler
    re-enqueues it, that copy is dead-lettered the same way, and at its attempt limit the
    reconciler marks the credit failed and releases the user."""
    dead_letters = rabbitmq.queue_depth(DEAD_LETTER_QUEUE)
    toxiproxy.slow_down(WORKER_MONGO, 1000)  # so the worker still holds the task when killed
    credit_id = _accept(api, user_id)

    handled = _kill_the_worker_on_each_delivery(credit_id, handled=0)

    assert handled == DELIVERIES_BEFORE_DEAD_LETTER
    wait_until(
        lambda: rabbitmq.queue_depth(DEAD_LETTER_QUEUE) == dead_letters + 1,
        15,
        "the dead-letter queue to grow",
    )
    assert _credit(db, credit_id)["status"] == "pending"

    run_reconciler()  # attempt 1 of 1
    handled = _kill_the_worker_on_each_delivery(credit_id, handled)

    assert handled == 2 * DELIVERIES_BEFORE_DEAD_LETTER
    wait_until(
        lambda: rabbitmq.queue_depth(DEAD_LETTER_QUEUE) == dead_letters + 2,
        15,
        "the dead-letter queue to grow again",
    )
    assert _credit(db, credit_id)["status"] == "pending"

    toxiproxy.heal()
    # A run can meet the last killed worker's open transaction; it then leaves the credit
    # pending for the next run.
    for _ in range(10):
        run_reconciler()
        if _credit(db, credit_id)["status"] == "failed":
            break
        time.sleep(1)
    assert _credit(db, credit_id)["attempts"] == 1
    assert _credit(db, credit_id)["status"] == "failed"
    assert _user(db, user_id) == {"pending_credits": 0, "status": "processed"}  # no balance
