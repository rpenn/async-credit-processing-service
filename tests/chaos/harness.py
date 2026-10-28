"""Controls for the chaos stack (compose.yaml plus compose.chaos.yaml).

Failures are injected from outside, never through application code: Toxiproxy breaks one
network link at a time, and Compose kills a process outright (SIGKILL, as `docker kill`).
"""

import json
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx2
from pydantic_settings import BaseSettings, SettingsConfigDict
from pymongo.database import Database

from credit_service.mongo import Document

ROOT = Path(__file__).parents[2]
COMPOSE = [
    "docker",
    "compose",
    "--project-name",
    "credit-service-chaos",  # its own containers and volumes, apart from the dev stack
    "--file",
    str(ROOT / "compose.yaml"),
    "--file",
    str(ROOT / "compose.chaos.yaml"),
]

# Host ports published by compose.chaos.yaml.
API_URL = "http://127.0.0.1:18000"
TOXIPROXY_URL = "http://127.0.0.1:18474"
RABBITMQ_API_URL = "http://127.0.0.1:25672/api"
MONGO_URI = "mongodb://127.0.0.1:28017/credit_service?directConnection=true"

# One Toxiproxy proxy per network link (tests/chaos/toxiproxy.json).
API_MONGO = "api_mongo"
API_RABBITMQ = "api_rabbitmq"
WORKER_MONGO = "worker_mongo"
WORKER_RABBITMQ = "worker_rabbitmq"

DEAD_LETTER_QUEUE = "credits.dead-letter"  # declared in rabbitmq/definitions.json


class RabbitMQLogin(BaseSettings):
    """The same login Compose gives RabbitMQ: from the environment, else from .env."""

    model_config = SettingsConfigDict(env_file=ROOT / ".env", extra="ignore")

    rabbitmq_user: str
    rabbitmq_password: str


def compose(*args: str) -> str:
    result = subprocess.run(  # noqa: S603 - a fixed command, no shell
        [*COMPOSE, *args], capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise AssertionError(f"docker compose {' '.join(args)} failed:\n{result.stderr}")
    return result.stdout


def wait_until(condition: Callable[[], object], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"waited {timeout} s for {what}")
        time.sleep(0.2)


class Toxiproxy:
    """Breaks network links through Toxiproxy's HTTP API."""

    def __init__(self, client: httpx2.Client) -> None:
        self._client = client

    def drop(self, link: str) -> None:
        """Close the link's connections and refuse new ones, like a stopped server."""
        self._client.post(f"/proxies/{link}", json={"enabled": False}).raise_for_status()

    def black_hole(self, link: str) -> None:
        """Half-open: connections stay up, but no data passes in either direction."""
        for stream in ("upstream", "downstream"):
            self._toxic(link, "timeout", stream, timeout=0)

    def slow_down(self, link: str, milliseconds: int) -> None:
        """Delay every reply on the link, so a task stays in flight long enough to interrupt."""
        self._toxic(link, "latency", "downstream", latency=milliseconds)

    def heal(self) -> None:
        """Re-enable every link and remove every toxic."""
        self._client.post("/reset").raise_for_status()

    def _toxic(self, link: str, kind: str, stream: str, **attributes: int) -> None:
        toxic = {"type": kind, "stream": stream, "attributes": attributes}
        self._client.post(f"/proxies/{link}/toxics", json=toxic).raise_for_status()


class RabbitMQ:
    """RabbitMQ's management HTTP API. Its queue statistics refresh every 5 seconds."""

    def __init__(self, client: httpx2.Client) -> None:
        self._client = client

    def queue_depth(self, queue: str) -> int:
        response = self._client.get(f"/queues/%2F/{queue}")
        response.raise_for_status()
        depth: int = response.json().get("messages", 0)
        return depth


def logs_about(service: str, credit_id: str) -> list[dict[str, Any]]:
    """The service's JSON log lines that mention the credit, across container restarts."""
    lines = compose("logs", "--no-log-prefix", service).splitlines()
    return [json.loads(line) for line in lines if line.startswith("{") and credit_id in line]


def events(service: str, credit_id: str) -> list[str]:
    """The credit's state-change events (see logs.py) logged by the service, in order."""
    return [entry["event"] for entry in logs_about(service, credit_id) if "event" in entry]


def deliveries(credit_id: str) -> int:
    """How many times the worker has received the credit's task."""
    received = [e for e in logs_about("worker", credit_id) if e["message"].endswith("received")]
    return len(received)


def run_reconciler() -> None:
    """One reconciler run, in a one-off container with the worker's image and settings.

    Not through the worker itself, so a test can kill the worker without catching the
    reconciler mid-run.
    """
    compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "worker",
        "python",
        "-c",
        "from credit_service.worker.tasks import reconcile_credits; reconcile_credits()",
    )


def open_transactions(db: Database[Document]) -> list[Document]:
    """Transactions MongoDB still holds open, including ones whose client has gone."""
    pipeline: list[Document] = [
        {"$currentOp": {"idleSessions": True, "allUsers": True}},
        {"$match": {"transaction": {"$exists": True}}},
    ]
    return db.client.admin.aggregate(pipeline).to_list()
