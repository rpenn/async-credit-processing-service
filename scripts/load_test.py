"""Load test for the running Compose stack (docker compose up --build --wait).

    uv run python scripts/load_test.py --requests 2000 --users 100

Creates fresh users, sends concurrent credits (a share of them repeat an earlier request with
the same Idempotency-Key, as a retrying client would), waits until every credit is processed,
then checks each user's balance against the sum of their unique credits. It reads MongoDB
directly, because the API has no endpoint for balances.
"""

import argparse
import asyncio
import random
import statistics
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal

import httpx2
from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase

from credit_service.models import CreditStatus, UserStatus
from credit_service.money import from_decimal128
from credit_service.mongo import CREDITS, USERS, Document, utc_now

POLL_SECONDS = 0.5


@dataclass(frozen=True)
class Credit:
    key: str
    user_id: str
    amount: Decimal


@dataclass(frozen=True)
class Sent:
    credit: Credit
    status: int  # 0 when the request failed without a response
    seconds: float
    credit_id: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument(
        "--mongo-uri", default="mongodb://localhost:27017/credit_service?directConnection=true"
    )
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=50, help="requests in flight")
    parser.add_argument(
        "--duplicate-share", type=float, default=0.1, help="share of requests that are repeats"
    )
    parser.add_argument("--timeout", type=float, default=300, help="seconds to wait to drain")
    return parser.parse_args()


def plan(users: list[str], requests: int, duplicate_share: float) -> list[Credit]:
    rng = random.Random()  # noqa: S311 (test data, not security)
    unique = [
        Credit(str(uuid.uuid4()), rng.choice(users), Decimal(rng.randint(1, 10_000)).scaleb(-2))
        for _ in range(round(requests * (1 - duplicate_share)))
    ]
    repeats = [rng.choice(unique) for _ in range(requests - len(unique))]
    credits = unique + repeats
    rng.shuffle(credits)
    return credits


async def send(client: httpx2.AsyncClient, slots: asyncio.Semaphore, credit: Credit) -> Sent:
    body = {"user_id": credit.user_id, "amount": str(credit.amount)}
    async with slots:
        started = time.perf_counter()
        try:
            response = await client.post(
                "/credits", json=body, headers={"Idempotency-Key": credit.key}
            )
        except httpx2.TransportError:
            return Sent(credit, 0, time.perf_counter() - started, None)
        seconds = time.perf_counter() - started
    accepted = response.status_code == 202
    return Sent(
        credit, response.status_code, seconds, response.json()["credit_id"] if accepted else None
    )


async def wait_until_processed(db: AsyncDatabase[Document], ids: list[str]) -> None:
    """Polls MongoDB, the only place the worker's progress shows."""
    pending = {"_id": {"$in": ids}, "status": CreditStatus.PENDING}
    while True:
        if not await db[CREDITS].count_documents(pending):
            return
        await asyncio.sleep(POLL_SECONDS)


async def end_to_end_seconds(db: AsyncDatabase[Document], ids: list[str]) -> float:
    """From the first credit stored to the last one finished, by the servers' clocks."""
    cursor = await db[CREDITS].aggregate(
        [
            {"$match": {"_id": {"$in": ids}}},
            {
                "$group": {
                    "_id": None,
                    "first": {"$min": "$created_at"},
                    "last": {"$max": "$updated_at"},
                }
            },
        ]
    )
    [times] = await cursor.to_list()
    return float((times["last"] - times["first"]).total_seconds())


async def check(db: AsyncDatabase[Document], users: list[str], accepted: list[Sent]) -> list[str]:
    """Every way the stored data can disagree with what the API accepted."""
    problems = []

    credit_ids_by_key = defaultdict(set)
    for sent in accepted:
        credit_ids_by_key[sent.credit.key].add(sent.credit_id)
    problems += [
        f"key {k} got credits {ids}" for k, ids in credit_ids_by_key.items() if len(ids) > 1
    ]

    stored = await db[CREDITS].find({"idempotency_key": {"$in": list(credit_ids_by_key)}}).to_list()
    if len(stored) != len(credit_ids_by_key):
        problems.append(f"{len(stored)} credits stored for {len(credit_ids_by_key)} unique keys")
    problems += [
        f"credit {c['_id']} is {c['status']}" for c in stored if c["status"] != CreditStatus.APPLIED
    ]

    expected: dict[str, Decimal] = defaultdict(Decimal)
    for credit in {sent.credit for sent in accepted}:
        expected[credit.user_id] += credit.amount
    async for user in db[USERS].find({"user_id": {"$in": users}}):
        balance = from_decimal128(user["balance"]) if "balance" in user else Decimal(0)
        if balance != expected[user["user_id"]]:
            problems.append(
                f"{user['user_id']}: balance {balance}, expected {expected[user['user_id']]}"
            )
        if (
            user["pending_credits"] != 0
            or user.get("status", UserStatus.PROCESSED) != UserStatus.PROCESSED
        ):
            problems.append(f"{user['user_id']}: still processing")
    return problems


async def main() -> int:
    args = parse_args()
    run_id = uuid.uuid4().hex[:8]
    users = [f"load-{run_id}-{n}" for n in range(args.users)]
    credits = plan(users, args.requests, args.duplicate_share)

    async with AsyncMongoClient[Document](args.mongo_uri) as mongo:
        db = mongo.get_default_database()
        # The same documents credit_service.seed creates.
        now = utc_now()
        await db[USERS].insert_many(
            [
                {"user_id": u, "pending_credits": 0, "created_at": now, "updated_at": now}
                for u in users
            ]
        )

        limits = httpx2.Limits(max_connections=args.concurrency)
        slots = asyncio.Semaphore(args.concurrency)
        async with httpx2.AsyncClient(base_url=args.api_url, limits=limits, timeout=30) as client:
            started = time.perf_counter()
            results = await asyncio.gather(*(send(client, slots, c) for c in credits))
            sending = time.perf_counter() - started

        accepted = [r for r in results if r.status == 202]
        ids = sorted({r.credit_id for r in accepted if r.credit_id})
        try:
            async with asyncio.timeout(args.timeout):
                await wait_until_processed(db, ids)
        except TimeoutError:
            problems = ["timed out waiting for the credits to be processed"]
        else:
            problems = await check(db, users, accepted)
        end_to_end = await end_to_end_seconds(db, ids)

    latencies = statistics.quantiles([r.seconds * 1000 for r in accepted], n=100)
    p50, p95, p99 = latencies[49], latencies[94], latencies[98]
    statuses = sorted({r.status for r in results})
    counts = ", ".join(f"{s}: {sum(r.status == s for r in results)}" for s in statuses)
    print(f"run {run_id}: {len(results)} requests, {len(ids)} unique credits, {args.users} users")
    print(f"responses:   {counts}")
    print(f"accepted:    {len(accepted) / sending:.0f} requests/s over {sending:.1f} s")
    print(f"202 latency: p50 {p50:.0f} ms, p95 {p95:.0f} ms, p99 {p99:.0f} ms")
    print(f"end to end:  {end_to_end:.1f} s from the first credit stored to the last applied")
    print("correct:     " + ("yes" if not problems else f"NO, {len(problems)} problems"))
    for problem in problems[:20]:
        print(f"  {problem}")
    return 1 if problems or len(accepted) < len(results) else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
