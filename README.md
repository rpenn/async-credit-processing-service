# Credit Service

A small service that adds money to a user's balance. A client asks for a credit, gets an
answer right away, and the balance is updated a moment later in the background.

The rule everything is built around: **a credit is applied exactly once, or it fails without
touching the balance**, even when requests are repeated, processes crash, or connections drop.

**Architecture, design decisions and tradeoffs:** [docs/design.pdf](docs/design.pdf), two
pages. Its source is [docs/design.html](docs/design.html).

**Results at a glance:**

- No money lost or doubled in any load test or chaos test.
- 10 chaos tests cut connections and kill processes for real ([Failure modes](#failure-modes)).
- About 440 credits per second per API process, on a laptop ([Load test](#load-test)).

## Reviewer's guide

**Run it in two minutes:** follow [Run it](#run-it) below. You need Docker.

**Five files worth reading, in this order:**

1. [`api/credits.py`](src/credit_service/api/credits.py): the endpoint. Stores the credit, then
   queues it. Handles repeated requests.
2. [`worker/tasks.py`](src/credit_service/worker/tasks.py): applies a credit, retries, and the
   reconciler that rescues stuck credits.
3. [`worker/repository.py`](src/credit_service/worker/repository.py): the database updates that
   make applying a credit twice impossible.
4. [`messaging.py`](src/credit_service/messaging.py): the queue settings that make sure no task is
   lost.
5. [`tests/chaos/test_failures.py`](tests/chaos/test_failures.py): real failures injected into
   the running system, and what each one must leave behind.

**Where each hard problem is handled** (app files are under `src/credit_service/`):

| Problem | How | Where |
|---|---|---|
| Money must be exact | Amounts are decimals, never floating point, from the request to the database | `models.py`, `money.py`, `api/decimal_json.py` |
| A client sends the same request twice | Every request must carry an `Idempotency-Key`, which is unique in the database; a repeat gets the original credit back | `api/credits.py` |
| A task is delivered twice | The worker only acts on a credit that is still `pending`, in the same transaction that updates the balance | `worker/repository.py` |
| Many credits for one user at once | Each update is an atomic increment inside a transaction | `worker/repository.py` |
| A credit is stored but its task never reaches the queue | The reconciler re-queues credits that stay `pending` too long | `worker/tasks.py` |
| A task that crashes the worker every time | RabbitMQ gives up after 5 redeliveries and moves it to a dead-letter queue | `rabbitmq/definitions.json` |
| A connection that silently dies | Every database call and queue publish has a time limit | `mongo.py`, `messaging.py` |
| Following one credit through the logs | Every log line is JSON and carries the request's correlation ID, in the API and in the worker | `logs.py`, `api/correlation.py` |

**Deliberately left out** (see [Future work](#tradeoffs-and-future-work)): authentication,
rate limiting, metrics and alerts, an endpoint to read a user's balance, and running more than
one copy of each database.

## Run it

You need [Docker](https://docs.docker.com/get-docker/) (Docker Desktop on a Mac or Windows) and
[Git](https://git-scm.com/downloads).

**1. Get the code.**

```bash
git clone https://github.com/rpenn/async-credit-processing-service.git
cd async-credit-processing-service
```

**2. Create your settings file.**

```bash
cp .env.example .env
```

This creates `.env`, your local settings. It is never committed.

> [!WARNING]
> **Open `.env` and change `RABBITMQ_PASSWORD` now, before step 3.** RabbitMQ saves the password
> the first time it starts. If you change it later, the services can't log in until you delete
> RabbitMQ's data with `docker compose down --volumes`.

**3. Build and start everything.**

```bash
docker compose up --build --wait
```

Starts the API, the worker, the reconciler, MongoDB and RabbitMQ. `--build` builds the app from
this code; `--wait` returns once every part reports healthy (about a minute the first time).

**4. Create two users.** Credits are only accepted for users that exist.

```bash
docker compose exec api python -m credit_service.seed u_123 u_456
```

**5. Send a credit.** It is labelled with a new random idempotency key (`uuidgen` makes one).

```bash
curl -X POST localhost:8000/credits -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" -d '{"user_id": "u_123", "amount": "150.00"}'
```

The answer comes back at once, with status `pending`, like this:
`{"credit_id": "a9ce84cc-…", "user_id": "u_123", "amount": "150.00", "status": "pending", …}`

**6. Check the balance.**

```bash
docker compose exec mongo mongosh credit_service --quiet --eval 'db.users.find({}, {_id: 0})'
```

A moment after step 5, `u_123` has `balance: Decimal128('150.00')` and `status: 'processed'`.

**Other things to look at:**

- **Interactive API docs:** http://localhost:8000/docs
- **Logs**, one JSON line per event: `docker compose logs -f api worker`
- **RabbitMQ dashboard** (queues, the dead-letter queue): http://localhost:15672, with the login
  from `.env`
- **MongoDB in Compass** (a desktop app for browsing data):
  `mongodb://localhost:27017/credit_service?directConnection=true`. Local only, no password.

**Stop it:** `docker compose down` keeps the data; `docker compose down --volumes` deletes it.

**Settings** (in `.env`, all optional): `MAX_CREDIT_AMOUNT` (largest single credit, default
1,000,000.00), and how often the reconciler runs and when it gives up
(`RECONCILE_INTERVAL_SECONDS`, `RECONCILE_STALE_AFTER_SECONDS`, `RECONCILE_MAX_ATTEMPTS`). Each
is explained in [`.env.example`](.env.example).

**Changing the RabbitMQ setup:** the queue rules live in
[`rabbitmq/definitions.json`](rabbitmq/definitions.json) and load on first start. After
editing the file, load it into the running broker:

```bash
docker compose exec rabbitmq rabbitmqctl import_definitions /etc/rabbitmq/definitions.json
```

## Test it

You also need [uv](https://docs.astral.sh/uv/getting-started/installation/), the Python
project tool. It installs the right Python and everything else on first use.

| Command | What it checks | Time |
|---|---|---|
| `uv run pytest` | 65 tests: the logic, plus the API and worker against real MongoDB and RabbitMQ (started in Docker by the tests) | ~35 s |
| `uv run pytest -m chaos` | 10 tests that break connections and kill processes in a separate copy of the stack ([details](#failure-modes)). Needs `.env`. | ~3 min |
| `uv run python scripts/load_test.py` | Thousands of concurrent credits, then checks every balance ([details](#load-test)). Needs the stack running. | ~5 s |
| `uv run ruff check && uv run mypy` | Code style and type checks | ~5 s |

GitHub runs the first and last rows on every push ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)).

## API

Full, interactive reference: http://localhost:8000/docs.

**`POST /credits`**: ask for a credit.

- Body: `{"user_id": "u_123", "amount": "150.00"}`. The amount can be a string or a number.
- Required `Idempotency-Key` header: a unique value for each credit, such as a random UUID,
  chosen by the client (1–255 printable characters). Send the same key when retrying, and you get
  the original credit back instead of a second one.
- Optional `X-Correlation-ID` header: your own ID for tracing (letters, digits, `.`, `_` or `-`,
  up to 64). It is returned in the response and appears in every log line for this credit. One
  is generated if you don't send it.

| Response | Meaning |
|---|---|
| `202 Accepted` | Stored and queued. The body is the credit; the `Location` header is where to check on it. |
| `400 Bad Request` | The `Idempotency-Key` header is missing. |
| `404 Not Found` | No user with this `user_id`. |
| `422 Unprocessable Content` | The body is invalid (for example, an amount of `0` or `1.234`), or the `Idempotency-Key` was already used with a different body. |
| `503 Service Unavailable` | The database is unavailable. Retry later with the same `Idempotency-Key`, which makes sure the credit is created only once. |

Errors use the standard [problem details](https://www.rfc-editor.org/rfc/rfc9457) format:
`{"title": "Not Found", "status": 404, "detail": "No user with this user_id. …"}`.

**`GET /credits/{credit_id}`**: one credit, with its status: `pending` (waiting), `applied`
(added to the balance) or `failed` (given up, balance unchanged).

**`GET /health/live`**: the API is running. **`GET /health/ready`**: the API can reach MongoDB
and RabbitMQ.

## Assumptions

The assignment left these open, so I chose:

- **RabbitMQ** as the queue: it keeps messages on disk, confirms each one it receives, and can
  set aside messages that keep failing.
- **Amounts** are positive, have at most 2 decimal places, and are at most `MAX_CREDIT_AMOUNT`.
  They are stored as exact decimals.
- **Users must exist** before they can be credited (`404` otherwise). A seed command creates
  them; creating users is not part of this service.
- **The "task ID" is the credit ID.** Each credit is a record in its own `credits` collection,
  so its history survives after the task is done.
- **No authentication.** Anyone who can reach the API can credit any user. A real deployment
  needs it; it was out of scope.

### User status: a deliberate change to the spec

The spec says: set the user to `processing` when a credit arrives, and to `processed` when its
balance is updated. Taken literally, that is wrong when two credits overlap: the first one to
finish would mark the user `processed` while the second is still waiting.

So each user also has `pending_credits`, the number of credits still in flight. A new credit
adds one; each finished credit (applied or failed) subtracts one. The user is `processed` only
when it reaches zero. The fields the spec asks for (`balance`, `status`) work as specified:

```json
{"user_id": "u_123", "balance": "150.00", "status": "processed", "pending_credits": 0,
 "created_at": "…", "updated_at": "…"}
```

### The Idempotency-Key is required

The assignment doesn't say how retries should work. Without a key, the service can't tell a
retry from a new credit: a client whose response got lost, and who tries again, credits the
user twice. For money that is the wrong default, so every request must carry a key, and one
without is refused with `400`, as the
[Idempotency-Key draft standard](https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/)
recommends for operations that require one.

### Why a repeated request never gets "409 Conflict"

The Idempotency-Key draft standard suggests `409` when a repeat arrives while the original is
still being processed. Here that can't happen: the credit and its key are saved in one
transaction, and the database makes a concurrent repeat wait until the original is saved. The
repeat then finds it and returns it. A test sends one key concurrently to prove this
(`test_concurrent_requests_with_one_key_create_one_credit`).

## Failure modes

Every row is proven by a test that causes the failure for real: connections are cut or
frozen with [Toxiproxy](https://github.com/Shopify/toxiproxy) (a proxy that simulates network
faults), and processes are killed with Docker. Nothing is mocked, and the app has no
test-only code.

🟢 handled at once, nothing lost · 🟡 retried automatically · 🔴 ends in an error for the
client, a `failed` credit (balance untouched), or a message set aside for a person

| What goes wrong | What happens | Test |
|---|---|---|
| RabbitMQ is down when the API queues a credit | 🟡 Still `202`: the credit is already saved as `pending`. The reconciler queues it later, within about 6 minutes (it waits 5 minutes, then runs every minute). | `test_broker_unreachable_on_publish_…[dropped]` |
| The API's connection to RabbitMQ freezes | 🟡 As above. The API stops waiting after about 22 s. | `…[half-open]` |
| MongoDB is down or frozen for the API | 🔴 `503` within 5 s. A retry with the same key creates the credit once MongoDB is back, exactly once. | `test_database_unreachable_from_the_api_…[dropped, half-open]` |
| The client never gets the response, and retries | 🟢 The first request was saved anyway; the retry returns that same credit. One credit. | `test_a_retried_request_after_a_lost_response_…` |
| Two identical requests at the same moment | 🟢 One credit; the other request gets it back. | `test_concurrent_requests_with_one_key_…` (integration) |
| MongoDB is down or frozen for the worker | 🟡→🔴 The worker retries after 1, 2, 4 … 64 s. Applied once when MongoDB returns; `failed` if it never does. | `test_database_unreachable_from_the_worker_…`, `test_exhausted_retries_…` (integration) |
| The worker is killed in the middle of a credit | 🟡 RabbitMQ hands the task to the restarted worker. Applied once. (See below.) | `test_worker_killed_mid_transaction_…` |
| The worker loses RabbitMQ in the middle of a credit | 🟡 The task is stopped and handed over again. Applied once. | `test_worker_loses_the_broker_mid_task_…` |
| A task crashes the worker every time | 🟡→🔴 After 6 tries, RabbitMQ moves it to the dead-letter queue (for a person to inspect). The reconciler tries the credit again, then marks it `failed`; the balance is unchanged. | `test_a_message_that_keeps_crashing_the_worker_…` |
| A broken task, or one for a credit that doesn't exist | 🔴 Moved straight to the dead-letter queue. | `test_unprocessable_messages_…` (integration) |
| The same task arrives twice | 🟢 The second does nothing. | `test_running_the_task_again_changes_nothing` (integration) |

Tests marked "(integration)" are in `tests/integration/`; the rest are in
[`tests/chaos/test_failures.py`](tests/chaos/test_failures.py).

**A finding worth knowing:** when a worker is killed mid-credit, MongoDB keeps its unfinished
transaction open for up to 90 seconds (measured: 88 s). Until then, the retried credit is
blocked. The worker's retries originally added up to about 91 seconds, and a test showed the
credit succeeding with only 2–3 seconds to spare. The worker now retries for 127 seconds. The
chaos tests shorten MongoDB's limit to 5 seconds so they run quickly.

## Load test

[`scripts/load_test.py`](scripts/load_test.py) creates fresh users, sends thousands of credits
at once (10% of them repeats with the same `Idempotency-Key`, as a retrying client would send),
waits for all of them to finish, then checks **every user's balance against the sum of their
credits**.

```bash
uv run python scripts/load_test.py --requests 2000 --users 100
```

Other options: `--concurrency` (requests in flight, default 50), `--duplicate-share` (default
0.1). Its users are named `load-…` and stay in the database.

**Results** on an Apple M3 Pro laptop, with Docker given 5 CPUs and 7.7 GB, and the stack exactly
as `compose.yaml` defines it:

| Requests | Users | Repeats | Accepted per second | Response time, median / 95% / 99% | All processed after | Balances correct |
|---|---|---|---|---|---|---|
| 2,000 | 100 | 10% | 420–456 | 93 / 200 / 285 ms | 4.5 s | yes (3 runs) |
| 6,000 | 500 | 10% | 387 | 111 / 217 / 401 ms | 15.4 s | yes |
| 10,000 | 1,000 | 10% | 439 | 204 / 327 / 638 ms | 22.6 s | yes |
| 2,000 | 5 | 30% | 265 | 69 / 656 / 1,949 ms | 7.5 s | yes |

- **No money was lost or doubled** in any run.
- **The worker keeps up:** the last credit is applied right after the last request arrives.
- **The API is the limit:** it used a full CPU while the worker used about 60% of its share.
  Running more copies of the API is the first step to more throughput.
- **Many credits for the same few users slow responses, but stay correct** (last row): updates
  to one user take turns.

Caveats: one laptop, with the test and the service on the same machine, and a small database.
These numbers show how the system behaves, not what it would handle in production.

## Tradeoffs and future work

- **Authentication and rate limiting**, before this is exposed to anyone.
- **Outbox instead of the reconciler.** The API saves a credit, then queues it. If the queue is
  down in between, the reconciler catches it, but only after about 5 minutes. A "transactional
  outbox" (saving the message in the same transaction and sending it from there) would close
  that gap, at the cost of another moving part.
- **Scaling.** First, more API processes (the load test's bottleneck). Beyond that, MongoDB is a
  single server here; production needs a three-member replica set, and eventually sharding by
  user. Credits for one user are serialized by design, so one very busy user stays a limit.
- **Metrics and alerts.** Logs are structured, but nothing watches them yet. What I'd alert
  on, most urgent first.

  **Page someone/Escalate** (money is wrong or stuck):
  - **Any message in the dead-letter queue.** Source: RabbitMQ's Prometheus plugin.
  - **Any credit marked `failed`.** Source: the `credit_failed` log event.
  - **A `pending` credit older than 10 minutes:** stuck, and the reconciler isn't rescuing it.
    Source: a MongoDB query for the oldest `pending` credit.
  - **The `credits` queue growing for several minutes.** Source: RabbitMQ's Prometheus plugin.
  - **No worker attached to the `credits` queue.** Source: RabbitMQ's Prometheus plugin.

  **Slack/Warn the team** (something is degrading):
  - **`POST /credits` much slower than usual** (the load test's 99th percentile was 285–638 ms).
    Source: the load balancer's response times. The API's access log has no durations yet.
  - **More `503` responses than usual.** Source: the API's access log.
  - **More `credit_publish_failed` events than usual:** RabbitMQ is flaky. Source: the log event.
  - **More `credit_retry_scheduled` events than usual:** MongoDB is struggling. Source: the log
    event.
- **CI runs on `ubuntu-22.04`** because MongoDB 8.0.34 refuses to start on GitHub's newer runner
  kernel ([details](https://github.com/actions/runner-images/issues/15005)). Switch back when
  MongoDB 8.0.35 is out.
