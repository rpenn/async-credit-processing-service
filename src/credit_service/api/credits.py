import asyncio
import logging
import uuid
from typing import Annotated

import pymongo
from celery import Celery
from fastapi import APIRouter, Depends, Header, Request, Response, status
from fastapi.exceptions import RequestValidationError
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.errors import DuplicateKeyError

from credit_service.api import repository
from credit_service.api.decimal_json import DecimalJSONRoute
from credit_service.api.problems import (
    IDEMPOTENCY_KEY_REUSED,
    PROBLEM_RESPONSE,
    ProblemDetails,
    ProblemError,
)
from credit_service.messaging import publish_apply_credit
from credit_service.models import CreditRequest, CreditResponse
from credit_service.money import from_decimal128
from credit_service.mongo import TIMEOUT_SECONDS, Document, utc_now
from credit_service.settings import Settings

logger = logging.getLogger(__name__)

# DecimalJSONRoute: JSON numbers in the body are parsed as Decimal, never float.
router = APIRouter(prefix="/credits", tags=["credits"], route_class=DecimalJSONRoute)

# The IETF Idempotency-Key draft makes the value a structured-field string, which is quoted
# ("abc"). Many clients send it bare (abc), so both are accepted and mean the same key.
# Printable ASCII without quotes or backslashes, 1-255 characters.
_IDEMPOTENCY_KEY = r'^(?:"[ !#-\[\]-~]{1,255}"|[!#-\[\]-~]{1,255})$'


def _mongo(request: Request) -> AsyncMongoClient[Document]:
    mongo: AsyncMongoClient[Document] = request.app.state.mongo
    return mongo


def _celery(request: Request) -> Celery:
    celery: Celery = request.app.state.celery
    return celery


def _settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


Mongo = Annotated[AsyncMongoClient[Document], Depends(_mongo)]
CeleryApp = Annotated[Celery, Depends(_celery)]
AppSettings = Annotated[Settings, Depends(_settings)]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={400: PROBLEM_RESPONSE, 404: PROBLEM_RESPONSE},
)
async def create_credit(
    body: CreditRequest,
    request: Request,
    response: Response,
    mongo: Mongo,
    celery: CeleryApp,
    settings: AppSettings,
    idempotency_key: Annotated[str, Header(pattern=_IDEMPOTENCY_KEY)],
) -> CreditResponse:
    """Accept a credit. The balance changes later, in the worker; poll the Location URL.

    The Idempotency-Key is required: without it, a client retrying after a lost response
    would credit the user twice. Reusing a key with the same body returns the original
    credit; with a different body, 422 problem details.
    """
    if body.amount > settings.max_credit_amount:
        raise RequestValidationError(
            [
                {
                    "type": "less_than_equal",
                    "loc": ("body", "amount"),
                    "msg": f"Input should be less than or equal to {settings.max_credit_amount}",
                    "input": body.amount,
                    "ctx": {"le": settings.max_credit_amount},
                }
            ]
        )

    key = idempotency_key.strip('"')
    db = mongo.get_default_database()
    now = utc_now()
    credit = repository.new_credit(body.user_id, body.amount, key, now)

    # with_transaction retries the whole callback on transient errors (such as a write
    # conflict with a concurrent credit for the same user), so it only touches the database.
    async def accept(session: AsyncClientSession) -> None:
        # Credit first: a reused key fails here, before the user document is touched.
        await repository.insert_credit(db, session, credit)
        if not await repository.reserve_user(db, session, body.user_id, now):
            raise ProblemError(  # aborts the transaction, so the credit is not kept
                ProblemDetails(
                    title="Not Found",
                    status=404,
                    detail="No user with this user_id. Users must exist before a credit.",
                )
            )

    try:
        # One deadline for the whole transaction: with_transaction would otherwise keep
        # retrying for up to 120 s while MongoDB is unreachable. On timeout the client gets a
        # 503 (see problems.py) and can safely retry with the same Idempotency-Key.
        with pymongo.timeout(TIMEOUT_SECONDS):
            async with mongo.start_session() as session:
                await session.with_transaction(accept)
    except DuplicateKeyError:
        # The key belongs to a credit that has already committed: until a transaction
        # commits, a concurrent one with the same key gets a write conflict and is retried.
        # So a duplicate is never "still in flight" and the draft's 409 cannot happen here.
        original = await repository.find_credit_by_idempotency_key(db, key)
        if original is None:
            raise
        _ensure_same_request(original, body)
        credit = original  # replay; the original request already published (or tried to)
        logger.info(
            "credit replayed for a reused Idempotency-Key",
            extra={"event": "credit_replayed", "credit_id": credit["_id"]},
        )
    else:
        logger.info(
            "credit accepted",
            extra={"event": "credit_accepted", "credit_id": credit["_id"], "user_id": body.user_id},
        )
        await _publish(celery, credit["_id"])

    response.headers["Location"] = str(
        request.app.url_path_for("get_credit", credit_id=credit["_id"])
    )
    return repository.to_response(credit)


@router.get("/{credit_id}", responses={404: PROBLEM_RESPONSE})
async def get_credit(credit_id: uuid.UUID, mongo: Mongo) -> CreditResponse:
    credit = await repository.find_credit(mongo.get_default_database(), str(credit_id))
    if credit is None:
        raise ProblemError(
            ProblemDetails(title="Not Found", status=404, detail="No credit with this credit_id.")
        )
    return repository.to_response(credit)


def _ensure_same_request(original: Document, body: CreditRequest) -> None:
    """Compare by value, so 10 and "10.00" are the same amount."""
    if original["user_id"] != body.user_id or from_decimal128(original["amount"]) != body.amount:
        raise ProblemError(
            ProblemDetails(
                type=IDEMPOTENCY_KEY_REUSED,
                title="Idempotency-Key is already used",
                status=422,
                detail="This Idempotency-Key was already used with a different request body.",
            )
        )


async def _publish(celery: Celery, credit_id: str) -> None:
    """Publish only after the transaction commits, and never fail the request over it.

    If the broker is down, the credit is still safely stored as pending, and the reconciler
    re-enqueues it later. Publishing first could enqueue a credit that then fails to commit.
    Publishing blocks, so it runs in a thread to keep the event loop free.
    """
    try:
        await asyncio.to_thread(publish_apply_credit, celery, credit_id)
    except Exception as exc:  # kombu raises many unrelated types when the broker is down
        logger.warning(
            "credit publish failed, left for the reconciler",
            extra={
                "event": "credit_publish_failed",
                "credit_id": credit_id,
                "error": type(exc).__name__,
            },
        )
    else:
        logger.info("credit published", extra={"event": "credit_published", "credit_id": credit_id})
