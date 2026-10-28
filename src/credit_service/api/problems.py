"""Error responses.

Errors the app raises itself are RFC 9457 problem details (application/problem+json).
Request validation errors keep FastAPI's standard 422 body.
"""

import logging
from decimal import Decimal

from fastapi import Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pymongo.errors import PyMongoError

from credit_service.mongo import is_transient

logger = logging.getLogger(__name__)

PROBLEM_JSON = "application/problem+json"


class ProblemDetails(BaseModel):
    # "about:blank" means the HTTP status says it all (RFC 9457, section 4.2.1).
    type: str = "about:blank"
    title: str
    status: int
    detail: str


# For the OpenAPI docs of routes that can return a problem.
PROBLEM_RESPONSE = {
    "description": "Problem details (RFC 9457)",
    "content": {PROBLEM_JSON: {"schema": ProblemDetails.model_json_schema()}},
}


class ProblemError(Exception):
    def __init__(self, problem: ProblemDetails) -> None:
        super().__init__(problem.title)
        self.problem = problem


async def problem_handler(request: Request, exc: ProblemError) -> JSONResponse:
    return JSONResponse(
        exc.problem.model_dump(), status_code=exc.problem.status, media_type=PROBLEM_JSON
    )


async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """FastAPI's default 422 body, except an echoed amount stays an exact string.

    The default encoder turns Decimal into float, which would show a different number
    than the client sent. https://fastapi.tiangolo.com/tutorial/handling-errors/
    """
    errors = jsonable_encoder(exc.errors(), custom_encoder={Decimal: str})
    return JSONResponse({"detail": errors}, status_code=422)


async def database_error_handler(request: Request, exc: PyMongoError) -> JSONResponse:
    """MongoDB is unreachable or too slow: 503, so the client knows to retry.

    The credit may have been stored even so (the commit can succeed after the deadline), so
    the safe retry carries the same Idempotency-Key. Other database errors stay 500s.
    """
    if not is_transient(exc):
        raise exc
    logger.warning("database unavailable", extra={"error": type(exc).__name__})
    problem = ProblemDetails(
        title="Service Unavailable",
        status=503,
        detail="The database is unavailable. Retry later with the same Idempotency-Key.",
    )
    return JSONResponse(problem.model_dump(), status_code=503, media_type=PROBLEM_JSON)
