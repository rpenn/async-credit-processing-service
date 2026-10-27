import asyncio
import logging
from typing import Any

import pymongo
from celery import Celery
from fastapi import APIRouter, Request, Response, status
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/health", tags=["health"])

# Readiness must answer quickly; a dependency that is this slow counts as down.
CHECK_TIMEOUT_SECONDS = 2


@router.get("/live")
async def live() -> dict[str, str]:
    """The process is up and serving requests. Checks no dependencies."""
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request, response: Response) -> dict[str, Any]:
    """Both MongoDB and the broker are reachable, so the service can accept credits."""
    checks = {
        "mongodb": await _check_mongodb(request.app.state.mongo),
        "broker": await asyncio.to_thread(_check_broker, request.app.state.celery),
    }
    healthy = all(result == "ok" for result in checks.values())
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ok" if healthy else "unavailable", "checks": checks}


async def _check_mongodb(mongo: AsyncMongoClient[dict[str, Any]]) -> str:
    try:
        with pymongo.timeout(CHECK_TIMEOUT_SECONDS):
            await mongo.admin.command("ping")
    except PyMongoError as exc:
        logger.warning("readiness check failed for mongodb: %s", type(exc).__name__)
        return "error"
    return "ok"


def _check_broker(celery: Celery) -> str:
    """Blocking (kombu is synchronous), so the caller runs it in a worker thread."""
    try:
        with celery.connection_for_write() as connection:
            connection.ensure_connection(max_retries=0, timeout=CHECK_TIMEOUT_SECONDS)
    except Exception as exc:  # kombu raises many unrelated types for "cannot connect"
        logger.warning("readiness check failed for broker: %s", type(exc).__name__)
        return "error"
    return "ok"
