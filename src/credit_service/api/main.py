from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from pymongo import AsyncMongoClient

from credit_service.api import health
from credit_service.messaging import create_celery_app
from credit_service.settings import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    """App factory. Uvicorn calls it with no arguments (`--factory`); tests pass settings."""
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        mongo: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
            settings.mongo_uri.get_secret_value()
        )
        app.state.mongo = mongo
        app.state.celery = create_celery_app(settings)
        yield
        await mongo.close()

    app = FastAPI(title="Credit Service", lifespan=lifespan)
    app.include_router(health.router)
    return app
