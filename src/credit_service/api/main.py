from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

from credit_service.api import credits, health
from credit_service.api.problems import ProblemError, problem_handler, validation_error_handler
from credit_service.messaging import create_celery_app
from credit_service.mongo import create_async_client, ensure_indexes_async
from credit_service.settings import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    """App factory. Uvicorn calls it with no arguments (`--factory`); tests pass settings."""
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        mongo = create_async_client(settings)
        # Fail startup (and let the container restart) rather than serve without indexes:
        # the unique indexes are what enforce one user per user_id and one credit per key.
        await ensure_indexes_async(mongo.get_default_database())
        app.state.settings = settings
        app.state.mongo = mongo
        app.state.celery = create_celery_app(settings)
        yield
        await mongo.close()

    app = FastAPI(
        title="Credit Service",
        lifespan=lifespan,
        exception_handlers={
            ProblemError: problem_handler,
            RequestValidationError: validation_error_handler,
        },
    )
    app.include_router(health.router)
    app.include_router(credits.router)
    return app
