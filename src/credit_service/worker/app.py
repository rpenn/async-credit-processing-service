"""Entry point for the Celery worker and beat processes (`celery -A credit_service.worker.app`)."""

import logging.config
from typing import Any

from celery import Task
from celery.signals import setup_logging, task_postrun, task_prerun

from credit_service.logs import CORRELATION_ID_HEADER, correlation_id, logging_config
from credit_service.messaging import create_celery_app
from credit_service.settings import Settings

settings = Settings()
app = create_celery_app(settings)
app.conf.include = ["credit_service.worker.tasks"]


@setup_logging.connect
def _configure_logging(loglevel: int | str | None = None, **_: Any) -> None:
    """JSON logs, as in the API. Connecting this signal stops Celery's own logging setup."""
    logging.config.dictConfig(logging_config(loglevel or "INFO"))


@task_prerun.connect
def _set_correlation_id(task_id: str, task: "Task[..., Any]", **_: Any) -> None:
    """Log under the ID of the request that published the task, else under the task's own ID
    (a scheduled reconciler run). Tasks this task publishes inherit it."""
    headers = task.request.headers or {}
    correlation_id.set(headers.get(CORRELATION_ID_HEADER) or task_id)


@task_postrun.connect
def _clear_correlation_id(**_: Any) -> None:
    correlation_id.set(None)
