"""Structured logs: one JSON object per line on stdout, for the API, the worker, and beat.

Every line carries the current correlation ID. It is set per HTTP request
(api/correlation.py), travels in the task message header (messaging.py), and is set again
for each task the worker runs (worker/app.py), so one credit can be traced from the request
to the worker.
"""

import json
import logging
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

# The HTTP request and response header, and the task message header that carries it on.
# Not "correlation_id": Celery already uses that name for the task ID.
CORRELATION_ID_HEADER = "x-correlation-id"

correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)

# Attributes every LogRecord has. Anything else on a record came from `extra=`.
_STANDARD_ATTRIBUTES = {*vars(logging.makeLogRecord({})), "message", "asctime"}


class CorrelationIdFilter(logging.Filter):
    """Adds the current correlation ID to each record (the logging cookbook's pattern)."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id.get()
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        entry.update((k, v) for k, v in vars(record).items() if k not in _STANDARD_ATTRIBUTES)
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        # json.dumps escapes newlines, so one record is always one line. default=str covers
        # values JSON has no type for, such as Decimal and datetime.
        return json.dumps(entry, default=str)


def logging_config(level: str | int = "INFO") -> dict[str, Any]:
    """A logging.config.dictConfig dict: one JSON handler on the root logger.

    Library loggers (uvicorn, celery, pymongo) propagate to the root, so they come out as
    JSON too.
    """
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {"correlation_id": {"()": CorrelationIdFilter}},
        "formatters": {"json": {"()": JsonFormatter}},
        "handlers": {
            "stdout": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "filters": ["correlation_id"],
                "formatter": "json",
            }
        },
        "root": {"level": level, "handlers": ["stdout"]},
    }
