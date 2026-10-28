import io
import json
import logging
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pytest

from credit_service.logs import CorrelationIdFilter, JsonFormatter, correlation_id


@pytest.fixture
def logger() -> Iterator[logging.Logger]:
    """A logger kept apart from the root logger, which other tests reconfigure."""
    logger = logging.getLogger("test_logs")
    logger.propagate = False
    logger.setLevel(logging.INFO)
    yield logger
    logger.handlers.clear()


def _output(logger: logging.Logger) -> io.StringIO:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(CorrelationIdFilter())
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return stream


def _records(stream: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_a_record_is_one_json_line_with_its_extras_and_the_correlation_id(
    logger: logging.Logger,
) -> None:
    stream = _output(logger)
    token = correlation_id.set("req-1")
    try:
        logger.warning("credit\naccepted", extra={"credit_id": "c1", "amount": Decimal("0.10")})
    finally:
        correlation_id.reset(token)

    [record] = _records(stream)  # the newline in the message did not split the line
    assert record | {"time": ""} == {
        "time": "",
        "level": "WARNING",
        "logger": "test_logs",
        "message": "credit\naccepted",
        "correlation_id": "req-1",
        "credit_id": "c1",
        "amount": "0.10",  # exact, never a float
    }


def test_outside_a_request_or_task_the_correlation_id_is_null(logger: logging.Logger) -> None:
    stream = _output(logger)
    logger.warning("starting")

    [record] = _records(stream)
    assert record["correlation_id"] is None


def test_an_exception_is_included_in_the_same_line(logger: logging.Logger) -> None:
    stream = _output(logger)
    try:
        raise ValueError("boom")
    except ValueError:
        logger.exception("failed")

    [record] = _records(stream)
    assert record["exception"].endswith("ValueError: boom")
