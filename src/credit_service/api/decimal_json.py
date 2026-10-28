"""Parse JSON request bodies so numbers become `Decimal`, never `float`.

Starlette's default `Request.json()` turns `10.1` into a float, which can silently change a
money value before validation runs. Pattern from
https://fastapi.tiangolo.com/how-to/custom-request-and-route/
"""

import json
from collections.abc import Callable, Coroutine
from decimal import Decimal
from typing import Any

from fastapi import Request, Response
from fastapi.routing import APIRoute

# Far beyond any valid amount, but small enough that a 422 response can echo the input back.
# Without a bound, `1e999999999` parses fine and then breaks the error response.
_MAX_NUMBER_LENGTH = 40
_MAX_EXPONENT = 40


def _parse_number(token: str) -> Decimal:
    number = Decimal(token)
    if len(token) > _MAX_NUMBER_LENGTH or abs(number.adjusted()) > _MAX_EXPONENT:
        raise ValueError("Number is out of range")
    return number


def _reject_constant(token: str) -> None:
    # Python's json module accepts NaN and Infinity, but they are not valid JSON (RFC 8259).
    raise ValueError(f"{token} is not valid JSON")


def loads(body: bytes) -> Any:
    """Like json.loads, but every number is a bounded Decimal.

    Problems raise JSONDecodeError so FastAPI answers 422 (json_invalid), as for bad syntax.
    """
    try:
        return json.loads(
            body,
            parse_float=_parse_number,
            parse_int=_parse_number,
            parse_constant=_reject_constant,
        )
    except json.JSONDecodeError:
        raise
    except ValueError as exc:
        raise json.JSONDecodeError(str(exc), body.decode(errors="replace"), 0) from exc


class DecimalJSONRequest(Request):
    async def json(self) -> Any:
        if not hasattr(self, "_json"):
            self._json = loads(await self.body())
        return self._json


class DecimalJSONRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def decimal_json_handler(request: Request) -> Response:
            return await handler(DecimalJSONRequest(request.scope, request.receive))

        return decimal_json_handler
