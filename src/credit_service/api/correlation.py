import re
import uuid

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from credit_service.logs import CORRELATION_ID_HEADER, correlation_id

# A client-sent ID ends up in logs and task headers, so only short, plain values are kept.
_CLIENT_ID = re.compile(r"[A-Za-z0-9._-]{1,64}")


class CorrelationIdMiddleware:
    """One correlation ID per request: the client's X-Correlation-ID if valid, else a new one.

    It is set for every log line of the request (and every task it publishes) and returned in
    the response header. Pure ASGI rather than BaseHTTPMiddleware, which Starlette documents
    as breaking contextvars propagation.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        sent = Headers(scope=scope).get(CORRELATION_ID_HEADER)
        value = sent if sent is not None and _CLIENT_ID.fullmatch(sent) else str(uuid.uuid4())

        async def send_with_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)[CORRELATION_ID_HEADER] = value
            await send(message)

        token = correlation_id.set(value)
        try:
            await self.app(scope, receive, send_with_id)
        finally:
            correlation_id.reset(token)
