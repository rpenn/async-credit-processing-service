"""Run the API: python -m credit_service.api

Started from Python rather than the uvicorn command so that uvicorn's own loggers (startup,
access log) use the same JSON logging config as the rest of the app.
"""

import uvicorn

from credit_service.logs import logging_config

uvicorn.run(
    "credit_service.api.main:create_app",
    factory=True,
    host="0.0.0.0",  # noqa: S104 - inside the container; Compose publishes it to 127.0.0.1 only
    port=8000,
    log_config=logging_config(),
)
