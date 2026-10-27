# One image for the API, the Celery worker, and Celery beat.
# Pattern follows https://docs.astral.sh/uv/guides/integration/docker/

# --- Build stage: resolve dependencies from the lockfile into /app/.venv ---
FROM python:3.13.18-slim-trixie AS builder
COPY --from=ghcr.io/astral-sh/uv:0.13.6 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_NO_DEV=1

WORKDIR /app

# Dependencies first, so this layer is cached until uv.lock changes.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-editable

COPY pyproject.toml uv.lock ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable

# --- Runtime stage: only the virtualenv, run as an unprivileged user ---
FROM python:3.13.18-slim-trixie

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --no-create-home app

COPY --from=builder /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

USER app
WORKDIR /app
EXPOSE 8000

CMD ["uvicorn", "credit_service.api.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
