"""Celery app shared by the API (publisher) and the worker (consumer).

The API only publishes tasks by name, so it never imports worker code.
"""

from celery import Celery

from credit_service.settings import Settings

# The worker registers its task under this name; the API publishes to it with send_task.
APPLY_CREDIT_TASK = "credit_service.apply_credit"


def create_celery_app(settings: Settings) -> Celery:
    app = Celery("credit_service", broker=settings.broker_url.get_secret_value())
    app.conf.update(
        # Remote-control queues (used by `celery inspect ping`) are non-durable. RabbitMQ 4.3
        # refuses non-durable queues unless they are exclusive (`transient_nonexcl_queues`).
        control_queue_exclusive=True,
    )
    return app
