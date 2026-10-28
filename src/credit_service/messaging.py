"""Celery app shared by the API (publisher) and the worker (consumer).

Both sides need the same queue settings, so all configuration lives here. The API only
publishes tasks by name, so it never imports worker code.
"""

from celery import Celery

from credit_service.settings import Settings

# The worker registers its task under this name; the API publishes to it with send_task.
APPLY_CREDIT_TASK = "credit_service.apply_credit"

# Must match the policy pattern in rabbitmq/definitions.json, which adds the delivery limit
# and the dead-letter exchange to this queue.
TASK_QUEUE = "credits"


def create_celery_app(settings: Settings) -> Celery:
    app = Celery("credit_service", broker=settings.broker_url.get_secret_value())
    app.conf.update(
        # JSON only: the worker never deserializes pickle, which could run arbitrary code.
        task_serializer="json",
        accept_content=["json"],
        # No result backend: the state of every credit lives in MongoDB.
        task_ignore_result=True,
        # One durable, replicated quorum queue. Retry countdowns on quorum queues use
        # RabbitMQ's native delayed delivery, which needs a topic exchange.
        task_default_queue=TASK_QUEUE,
        task_default_queue_type="quorum",
        task_default_exchange_type="topic",
        # Publisher confirms: a publish returns only once the broker has the message.
        broker_transport_options={"confirm_publish": True},
        broker_connection_retry_on_startup=True,
        # Ack only after the task finishes, so a crash mid-task redelivers the message.
        task_acks_late=True,
        # Also redeliver if the worker process dies. The queue's delivery limit stops a
        # message that keeps crashing workers from looping forever.
        task_reject_on_worker_lost=True,
        # An unexpected error rejects the message (without requeue) into the dead-letter
        # queue for inspection, instead of acking it away. The credit stays pending.
        task_acks_on_failure_or_timeout=False,
        # On a lost broker connection the message is redelivered anyway, so stop the
        # running copy rather than let two run. (Celery 6 makes this the default.)
        worker_cancel_long_running_tasks_on_connection_loss=True,
        # Take one message at a time, so a slow task does not hold others hostage.
        worker_prefetch_multiplier=1,
        # Remote-control queues (used by `celery inspect ping`) are non-durable. RabbitMQ 4.3
        # refuses non-durable queues unless they are exclusive (`transient_nonexcl_queues`).
        control_queue_exclusive=True,
    )
    return app
