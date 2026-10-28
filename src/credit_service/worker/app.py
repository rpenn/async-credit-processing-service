"""Entry point for the Celery worker and beat processes (`celery -A credit_service.worker.app`)."""

from credit_service.messaging import create_celery_app
from credit_service.settings import Settings

settings = Settings()
app = create_celery_app(settings)
app.conf.include = ["credit_service.worker.tasks"]
