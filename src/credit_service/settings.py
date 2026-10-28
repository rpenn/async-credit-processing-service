from decimal import Decimal

from pydantic import PositiveInt, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from credit_service.models import Amount


class Settings(BaseSettings):
    """Configuration read from environment variables (names are case-insensitive)."""

    model_config = SettingsConfigDict(extra="ignore")

    # Connection strings can carry credentials, so they are secrets: never printed or logged.
    mongo_uri: SecretStr
    broker_url: SecretStr

    # Business limit for one credit. Typed as Amount so the limit itself is a valid amount.
    max_credit_amount: Amount = Decimal("1000000.00")

    # Reconciler: every interval, re-enqueue credits still pending after the threshold.
    # The threshold must outlast the worker's own retries (about 90 s at worst), or the
    # reconciler re-enqueues credits the worker is still retrying (harmless, but wasted).
    reconcile_interval_seconds: PositiveInt = 60
    reconcile_stale_after_seconds: PositiveInt = 300
    # After this many re-enqueues a still-pending credit is marked failed.
    reconcile_max_attempts: PositiveInt = 3
