from decimal import Decimal

from pydantic import SecretStr
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
