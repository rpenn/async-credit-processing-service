from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration read from environment variables (names are case-insensitive)."""

    model_config = SettingsConfigDict(extra="ignore")

    # Connection strings can carry credentials, so they are secrets: never printed or logged.
    mongo_uri: SecretStr
    broker_url: SecretStr
