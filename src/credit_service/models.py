import re
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any

from pydantic import AfterValidator, BaseModel, BeforeValidator, ConfigDict, Field
from pydantic_core import PydanticCustomError

from credit_service.money import to_cents


class CreditStatus(StrEnum):
    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"


class UserStatus(StrEnum):
    PROCESSING = "processing"
    PROCESSED = "processed"


# Plain decimal notation only: no exponents, spaces, underscores, "+", "NaN" or "Infinity".
# A leading "-" is allowed here so that negatives fail with the clearer "greater than 0" error.
_AMOUNT_STRING = re.compile(r"-?\d{1,20}(\.\d{1,20})?")


def _parse_amount(value: Any) -> Any:
    """Accept a JSON string or number. Decimal and int pass through; everything else fails.

    JSON numbers arrive as Decimal because the API parses request bodies with
    `parse_float=Decimal` (see api/decimal_json.py), so a float here is a programming error.
    """
    if isinstance(value, str):
        if not _AMOUNT_STRING.fullmatch(value):
            raise PydanticCustomError(
                "decimal_parsing", 'Amount must be a decimal number such as "10.50"'
            )
        return Decimal(value)
    if isinstance(value, bool) or not isinstance(value, int | Decimal):
        raise PydanticCustomError("decimal_type", "Amount must be a JSON string or number")
    return value


UserId = Annotated[
    str,
    Field(pattern=r"^[A-Za-z0-9_-]{1,64}$", examples=["u_123"]),
]

# Positive, finite (pydantic rejects NaN and Infinity), at most 2 decimal places, at most
# 15 integer digits. The configurable business maximum is checked by the API.
# Field comes first so pydantic applies it to the Decimal itself (and emits valid JSON Schema).
Amount = Annotated[
    Decimal,
    Field(gt=0, max_digits=17, decimal_places=2, examples=["150.00"]),
    BeforeValidator(_parse_amount),
    AfterValidator(to_cents),
]


class CreditRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: UserId
    amount: Amount


class CreditResponse(BaseModel):
    credit_id: str
    user_id: str
    amount: Decimal  # serialized as a JSON string, e.g. "150.00"
    status: CreditStatus
    created_at: datetime
    updated_at: datetime
