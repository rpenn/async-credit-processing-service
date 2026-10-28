from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from credit_service.api import decimal_json
from credit_service.models import CreditRequest


def parse(amount_json: str) -> Any:
    """Parse the way the API does: JSON numbers become Decimal, never float."""
    return decimal_json.loads(f'{{"user_id": "u_123", "amount": {amount_json}}}'.encode())


def error_types(amount_json: str) -> list[str]:
    with pytest.raises(ValidationError) as exc_info:
        CreditRequest.model_validate(parse(amount_json))
    return [error["type"] for error in exc_info.value.errors()]


@pytest.mark.parametrize(
    ("amount_json", "expected"),
    [
        ("10", "10.00"),  # JSON number
        ('"10.5"', "10.50"),  # JSON string
        ("10.000", "10.00"),  # trailing zeros are still two places of value
        ("1e2", "100.00"),  # exponent syntax is valid for JSON numbers
        ("0.01", "0.01"),  # smallest amount
        ("999999999999999.99", "999999999999999.99"),  # largest the model allows
    ],
)
def test_valid_amounts_become_exact_two_place_decimals(amount_json: str, expected: str) -> None:
    amount = CreditRequest.model_validate(parse(amount_json)).amount

    assert isinstance(amount, Decimal)
    assert str(amount) == expected


@pytest.mark.parametrize(
    ("amount_json", "error_type"),
    [
        ("0", "greater_than"),
        ('"-5"', "greater_than"),  # the string format allows "-" so this error is clear
        ("10.001", "decimal_max_places"),
        ("1000000000000000", "decimal_whole_digits"),
        ("0.1000000000000000055511151231257827", "decimal_max_digits"),
        # Python's Decimal() accepts all of these strings; our format does not.
        ('"NaN"', "decimal_parsing"),
        ('"1e2"', "decimal_parsing"),
        ('"1_000"', "decimal_parsing"),
        ('" 10"', "decimal_parsing"),
        ("true", "decimal_type"),  # bool is an int subclass in Python
        ("null", "decimal_type"),
    ],
)
def test_invalid_amounts_are_rejected(amount_json: str, error_type: str) -> None:
    assert error_types(amount_json) == [error_type]


def test_float_is_rejected_even_from_python_callers() -> None:
    with pytest.raises(ValidationError) as exc_info:
        CreditRequest.model_validate({"user_id": "u_123", "amount": 10.5})

    assert exc_info.value.errors()[0]["type"] == "decimal_type"


@pytest.mark.parametrize("user_id", ["", "a" * 65, "$where"])
def test_invalid_user_ids_are_rejected(user_id: str) -> None:
    with pytest.raises(ValidationError):
        CreditRequest.model_validate({"user_id": user_id, "amount": "10"})


def test_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError) as exc_info:
        CreditRequest.model_validate({"user_id": "u_123", "amount": "10", "balance": "1"})

    assert exc_info.value.errors()[0]["type"] == "extra_forbidden"
