"""Money stays a `Decimal` in Python and a BSON `Decimal128` in MongoDB. Never a float.

Convert only at the persistence boundary, with the two helpers below.
"""

from decimal import Decimal

from bson.decimal128 import Decimal128

CENT = Decimal("0.01")


def to_cents(amount: Decimal) -> Decimal:
    """Same value with exactly two decimal places, e.g. 10 -> 10.00, 1.5 -> 1.50.

    Callers have already rejected amounts with more than two places, so nothing is rounded.
    """
    return amount.quantize(CENT)


def to_decimal128(amount: Decimal) -> Decimal128:
    return Decimal128(amount)


def from_decimal128(value: Decimal128) -> Decimal:
    return value.to_decimal()
