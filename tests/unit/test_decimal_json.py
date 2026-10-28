"""The API's JSON parsing never turns an amount into a float, and rejects absurd numbers."""

from collections.abc import Iterator
from typing import Annotated, Any

import pytest
from fastapi import APIRouter, Body, FastAPI
from fastapi.testclient import TestClient

from credit_service.api.decimal_json import DecimalJSONRoute
from credit_service.models import CreditRequest


@pytest.fixture
def client() -> Iterator[TestClient]:
    router = APIRouter(route_class=DecimalJSONRoute)

    @router.post("/raw")
    async def raw(body: Annotated[Any, Body()]) -> dict[str, str]:
        return {"type": type(body["amount"]).__name__, "value": str(body["amount"])}

    @router.post("/validated")
    async def validated(body: CreditRequest) -> dict[str, str]:
        return {"amount": str(body.amount)}

    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as client:
        yield client


def post(client: TestClient, path: str, body: str) -> Any:
    return client.post(path, content=body.encode(), headers={"content-type": "application/json"})


def test_json_numbers_are_parsed_as_exact_decimals(client: TestClient) -> None:
    # As a float this would be 0.1: the extra digits would be silently lost.
    number = "0.1000000000000000055511151231257827"

    response = post(client, "/raw", f'{{"amount": {number}}}')

    assert response.json() == {"type": "Decimal", "value": number}


@pytest.mark.parametrize("number", ["NaN", "1e999999999", "1" * 41])
def test_invalid_or_absurd_numbers_are_rejected_as_invalid_json(
    client: TestClient, number: str
) -> None:
    response = post(client, "/validated", f'{{"user_id": "u_1", "amount": {number}}}')

    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "json_invalid"


def test_validation_error_for_a_large_number_still_returns_422(client: TestClient) -> None:
    # The 422 body echoes the input; this checks that encoding it does not fail.
    response = post(client, "/validated", '{"user_id": "u_1", "amount": 1e30}')

    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "decimal_max_digits"
