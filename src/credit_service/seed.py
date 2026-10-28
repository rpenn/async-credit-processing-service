"""Create test users so they can be credited. Safe to run more than once.

docker compose exec api python -m credit_service.seed u_123 u_456
"""

import argparse

from pydantic import TypeAdapter, ValidationError

from credit_service.models import UserId
from credit_service.mongo import USERS, create_sync_client, ensure_indexes
from credit_service.settings import Settings


def _user_id(value: str) -> str:
    """Same rule as the API, so every seeded user can be credited."""
    try:
        return TypeAdapter(UserId).validate_python(value)
    except ValidationError:
        raise argparse.ArgumentTypeError(
            f"{value!r}: use 1-64 letters, digits, '_' or '-'"
        ) from None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("user_ids", nargs="+", metavar="USER_ID", type=_user_id)
    user_ids: list[str] = parser.parse_args().user_ids

    with create_sync_client(Settings()) as client:
        db = client.get_default_database()
        ensure_indexes(db)
        for user_id in user_ids:
            # No balance or status yet: the first credit creates them.
            result = db[USERS].update_one(
                {"user_id": user_id},
                {"$setOnInsert": {"user_id": user_id, "pending_credits": 0}},
                upsert=True,
            )
            print(f"{'created' if result.upserted_id else 'exists '} {user_id}")


if __name__ == "__main__":
    main()
