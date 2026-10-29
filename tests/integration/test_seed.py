import sys
from datetime import datetime

import pytest
from pymongo.database import Database

from credit_service import seed
from credit_service.mongo import USERS, Document
from credit_service.settings import Settings


def test_seed_creates_users_once(
    settings: Settings,
    db: Database[Document],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("MONGO_URI", settings.mongo_uri.get_secret_value())
    monkeypatch.setenv("BROKER_URL", settings.broker_url.get_secret_value())
    monkeypatch.setattr(sys, "argv", ["seed", "u_1", "u_2"])

    seed.main()
    seed.main()

    assert capsys.readouterr().out.split("\n") == [
        "created u_1",
        "created u_2",
        "exists  u_1",
        "exists  u_2",
        "",
    ]
    assert db[USERS].count_documents({}) == 2
    user = db[USERS].find_one({"user_id": "u_1"}, {"_id": 0})
    assert user is not None
    created_at = user.pop("created_at")
    assert isinstance(created_at, datetime)
    # The second run left the user alone, so both timestamps are still the first run's.
    assert user == {"user_id": "u_1", "pending_credits": 0, "updated_at": created_at}
