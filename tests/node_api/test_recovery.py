"""node-api/recovery.py: POST /recovery/complete (F6, F9). Không cần CSDL thật."""

import main
import pytest
from fastapi.testclient import TestClient

from common.constants import INTERNAL_KEY_HEADER, NodeMode

KEY = "khoa-test"


class FakePool:
    async def fetchval(self, _sql: str) -> int:
        return 1

    async def close(self) -> None:
        pass


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DC_ID", "2")
    monkeypatch.setenv("INTERNAL_KEY", KEY)
    monkeypatch.setenv("NODE_DB_URL", "postgresql://x@127.0.0.1:1/dc2")
    monkeypatch.setenv("HQ_URL", "http://127.0.0.1:1")

    async def fake_create_pool(*_a, **_k):  # type: ignore[no-untyped-def]
        return FakePool()

    monkeypatch.setattr(main.asyncpg, "create_pool", fake_create_pool)
    with TestClient(main.create_app()) as c:
        yield c


def test_recovery_complete_switches_to_online(client: TestClient) -> None:
    assert client.app.state.mode == NodeMode.RECOVERING  # F9: khởi động luôn ở RECOVERING
    r = client.post("/recovery/complete", headers={INTERNAL_KEY_HEADER: KEY})
    assert r.status_code == 200
    assert r.json() == {"mode": "ONLINE"}
    assert client.get("/health").json()["mode"] == "ONLINE"


def test_recovery_complete_is_idempotent(client: TestClient) -> None:
    for _ in range(2):
        r = client.post("/recovery/complete", headers={INTERNAL_KEY_HEADER: KEY})
        assert r.status_code == 200 and r.json() == {"mode": "ONLINE"}


def test_recovery_complete_requires_internal_key(client: TestClient) -> None:
    assert client.post("/recovery/complete").status_code == 401
    assert client.app.state.mode == NodeMode.RECOVERING
