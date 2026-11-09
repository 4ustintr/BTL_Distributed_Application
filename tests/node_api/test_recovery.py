"""node-api/recovery.py: POST /recovery/complete (F6, F9). Không cần CSDL thật."""

import logging
import time

import main
import pytest
import recovery
from fastapi.testclient import TestClient

from common.constants import INTERNAL_KEY_HEADER, NodeMode

KEY = "khoa-test"


class FakePool:
    async def fetchval(self, _sql: str) -> int:
        return 1

    async def close(self) -> None:
        pass


@pytest.fixture
def client_factory(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    """Dựng Node API; tham số là RECOVERING_WARN_AFTER_S dùng cho lần dựng đó."""
    monkeypatch.setenv("DC_ID", "2")
    monkeypatch.setenv("INTERNAL_KEY", KEY)
    monkeypatch.setenv("NODE_DB_URL", "postgresql://x@127.0.0.1:1/dc2")
    monkeypatch.setenv("HQ_URL", "http://127.0.0.1:1")

    async def fake_create_pool(*_a, **_k):  # type: ignore[no-untyped-def]
        return FakePool()

    monkeypatch.setattr(main.asyncpg, "create_pool", fake_create_pool)

    def factory(warn_after_s: float = 60.0) -> TestClient:
        monkeypatch.setattr(recovery, "RECOVERING_WARN_AFTER_S", warn_after_s)
        return TestClient(main.create_app())

    return factory


@pytest.fixture
def client(client_factory):  # type: ignore[no-untyped-def]
    with client_factory() as c:
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


# ---------------------------------------------------------------------------
# F11: kho ở RECOVERING quá RECOVERING_WARN_AFTER_S giây thì ghi cảnh báo vào log
# ---------------------------------------------------------------------------
def test_warns_when_still_recovering_after_threshold(
    client_factory, caplog: pytest.LogCaptureFixture
) -> None:  # type: ignore[no-untyped-def]
    with caplog.at_level(logging.WARNING, logger="node-api"), client_factory(0.05):
        time.sleep(0.4)
    warnings = [r for r in caplog.records if "RECOVERING" in r.getMessage()]
    assert len(warnings) == 1  # chỉ cảnh báo một lần
    assert "2" in warnings[0].getMessage()  # có mã kho


def test_no_warning_when_recovery_completes_in_time(
    client_factory, caplog: pytest.LogCaptureFixture
) -> None:  # type: ignore[no-untyped-def]
    with caplog.at_level(logging.WARNING, logger="node-api"), client_factory(0.3) as c:
        c.post("/recovery/complete", headers={INTERNAL_KEY_HEADER: KEY})
        time.sleep(0.6)
    assert not [r for r in caplog.records if "RECOVERING" in r.getMessage()]


def test_shutdown_cancels_pending_warning(client_factory) -> None:  # type: ignore[no-untyped-def]
    with client_factory(60):  # rời khối with phải không bị treo chờ 60 giây
        pass
