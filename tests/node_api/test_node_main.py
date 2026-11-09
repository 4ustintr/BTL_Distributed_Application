"""node-api/main.py: /health (F6, F9), X-Internal-Key (F5), CORS. Không cần CSDL thật."""

import main
import pytest
from fastapi.testclient import TestClient

from common.constants import INTERNAL_KEY_HEADER

KEY = "khoa-test"


class FakePool:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail

    async def fetchval(self, _sql: str) -> int:
        if self.fail:
            raise OSError("db down")
        return 1

    async def close(self) -> None:
        pass


@pytest.fixture
def make_client(monkeypatch):  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DC_ID", "2")
    monkeypatch.setenv("INTERNAL_KEY", KEY)
    monkeypatch.setenv("NODE_DB_URL", "postgresql://x@127.0.0.1:1/dc2")
    monkeypatch.setenv("HQ_URL", "http://127.0.0.1:1")

    def factory(pool: FakePool) -> TestClient:
        async def fake_create_pool(*_a, **_k):  # type: ignore[no-untyped-def]
            return pool

        monkeypatch.setattr(main.asyncpg, "create_pool", fake_create_pool)
        return TestClient(main.create_app())

    return factory


def test_health_starts_recovering(make_client) -> None:  # type: ignore[no-untyped-def]
    with make_client(FakePool()) as c:
        r = c.get("/health")
    assert r.status_code == 200
    assert r.json() == {"dc_id": 2, "mode": "RECOVERING", "db": "ok"}


def test_health_503_when_db_down(make_client) -> None:  # type: ignore[no-untyped-def]
    with make_client(FakePool(fail=True)) as c:
        r = c.get("/health")
    assert r.status_code == 503
    assert r.json()["mode"] == "RECOVERING"


def test_internal_route_requires_key(make_client) -> None:  # type: ignore[no-untyped-def]
    with make_client(FakePool()) as c:

        @c.app.post("/replay")  # route giả: chỉ kiểm khóa, không phụ thuộc /replay thật (F10.a)
        async def replay() -> dict[str, bool]:
            return {"ok": True}

        # Đưa route giả lên đầu để thắng route /replay thật (nếu đã có) khi khớp đường dẫn
        routes = c.app.router.routes
        routes.insert(0, routes.pop())

        assert c.post("/replay").status_code == 401
        assert c.post("/replay", headers={INTERNAL_KEY_HEADER: KEY}).status_code == 200
        assert c.get("/health").status_code == 200  # /health không cần khóa


def test_cors_for_wms_origin(make_client) -> None:  # type: ignore[no-untyped-def]
    with make_client(FakePool()) as c:
        r = c.get("/health", headers={"Origin": "http://192.168.1.20:3002"})
        assert r.headers["access-control-allow-origin"] == "http://192.168.1.20:3002"
        r = c.get("/health", headers={"Origin": "http://192.168.1.20:9999"})
        assert "access-control-allow-origin" not in r.headers
