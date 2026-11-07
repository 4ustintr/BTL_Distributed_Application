"""hq-api/main.py: /health (F7), NODE_URLS (F2), X-Internal-Key (F5). Không cần CSDL thật."""

import main
import pytest
from fastapi.testclient import TestClient

from common.constants import INTERNAL_KEY_HEADER

KEY = "khoa-test"
NODE_URLS = '{"1": "http://b:8001", "2": "http://b:8002", "3": "http://b:8003"}'


class FakePool:
    async def close(self) -> None:
        pass


@pytest.fixture
def client(monkeypatch):  # type: ignore[no-untyped-def]
    monkeypatch.setenv("INTERNAL_KEY", KEY)
    monkeypatch.setenv("HQ_DB_URL", "postgresql://x@127.0.0.1:1/hq_db")
    monkeypatch.setenv("NODE_URLS", NODE_URLS)

    async def fake_create_pool(*_a, **_k):  # type: ignore[no-untyped-def]
        return FakePool()

    monkeypatch.setattr(main.asyncpg, "create_pool", fake_create_pool)
    with TestClient(main.create_app()) as c:
        yield c


def test_health_ok(client: TestClient) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_node_clients_from_env(client: TestClient) -> None:
    nodes = client.app.state.nodes  # type: ignore[attr-defined]
    assert sorted(nodes) == [1, 2, 3]
    assert nodes[2].base_url.startswith("http://b:8002")
    assert len(client.app.state.worker_tasks) == 2  # type: ignore[attr-defined]


def test_parse_node_urls() -> None:
    assert main.parse_node_urls(NODE_URLS)[3] == "http://b:8003"


def test_callback_requires_key(client: TestClient) -> None:
    @client.app.post("/transfers/{transfer_id}/shipped")  # type: ignore[attr-defined]
    async def shipped(transfer_id: str) -> dict[str, str]:
        return {"id": transfer_id}

    assert client.post("/transfers/t1/shipped").status_code == 401
    r = client.post("/transfers/t1/shipped", headers={INTERNAL_KEY_HEADER: KEY})
    assert r.status_code == 200
