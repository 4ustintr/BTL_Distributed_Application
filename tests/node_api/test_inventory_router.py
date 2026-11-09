"""node-api/routers/inventory.py: GET /inventory, POST /inbound, POST /outbound (F6, F10.a).

Unit test: router chạy với pool giả; các hàm của services/stock.py được thay bằng hàm giả ghi lại
thứ tự gọi. Hợp đồng của stock.py xem docstring của test_stock.py.
"""

import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import main
import pytest
from fastapi.testclient import TestClient
from node_fakes import FakeConn, FakePool
from services import stock

from common.constants import SQL_SET_APP_SOURCE, ErrorCode, NodeMode
from common.errors import AppError

DC_ID = 3
REQ = "22222222-2222-4222-8222-222222222222"
WHO = "tester"


def result(qty_on_hand: int = 15, qty_reserved: int = 0, version: int = 4) -> SimpleNamespace:
    return SimpleNamespace(
        qty_on_hand=qty_on_hand, qty_reserved=qty_reserved, version=version, duplicate=False
    )


def inbound_body(**over: Any) -> dict[str, Any]:
    body = {"request_id": REQ, "product_id": 1, "qty": 10, "ref_type": "RECEIPT", "created_by": WHO}
    return {**body, **over}


def outbound_body(**over: Any) -> dict[str, Any]:
    body = {"request_id": REQ, "product_id": 1, "qty": 5, "ref_type": "ISSUE", "created_by": WHO}
    return {**body, **over}


class Env(SimpleNamespace):
    client: TestClient
    conn: FakeConn
    events: list[str]
    kw: dict[str, dict[str, Any]]  # tên hàm stock -> tham số keyword của lần gọi cuối
    fake: Any  # fake(name, returns=None, raises=None): thay một hàm của services/stock.py


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DC_ID", str(DC_ID))
    monkeypatch.setenv("INTERNAL_KEY", "khoa-test")
    monkeypatch.setenv("NODE_DB_URL", "postgresql://x@127.0.0.1:1/dc3")
    monkeypatch.setenv("HQ_URL", "http://127.0.0.1:1")

    events: list[str] = []
    conn = FakeConn(events=events)
    pool = FakePool(conn)

    async def fake_create_pool(*_a: Any, **_k: Any) -> FakePool:
        return pool

    monkeypatch.setattr(main.asyncpg, "create_pool", fake_create_pool)

    kw: dict[str, dict[str, Any]] = {}

    def fake(name: str, returns: Any = None, raises: Exception | None = None) -> None:
        async def fn(_conn: Any, *args: Any, **kwargs: Any) -> Any:
            events.append(name)
            kw[name] = kwargs
            if raises is not None:
                raise raises
            return returns

        monkeypatch.setattr(stock, name, fn)

    e = Env(conn=conn, events=events, kw=kw, fake=fake)
    # mặc định: không trùng, SKU có trong danh mục, ghi thành công
    fake("movement_exists", False)
    fake("product_exists", True)
    fake("add_stock", result(20, 0, 2))
    fake("consume", result(15, 0, 4))
    fake("write_outbox")
    fake("current_stock", result(20, 0, 7))
    fake("list_inventory", [])
    with TestClient(main.create_app()) as client:
        client.app.state.mode = NodeMode.ONLINE
        e.client = client
        yield e


# ---------------------------------------------------------------------------
# POST /inbound
# ---------------------------------------------------------------------------
def test_inbound_ok_follows_f10a_order(env: Env) -> None:
    r = env.client.post("/inbound", json=inbound_body())
    assert r.status_code == 200
    assert r.json() == {"qty_on_hand": 20, "version": 2, "duplicate": False}
    # bước 0 SET LOCAL, bước 1 kiểm trùng, kiểm SKU (G3), bước 2–3 nhập, bước 4 outbox
    assert env.events == [
        "sql:set_local",
        "movement_exists",
        "product_exists",
        "add_stock",
        "write_outbox",
    ]
    assert env.conn.calls[0].sql == SQL_SET_APP_SOURCE.lower()
    assert [t.committed for t in env.conn.transactions] == [True]


def test_inbound_passes_request_fields_and_outbox_values(env: Env) -> None:
    env.client.post("/inbound", json=inbound_body())
    add = env.kw["add_stock"]
    assert (add["dc_id"], add["product_id"], add["qty"]) == (DC_ID, 1, 10)
    assert str(add["request_id"]) == REQ and add["created_by"] == WHO
    assert str(add["ref_type"]) == "RECEIPT"
    out = env.kw["write_outbox"]
    assert (out["dc_id"], out["product_id"]) == (DC_ID, 1)
    assert (out["qty_on_hand"], out["qty_reserved"], out["version"]) == (20, 0, 2)
    assert str(out["request_id"]) == REQ and str(out["ref_type"]) == "RECEIPT"


def test_inbound_needs_no_internal_key(env: Env) -> None:
    # /inbound do WMS gọi, không nằm trong INTERNAL_ROUTES (F5, F6)
    assert env.client.post("/inbound", json=inbound_body()).status_code == 200


def test_inbound_503_while_recovering(env: Env) -> None:
    env.client.app.state.mode = NodeMode.RECOVERING
    r = env.client.post("/inbound", json=inbound_body())
    assert r.status_code == 503
    assert r.json()["error_code"] == ErrorCode.DC_RECOVERING
    assert not {"movement_exists", "add_stock", "write_outbox"} & set(env.events)


def test_inbound_duplicate_at_step1_returns_current_stock(env: Env) -> None:
    env.fake("movement_exists", True)
    r = env.client.post("/inbound", json=inbound_body())
    assert r.status_code == 200
    assert r.json() == {"qty_on_hand": 20, "version": 7, "duplicate": True}  # G4
    assert not {"add_stock", "consume", "write_outbox"} & set(env.events)


def test_inbound_duplicate_at_step3_rolls_back(env: Env) -> None:
    env.fake("add_stock", raises=stock.DuplicateRequest())
    r = env.client.post("/inbound", json=inbound_body())
    assert r.status_code == 200
    assert r.json() == {"qty_on_hand": 20, "version": 7, "duplicate": True}
    assert "write_outbox" not in env.events
    assert any(t.rolled_back for t in env.conn.transactions)


def test_inbound_unknown_product_is_validation_error(env: Env) -> None:
    env.fake("product_exists", False)
    r = env.client.post("/inbound", json=inbound_body(product_id=99))
    assert r.status_code == 400
    assert r.json()["error_code"] == ErrorCode.VALIDATION_ERROR
    assert "add_stock" not in env.events


@pytest.mark.parametrize(
    "body",
    [
        inbound_body(qty=0),
        inbound_body(qty=-3),
        inbound_body(ref_type="ISSUE"),
        {k: v for k, v in inbound_body().items() if k != "request_id"},
        inbound_body(request_id="khong-phai-uuid"),
    ],
)
def test_inbound_invalid_body_is_400_before_any_sql(env: Env, body: dict[str, Any]) -> None:
    r = env.client.post("/inbound", json=body)
    assert r.status_code == 400
    assert r.json()["error_code"] == ErrorCode.VALIDATION_ERROR
    assert env.events == []


# ---------------------------------------------------------------------------
# POST /outbound
# ---------------------------------------------------------------------------
def test_outbound_ok_follows_f10a_order(env: Env) -> None:
    r = env.client.post("/outbound", json=outbound_body())
    assert r.status_code == 200
    assert r.json() == {"qty_on_hand": 15, "version": 4, "duplicate": False}
    assert env.events == [
        "sql:set_local",
        "movement_exists",
        "product_exists",
        "consume",
        "write_outbox",
    ]
    assert [t.committed for t in env.conn.transactions] == [True]
    call = env.kw["consume"]
    assert (call["dc_id"], call["product_id"], call["qty"]) == (DC_ID, 1, 5)
    assert str(call["ref_type"]) == "ISSUE" and call["created_by"] == WHO
    out = env.kw["write_outbox"]
    assert (out["qty_on_hand"], out["version"]) == (15, 4) and str(out["ref_type"]) == "ISSUE"


def test_outbound_503_while_recovering(env: Env) -> None:
    env.client.app.state.mode = NodeMode.RECOVERING
    r = env.client.post("/outbound", json=outbound_body())
    assert r.status_code == 503
    assert r.json()["error_code"] == ErrorCode.DC_RECOVERING
    assert "consume" not in env.events


def test_outbound_duplicate_check_runs_before_consume(env: Env) -> None:
    # F10.a bước 1: lệnh đã áp dụng gửi lại lúc tồn không còn đủ không được nhận 422
    env.fake("movement_exists", True)
    env.fake("consume", raises=AppError(ErrorCode.INSUFFICIENT_STOCK, "không đủ"))
    r = env.client.post("/outbound", json=outbound_body())
    assert r.status_code == 200 and r.json()["duplicate"] is True
    assert "consume" not in env.events


def test_outbound_duplicate_at_step3_rolls_back(env: Env) -> None:
    env.fake("consume", raises=stock.DuplicateRequest())
    r = env.client.post("/outbound", json=outbound_body())
    assert r.status_code == 200 and r.json()["duplicate"] is True
    assert "write_outbox" not in env.events
    assert any(t.rolled_back for t in env.conn.transactions)


def test_outbound_insufficient_stock_is_422_and_rolls_back(env: Env) -> None:
    env.fake("consume", raises=AppError(ErrorCode.INSUFFICIENT_STOCK, "không đủ hàng"))
    r = env.client.post("/outbound", json=outbound_body(qty=30))
    assert r.status_code == 422
    body = r.json()
    assert body["error_code"] == ErrorCode.INSUFFICIENT_STOCK
    assert {"error_code", "message", "details"} <= body.keys()
    assert "write_outbox" not in env.events
    assert any(t.rolled_back for t in env.conn.transactions)


def test_outbound_version_conflict_is_409(env: Env) -> None:
    env.fake("consume", raises=AppError(ErrorCode.VERSION_CONFLICT, "xung đột version"))
    r = env.client.post("/outbound", json=outbound_body())
    assert r.status_code == 409
    assert r.json()["error_code"] == ErrorCode.VERSION_CONFLICT


def test_outbound_unknown_product_is_validation_error(env: Env) -> None:
    env.fake("product_exists", False)
    r = env.client.post("/outbound", json=outbound_body(product_id=99))
    assert r.status_code == 400
    assert r.json()["error_code"] == ErrorCode.VALIDATION_ERROR
    assert "consume" not in env.events


@pytest.mark.parametrize("body", [outbound_body(qty=0), outbound_body(ref_type="RECEIPT")])
def test_outbound_invalid_body_is_400_before_any_sql(env: Env, body: dict[str, Any]) -> None:
    r = env.client.post("/outbound", json=body)
    assert r.status_code == 400
    assert env.events == []


# ---------------------------------------------------------------------------
# GET /inventory
# ---------------------------------------------------------------------------
def test_get_inventory_returns_available(env: Env) -> None:
    env.fake(
        "list_inventory",
        [{"product_id": 1, "qty_on_hand": 100, "qty_reserved": 50, "version": 3}],
    )
    r = env.client.get("/inventory", params={"product_id": 1})
    assert r.status_code == 200
    assert r.json() == {
        "items": [
            {"product_id": 1, "qty_on_hand": 100, "qty_reserved": 50, "available": 50, "version": 3}
        ]
    }


def test_get_inventory_works_while_recovering(env: Env) -> None:
    # G5: đọc không bị chặn bởi RECOVERING
    env.client.app.state.mode = NodeMode.RECOVERING
    assert env.client.get("/inventory", params={"product_id": 1}).status_code == 200


def test_get_inventory_without_product_lists_all(env: Env) -> None:
    env.fake(
        "list_inventory",
        [
            {"product_id": 1, "qty_on_hand": 10, "qty_reserved": 0, "version": 1},
            {"product_id": 2, "qty_on_hand": 4, "qty_reserved": 1, "version": 2},
        ],
    )
    r = env.client.get("/inventory")
    assert r.status_code == 200
    assert [i["product_id"] for i in r.json()["items"]] == [1, 2]
    assert "product_exists" not in env.events


def test_get_inventory_known_product_without_row_is_empty(env: Env) -> None:
    r = env.client.get("/inventory", params={"product_id": 1})
    assert r.status_code == 200 and r.json() == {"items": []}


def test_get_inventory_unknown_product_is_404(env: Env) -> None:
    env.fake("product_exists", False)
    r = env.client.get("/inventory", params={"product_id": 99})
    assert r.status_code == 404
    assert r.json()["error_code"] == ErrorCode.NOT_FOUND


# ---------------------------------------------------------------------------
# Quy tắc sở hữu: SQL ghi kho chỉ nằm trong services/stock.py (docs/plan.md mục 1)
# ---------------------------------------------------------------------------
def test_router_contains_no_write_sql() -> None:
    src = (Path(main.__file__).parent / "routers" / "inventory.py").read_text(encoding="utf-8")
    assert not re.search(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from)\b", src, re.I)
