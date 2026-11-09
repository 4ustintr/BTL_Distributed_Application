"""Connection asyncpg giả và hàm dựng Node API cho test Saga ở node-api (không cần CSDL).

Dùng cho test_transfer_notice.py, test_transfer_cancel.py, test_transfer_sync_status.py,
test_transfer_ship_receive.py, test_transfer_decline.py.

reserve, release, consume, add_stock, write_outbox của services/stock.py được thay bằng hàm giả
cùng chữ ký hàm thật; app.state.hq (client gọi HQ) được thay bằng FakeHq ghi lại lệnh gọi.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
import main
import pytest
from fastapi.testclient import TestClient
from services import stock

from common.constants import (
    ENV_DC_ID,
    ENV_HQ_URL,
    ENV_INTERNAL_KEY,
    ENV_NODE_DB_URL,
    INTERNAL_KEY_HEADER,
    SQL_SET_APP_SOURCE,
    AggregateType,
    EventType,
)
from common.errors import AppError
from common.ids import inventory_aggregate_id
from common.models import InventoryEventPayload, TransferEventPayload

KEY = "khoa-test"
BEGIN, COMMIT, ROLLBACK = "begin", "commit", "rollback"


def norm(sql: str) -> str:
    return " ".join(sql.split()).lower()


def outbox(event_type: EventType) -> str:
    return f"outbox:{event_type}"


def reserved(product_id: int, qty: int) -> str:
    return f"stock.reserve:{product_id}:{qty}"


def released(product_id: int, qty: int) -> str:
    return f"stock.release:{product_id}:{qty}"


def consumed(movement_type: str, product_id: int, qty: int) -> str:
    return f"stock.consume:{movement_type}:{product_id}:{qty}"


def added(movement_type: str, product_id: int, qty: int) -> str:
    return f"stock.add_stock:{movement_type}:{product_id}:{qty}"


HQ_CALL = "hq:"  # tiền tố trong log khi kho gọi HQ, ví dụ "hq:/transfers/<id>/shipped"


class FakeHq:
    """Thay app.state.hq. replies: hậu tố đường dẫn -> httpx.Response hoặc Exception để ném.

    Không có trong replies thì trả 200 {"status": "OK"}. Mỗi lệnh gọi ghi vào log chung (sau
    COMMIT mới được gọi) và vào calls (path, json, kwargs).
    """

    def __init__(self, replies: dict[str, Any] | None = None) -> None:
        self.replies = replies or {}
        self.calls: list[tuple[str, Any, dict[str, Any]]] = []
        self.log: list[str] | None = None

    async def post(self, path: str, *, json: Any = None, **kwargs: Any) -> httpx.Response:
        self.calls.append((path, json, kwargs))
        if self.log is not None:
            self.log.append(f"{HQ_CALL}{path}")
        for suffix, reply in self.replies.items():
            if path.endswith(suffix):
                if isinstance(reply, Exception):
                    raise reply
                return reply
        return httpx.Response(200, json={"status": "OK"})

    async def aclose(self) -> None:
        return None


class FakeTransaction:
    def __init__(self, log: list[str]) -> None:
        self.log = log

    async def __aenter__(self) -> None:
        self.log.append(BEGIN)

    async def __aexit__(self, exc_type: type[BaseException] | None, *_: object) -> None:
        self.log.append(COMMIT if exc_type is None else ROLLBACK)


class FakeConn:
    """Trả kết quả theo tiền tố câu SQL đã chuẩn hóa (giá trị, hoặc hàm nhận tham số SQL);
    ghi lại mọi lệnh và tham số theo thứ tự chạy."""

    def __init__(self, log: list[str], results: dict[str, Any]) -> None:
        self.log = log
        self.results = {norm(SQL_SET_APP_SOURCE): None, **results}
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.stock_calls: list[dict[str, Any]] = []  # tham số các lệnh consume, add_stock

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self.log)

    def _run(self, sql: str, args: tuple[Any, ...]) -> Any:
        s = norm(sql)
        self.log.append(s)
        self.calls.append((s, args))
        for prefix, value in self.results.items():
            if s.startswith(prefix):
                return value(*args) if callable(value) else value
        raise AssertionError(f"lệnh SQL ngoài dự kiến: {s}")

    async def execute(self, sql: str, *args: Any) -> Any:
        return self._run(sql, args)

    async def executemany(self, sql: str, rows: Any) -> None:
        self._run(sql, tuple(rows))

    async def fetch(self, sql: str, *args: Any) -> Any:
        return self._run(sql, args)

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        return self._run(sql, args)

    async def fetchval(self, sql: str, *args: Any) -> Any:
        return self._run(sql, args)


class FakeAcquire:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeConn:
        return self.conn

    async def __aexit__(self, *_: object) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.conn)

    async def close(self) -> None:
        return None


class NoHttp:
    """Thay app.state.hq: kho không được gọi HQ trong các lệnh này (F10, CLAUDE.md)."""

    async def aclose(self) -> None:
        return None

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"không được gọi HQ (app.state.hq.{name})")


@dataclass
class Node:
    client: TestClient
    conn: FakeConn
    events: list[tuple[EventType, dict[str, Any]]] = field(default_factory=list)

    @property
    def log(self) -> list[str]:
        return self.conn.log

    def post(self, path: str, body: dict[str, Any], *, key: str | None = KEY) -> httpx.Response:
        headers = {INTERNAL_KEY_HEADER: key} if key else {}
        return self.client.post(path, json=body, headers=headers)

    def index(self, entry: str) -> int:
        assert entry in self.log, f"thiếu bước {entry!r}; log = {self.log}"
        return self.log.index(entry)

    def first(self, prefix: str) -> int:
        for i, entry in enumerate(self.log):
            if entry.startswith(prefix):
                return i
        raise AssertionError(f"thiếu bước {prefix!r}; log = {self.log}")

    def args(self, prefix: str) -> tuple[str, ...]:
        """Tham số (dạng chuỗi) của lệnh SQL đầu tiên bắt đầu bằng prefix."""
        for sql, args in self.conn.calls:
            if sql.startswith(prefix):
                return tuple(str(a) for a in args)
        raise AssertionError(f"thiếu lệnh {prefix!r}; log = {self.log}")

    def touched_stock(self) -> list[str]:
        return [e for e in self.log if e.startswith(("stock.", "outbox:"))]


def build_node(
    monkeypatch: pytest.MonkeyPatch,
    stack: ExitStack,
    dc_id: int,
    *,
    results: dict[str, Any] | None = None,
    reserve_error: AppError | None = None,
    release_error: AppError | None = None,
    consume_error: AppError | None = None,
    hq: FakeHq | None = None,
) -> Node:
    """Node API kho dc_id với CSDL giả trả `results` theo tiền tố SQL (đã chuẩn hóa)."""
    conn = FakeConn([], results or {})
    dc_id_expected = dc_id  # các hàm giả có tham số keyword trùng tên dc_id
    events: list[tuple[EventType, dict[str, Any]]] = []
    monkeypatch.setenv(ENV_DC_ID, str(dc_id))
    monkeypatch.setenv(ENV_INTERNAL_KEY, KEY)
    monkeypatch.setenv(ENV_NODE_DB_URL, f"postgresql://x@127.0.0.1:1/dc{dc_id}")
    monkeypatch.setenv(ENV_HQ_URL, "http://127.0.0.1:1")

    async def fake_create_pool(*_a: Any, **_k: Any) -> FakePool:
        return FakePool(conn)

    async def reserve(c: FakeConn, dc: int, product_id: int, qty: int) -> dict[str, int]:
        assert (c, dc) == (conn, dc_id)
        conn.log.append(reserved(product_id, qty))
        if reserve_error is not None:
            raise reserve_error
        return {"version": 2, "qty_on_hand": 100, "qty_reserved": qty}

    async def release(c: FakeConn, dc: int, product_id: int, qty: int) -> dict[str, int]:
        assert (c, dc) == (conn, dc_id)
        conn.log.append(released(product_id, qty))
        if release_error is not None:
            raise release_error
        return {"version": 3, "qty_on_hand": 100, "qty_reserved": 0}

    async def consume(
        c: FakeConn,
        *,
        dc_id: int,
        product_id: int,
        qty: int,
        request_id: Any,
        created_by: str,
        ref_type: Any = None,
        lock_mode: Any = None,
        movement_type: Any = None,
        ref_id: str | None = None,
    ) -> dict[str, Any]:
        assert (c, dc_id) == (conn, dc_id_expected)
        conn.log.append(consumed(str(movement_type), product_id, qty))
        conn.stock_calls.append(
            {"fn": "consume", "product_id": product_id, "qty": qty, "request_id": request_id,
             "created_by": created_by, "ref_type": ref_type, "movement_type": movement_type,
             "ref_id": ref_id}
        )  # fmt: skip
        if consume_error is not None:
            raise consume_error
        return {"version": 4, "qty_on_hand": 20, "qty_reserved": 0}

    async def add_stock(
        c: FakeConn,
        *,
        dc_id: int,
        product_id: int,
        qty: int,
        request_id: Any,
        created_by: str,
        ref_type: Any = None,
        movement_type: Any = None,
        ref_id: str | None = None,
    ) -> dict[str, Any]:
        assert (c, dc_id) == (conn, dc_id_expected)
        conn.log.append(added(str(movement_type), product_id, qty))
        conn.stock_calls.append(
            {"fn": "add_stock", "product_id": product_id, "qty": qty, "request_id": request_id,
             "created_by": created_by, "ref_type": ref_type, "movement_type": movement_type,
             "ref_id": ref_id}
        )  # fmt: skip
        return {"version": 2, "qty_on_hand": 60, "qty_reserved": 0}

    async def write_outbox(
        c: FakeConn,
        dc: int,
        aggregate_type: AggregateType,
        aggregate_id: str,
        event_type: EventType,
        payload: dict[str, Any],
    ) -> None:
        assert (c, dc) == (conn, dc_id)
        # write_outbox thật tự điền 4 trường này; payload đầy đủ phải đúng mẫu F8
        full = {
            **payload,
            "event_id": str(uuid4()),
            "event_type": event_type,
            "source_dc": dc,
            "occurred_at": datetime.now(UTC).isoformat(),
        }
        json.dumps(full)  # cột payload là JSONB (db.md 2.11)
        if aggregate_type == AggregateType.INVENTORY:
            InventoryEventPayload.model_validate(full)
            assert aggregate_id == inventory_aggregate_id(dc, payload["product_id"])
        else:
            TransferEventPayload.model_validate(full)
            assert aggregate_id == payload["transfer_id"]
        conn.log.append(outbox(event_type))
        events.append((event_type, payload))

    monkeypatch.setattr(main.asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(stock, "reserve", reserve, raising=False)
    monkeypatch.setattr(stock, "release", release, raising=False)
    monkeypatch.setattr(stock, "consume", consume, raising=False)
    monkeypatch.setattr(stock, "add_stock", add_stock, raising=False)
    monkeypatch.setattr(stock, "write_outbox", write_outbox, raising=False)
    client = stack.enter_context(TestClient(main.create_app()))
    if hq is None:
        client.app.state.hq = NoHttp()
    else:
        hq.log = conn.log
        client.app.state.hq = hq
    return Node(client, conn, events)


def node_fixture(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Thân fixture make_node cho từng file test: `yield from node_fixture(monkeypatch)`."""
    with ExitStack() as stack:

        def factory(dc_id: int, **kwargs: Any) -> Node:
            return build_node(monkeypatch, stack, dc_id, **kwargs)

        yield factory


MakeNode = Callable[..., Node]
