"""TC08 (tùy chọn) – Gửi lại Transfer Notice (docs/tests.md mục 4), và test CI của notice.

TC08: gọi POST /transfers/notice ở kho Bắc 2 lần cùng transfer_code (phiếu 1, tests.md mục 2).
Assert: lần 2 duplicate=true; qty_reserved không tăng gấp đôi.

Test CI (không cần CSDL): connection giả ghi lại lệnh SQL theo thứ tự chạy; reserve và
write_outbox của services/stock.py (Tuấn Minh, chưa có) được thay bằng hàm giả đúng chữ ký
giả định ở node-api/routers/transfers.py.

Spec: F5 (gửi trùng, dạng lỗi, X-Internal-Key), F6 (/transfers/notice), F8, F10 (câu dẫn),
F10.b; db.md 2.7, 2.9–2.11.
"""

from __future__ import annotations

import inspect
import json
import os
import subprocess
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

import asyncpg
import httpx
import main
import pytest
from fastapi.testclient import TestClient
from fixtures import REPO_ROOT, SKU_A_PRODUCT_ID, load_demo_ids
from services import stock

from common.constants import (
    DC_IDS,
    ENV_DC_ID,
    ENV_HOST_A_IP,
    ENV_HOST_B_IP,
    ENV_HQ_URL,
    ENV_INTERNAL_KEY,
    ENV_NODE_DB_URL,
    INTERNAL_KEY_HEADER,
    SQL_SET_APP_SOURCE,
    AggregateType,
    ErrorCode,
    EventType,
    TransferDirection,
    TransferStatus,
)
from common.errors import AppError
from common.ids import inventory_aggregate_id, make_transfer_code
from common.models import (
    InventoryEventPayload,
    TransferEventPayload,
    TransferNoticeItem,
    TransferNoticeRequest,
)

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS  # db.md 2.1
NOTICE_PATH = "/transfers/notice"  # F6
QTY = 50  # tests.md mục 2 – phiếu 1: Bắc → Nam, 50 thùng
STOCK_BAC = 100  # tests.md mục 2 – tồn ban đầu Bắc


def notice_body(direction: TransferDirection) -> dict[str, Any]:
    """Transfer Notice của phiếu 1 (tests.md mục 2) gửi tới kho nguồn hoặc kho đích."""
    return TransferNoticeRequest(
        transfer_id=uuid4(),  # GIẢ ĐỊNH: demo_ids.env chưa có transfer_id cố định cho phiếu
        transfer_code=make_transfer_code(date.today().year, 1),
        from_dc_id=DC_BAC,
        to_dc_id=DC_NAM,
        direction=direction,
        items=[TransferNoticeItem(line_no=1, product_id=SKU_A_PRODUCT_ID, qty=QTY)],
    ).model_dump(mode="json")


# ---------------------------------------------------------------------------
# TC08 – cần hệ thống thật (make reset)

NODE_BAC_PORT = 8001  # F2
HQ_DB_PORT = 5432  # F2
ENV_POSTGRES_PASSWORD = "POSTGRES_PASSWORD"  # .env.example – chưa có trong F2, common/constants.py


def make_reset() -> None:
    """docs/tests.md mục 4: test tự gọi make reset trước khi chạy."""
    subprocess.run(["make", "reset"], cwd=REPO_ROOT, check=True)


@pytest.mark.integration
@pytest.mark.optional
async def test_tc08_notice_duplicate() -> None:
    make_reset()
    env = {**load_demo_ids(REPO_ROOT / ".env"), **os.environ}  # .env cùng dạng KEY=VALUE
    node_bac = f"http://{env[ENV_HOST_B_IP]}:{NODE_BAC_PORT}"
    hq_db = (
        f"postgresql://hq_app:{env[ENV_POSTGRES_PASSWORD]}@{env[ENV_HOST_A_IP]}:{HQ_DB_PORT}/hq_db"
    )
    headers = {INTERNAL_KEY_HEADER: env[ENV_INTERNAL_KEY]}
    body = notice_body(TransferDirection.OUTBOUND)

    async with (
        httpx.AsyncClient(timeout=15) as http,
        asyncpg.create_pool(hq_db, min_size=1, max_size=1) as db,
    ):
        r = await http.post(f"{node_bac}{NOTICE_PATH}", json=body, headers=headers)
        assert r.status_code == 200, r.text
        assert r.json() == {"status": TransferStatus.CREATED, "duplicate": False}

        # HQ gửi lại đúng notice đó (F5: gửi trùng trả 200 duplicate và trạng thái hiện tại)
        r = await http.post(f"{node_bac}{NOTICE_PATH}", json=body, headers=headers)
        assert r.status_code == 200, r.text
        assert r.json() == {"status": TransferStatus.CREATED, "duplicate": True}

        # qty_reserved không tăng gấp đôi: Bắc 100/50 như KB1-1 (đọc kho qua FDW, F3)
        row = await db.fetchrow(
            f"SELECT qty_on_hand, qty_reserved FROM dc{DC_BAC}.inventory "
            "WHERE dc_id = $1 AND product_id = $2",
            DC_BAC,
            SKU_A_PRODUCT_ID,
        )
        assert row is not None, "kho Bắc chưa có dòng inventory cho SKU A (make seed)"
        assert (row["qty_on_hand"], row["qty_reserved"]) == (STOCK_BAC, QTY)
        orders = await db.fetchval(
            f"SELECT count(*) FROM dc{DC_BAC}.transfer_order WHERE transfer_code = $1",
            body["transfer_code"],
        )
        assert orders == 1


# ---------------------------------------------------------------------------
# Test CI – không cần CSDL

KEY = "khoa-test"
BEGIN, COMMIT, ROLLBACK = "begin", "commit", "rollback"
RESERVE = "stock.reserve"
REAL_RESERVE = getattr(stock, "reserve", None)  # hàm thật của Tuấn Minh, khi đã có
REAL_WRITE_OUTBOX = getattr(stock, "write_outbox", None)


def norm(sql: str) -> str:
    return " ".join(sql.split()).lower()


def outbox(event_type: EventType) -> str:
    return f"outbox:{event_type}"


class FakeTransaction:
    def __init__(self, log: list[str]) -> None:
        self.log = log

    async def __aenter__(self) -> None:
        self.log.append(BEGIN)

    async def __aexit__(self, exc_type: type[BaseException] | None, *_: object) -> None:
        self.log.append(COMMIT if exc_type is None else ROLLBACK)


class FakeConn:
    """asyncpg connection giả: ghi lại mọi lệnh SQL theo thứ tự chạy."""

    def __init__(self, log: list[str], *, duplicate: bool, existing: TransferStatus) -> None:
        self.log = log
        self.duplicate = duplicate  # transfer_code đã có ở kho: ON CONFLICT không ghi dòng nào
        self.existing = existing  # trạng thái hiện tại của phiếu đã có

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self.log)

    async def execute(self, sql: str, *_args: Any) -> None:
        self.log.append(norm(sql))

    async def executemany(self, sql: str, _args: Any) -> None:
        self.log.append(norm(sql))

    async def fetchval(self, sql: str, *args: Any) -> Any:
        s = norm(sql)
        self.log.append(s)
        if s.startswith("insert into transfer_order"):
            return None if self.duplicate else args[0]  # RETURNING transfer_id
        if s.startswith("select status from transfer_order"):
            return str(self.existing)
        raise AssertionError(f"lệnh SQL ngoài dự kiến: {s}")


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
    """Thay app.state.hq: notice không được gọi HTTP (F10, CLAUDE.md)."""

    async def aclose(self) -> None:
        return None

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"notice không được gọi HQ (app.state.hq.{name})")


@dataclass
class Node:
    client: TestClient
    log: list[str]

    def post(self, body: dict[str, Any], *, key: str | None = KEY) -> httpx.Response:
        headers = {INTERNAL_KEY_HEADER: key} if key else {}
        return self.client.post(NOTICE_PATH, json=body, headers=headers)

    def index(self, entry: str) -> int:
        assert entry in self.log, f"thiếu bước {entry!r}; log = {self.log}"
        return self.log.index(entry)

    def first(self, prefix: str) -> int:
        for i, entry in enumerate(self.log):
            if entry.startswith(prefix):
                return i
        raise AssertionError(f"thiếu bước {prefix!r}; log = {self.log}")


@pytest.fixture
def make_node(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[..., Node]]:
    stack = ExitStack()

    def factory(
        dc_id: int,
        *,
        duplicate: bool = False,
        existing: TransferStatus = TransferStatus.CREATED,
        insufficient: bool = False,
    ) -> Node:
        log: list[str] = []
        conn = FakeConn(log, duplicate=duplicate, existing=existing)
        monkeypatch.setenv(ENV_DC_ID, str(dc_id))
        monkeypatch.setenv(ENV_INTERNAL_KEY, KEY)
        monkeypatch.setenv(ENV_NODE_DB_URL, f"postgresql://x@127.0.0.1:1/dc{dc_id}")
        monkeypatch.setenv(ENV_HQ_URL, "http://127.0.0.1:1")

        async def fake_create_pool(*_a: Any, **_k: Any) -> FakePool:
            return FakePool(conn)

        # Hàm giả theo chữ ký GIẢ ĐỊNH (chờ Tuấn Minh chốt), kiểm luôn tham số router truyền
        async def reserve(c: FakeConn, dc: int, product_id: int, qty: int) -> dict[str, int]:
            assert (c, dc, product_id, qty) == (conn, dc_id, SKU_A_PRODUCT_ID, QTY)
            log.append(RESERVE)
            if insufficient:
                raise AppError(ErrorCode.INSUFFICIENT_STOCK, "Không đủ hàng khả dụng")
            return {"version": 2, "qty_on_hand": STOCK_BAC, "qty_reserved": QTY}

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
            log.append(outbox(event_type))

        monkeypatch.setattr(main.asyncpg, "create_pool", fake_create_pool)
        monkeypatch.setattr(stock, "reserve", reserve, raising=False)
        monkeypatch.setattr(stock, "write_outbox", write_outbox, raising=False)
        client = stack.enter_context(TestClient(main.create_app()))
        client.app.state.hq = NoHttp()
        return Node(client, log)

    with stack:
        yield factory


@pytest.mark.skipif(
    REAL_RESERVE is None or REAL_WRITE_OUTBOX is None,
    reason="services/stock.py chưa có reserve, write_outbox (Tuấn Minh)",
)
def test_stock_signature_matches_router() -> None:
    """Khi stock.py đã có hàm thật: router gọi đúng chữ ký (đang là GIẢ ĐỊNH)."""
    inspect.signature(REAL_RESERVE).bind(None, DC_BAC, SKU_A_PRODUCT_ID, QTY)
    inspect.signature(REAL_WRITE_OUTBOX).bind(
        None, DC_BAC, AggregateType.INVENTORY, "1:1", EventType.INVENTORY_CHANGED, {}
    )


def test_notice_requires_internal_key(make_node: Callable[..., Node]) -> None:
    node = make_node(DC_BAC)
    r = node.post(notice_body(TransferDirection.OUTBOUND), key=None)
    assert r.status_code == 401, r.text
    assert r.json()["error_code"] == ErrorCode.UNAUTHORIZED
    assert node.log == []


def test_notice_invalid_body_400(make_node: Callable[..., Node]) -> None:
    # Các ca dữ liệu sai khác của TransferNoticeRequest đã test ở tests/common
    node = make_node(DC_BAC)
    r = node.post({**notice_body(TransferDirection.OUTBOUND), "to_dc_id": DC_BAC})
    assert r.status_code == 400, r.text
    assert r.json()["error_code"] == ErrorCode.VALIDATION_ERROR
    assert node.log == []


def test_notice_outbound_reserves_in_one_transaction(make_node: Callable[..., Node]) -> None:
    node = make_node(DC_BAC)
    r = node.post(notice_body(TransferDirection.OUTBOUND))
    assert r.status_code == 200, r.text
    assert r.json() == {"status": TransferStatus.CREATED, "duplicate": False}

    log = node.log
    # F10: một transaction, lệnh đầu là SET LOCAL app.source = 'api'
    assert log[:2] == [BEGIN, norm(SQL_SET_APP_SOURCE)], log
    assert log[-1] == COMMIT and ROLLBACK not in log, log
    # F10.b: transfer_order -> transfer_item -> giữ chỗ -> InventoryChanged -> TransferCreated
    order = node.first("insert into transfer_order")
    item = node.first("insert into transfer_item")
    reserve = node.index(RESERVE)
    inv_event = node.index(outbox(EventType.INVENTORY_CHANGED))
    created = node.index(outbox(EventType.TRANSFER_CREATED))
    assert order < item < reserve < inv_event < created < len(log) - 1, log
    # mỗi SKU một InventoryChanged, một TransferCreated cho phiếu (db.md 2.11)
    assert log.count(outbox(EventType.INVENTORY_CHANGED)) == 1, log
    assert log.count(outbox(EventType.TRANSFER_CREATED)) == 1, log


def test_notice_inbound_records_order_without_reserve(make_node: Callable[..., Node]) -> None:
    node = make_node(DC_NAM)
    r = node.post(notice_body(TransferDirection.INBOUND))
    assert r.status_code == 200, r.text
    assert r.json() == {"status": TransferStatus.CREATED, "duplicate": False}

    log = node.log
    assert log[:2] == [BEGIN, norm(SQL_SET_APP_SOURCE)], log
    assert log[-1] == COMMIT, log
    node.first("insert into transfer_order")
    node.first("insert into transfer_item")
    # F10.b bước 3 chỉ áp dụng cho OUTBOUND
    assert RESERVE not in log, log
    assert outbox(EventType.INVENTORY_CHANGED) not in log, log
    assert log.count(outbox(EventType.TRANSFER_CREATED)) == 1, log


def test_notice_duplicate_returns_current_status(make_node: Callable[..., Node]) -> None:
    # Phiếu đã có ở kho và đã đi tiếp; F5: trả 200 duplicate kèm trạng thái hiện tại
    node = make_node(DC_BAC, duplicate=True, existing=TransferStatus.SHIPPED)
    r = node.post(notice_body(TransferDirection.OUTBOUND))
    assert r.status_code == 200, r.text
    assert r.json() == {"status": TransferStatus.SHIPPED, "duplicate": True}

    log = node.log
    # F10.b bước 1: không ghi được transfer_order thì ROLLBACK, không giữ chỗ, không ghi sự kiện
    assert ROLLBACK in log and COMMIT not in log, log
    assert RESERVE not in log, log
    assert not [e for e in log if e.startswith("outbox:")], log


def test_notice_insufficient_stock_rolls_back(make_node: Callable[..., Node]) -> None:
    node = make_node(DC_BAC, insufficient=True)
    r = node.post(notice_body(TransferDirection.OUTBOUND))
    assert r.status_code == 422, r.text
    assert r.json()["error_code"] == ErrorCode.INSUFFICIENT_STOCK

    log = node.log
    # F10.b bước 3: một dòng không đủ hàng thì ROLLBACK cả transaction (kể cả transfer_order)
    assert ROLLBACK in log and COMMIT not in log, log
    assert outbox(EventType.TRANSFER_CREATED) not in log, log
