"""POST /transfers/notice ở kho (F6, F10.b) – test CI, không cần CSDL.

Kho nguồn ghi phiếu OUTBOUND và giữ chỗ; kho đích ghi phiếu INBOUND. Connection giả ở saga_fakes.py
ghi lại lệnh SQL theo thứ tự chạy. Test đầu-cuối gửi lại notice: test_tc08_notice_duplicate.py.
Spec: F5 (gửi trùng, dạng lỗi, X-Internal-Key, SKU không có trong danh mục), F6, F8,
F10 (câu dẫn), F10.b; db.md 2.7, 2.9–2.11.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterator
from datetime import date
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from fixtures import SKU_A_PRODUCT_ID
from saga_fakes import BEGIN, COMMIT, ROLLBACK, MakeNode, Node, node_fixture, norm, outbox, reserved
from services import stock

from common.constants import (
    DC_IDS,
    SQL_SET_APP_SOURCE,
    AggregateType,
    ErrorCode,
    EventType,
    TransferDirection,
    TransferStatus,
)
from common.errors import AppError
from common.ids import make_transfer_code
from common.models import TransferNoticeItem, TransferNoticeRequest

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS  # db.md 2.1
NOTICE_PATH = "/transfers/notice"  # F6
QTY = 50  # tests.md mục 2 – phiếu 1: Bắc → Nam, 50 thùng
UNKNOWN_PRODUCT_ID = 999  # chỉ dùng trong connection giả: SKU không có trong danh mục
REAL_RESERVE = getattr(stock, "reserve", None)  # hàm thật của Tuấn Minh, khi đã có
REAL_WRITE_OUTBOX = getattr(stock, "write_outbox", None)

INSERT_ORDER = "insert into transfer_order"
INSERT_ITEM = "insert into transfer_item"
SELECT_STATUS = "select status from transfer_order"


def notice_body(direction: TransferDirection, product_id: int = SKU_A_PRODUCT_ID) -> dict[str, Any]:
    """Transfer Notice của phiếu 1 (tests.md mục 2) gửi tới kho nguồn hoặc kho đích."""
    return TransferNoticeRequest(
        transfer_id=uuid4(),
        transfer_code=make_transfer_code(date.today().year, 1),
        from_dc_id=DC_BAC,
        to_dc_id=DC_NAM,
        direction=direction,
        items=[TransferNoticeItem(line_no=1, product_id=product_id, qty=QTY)],
    ).model_dump(mode="json")


def order_inserted(transfer_id: Any, *_: Any) -> Any:
    return transfer_id  # RETURNING transfer_id


def notice_db(
    *, duplicate: bool = False, existing: TransferStatus = TransferStatus.CREATED, item: Any = None
) -> dict[str, Any]:
    """Kết quả CSDL giả: duplicate = transfer_code đã có ở kho (ON CONFLICT không ghi dòng nào)."""
    return {
        INSERT_ORDER: None if duplicate else order_inserted,
        INSERT_ITEM: item,
        SELECT_STATUS: str(existing),
    }


@pytest.fixture
def make_node(monkeypatch: pytest.MonkeyPatch) -> Iterator[MakeNode]:
    yield from node_fixture(monkeypatch)


def post(node: Node, body: dict[str, Any], **kwargs: Any) -> Any:
    return node.post(NOTICE_PATH, body, **kwargs)


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


def test_notice_requires_internal_key(make_node: MakeNode) -> None:
    node = make_node(DC_BAC, results=notice_db())
    r = post(node, notice_body(TransferDirection.OUTBOUND), key=None)
    assert r.status_code == 401, r.text
    assert r.json()["error_code"] == ErrorCode.UNAUTHORIZED
    assert node.log == []


def test_notice_invalid_body_400(make_node: MakeNode) -> None:
    # Các ca dữ liệu sai khác của TransferNoticeRequest đã test ở tests/common
    node = make_node(DC_BAC, results=notice_db())
    r = post(node, {**notice_body(TransferDirection.OUTBOUND), "to_dc_id": DC_BAC})
    assert r.status_code == 400, r.text
    assert r.json()["error_code"] == ErrorCode.VALIDATION_ERROR
    assert node.log == []


def test_notice_outbound_reserves_in_one_transaction(make_node: MakeNode) -> None:
    node = make_node(DC_BAC, results=notice_db())
    r = post(node, notice_body(TransferDirection.OUTBOUND))
    assert r.status_code == 200, r.text
    assert r.json() == {"status": TransferStatus.CREATED, "duplicate": False}

    log = node.log
    # F10: một transaction, lệnh đầu là SET LOCAL app.source = 'api'
    assert log[:2] == [BEGIN, norm(SQL_SET_APP_SOURCE)], log
    assert log[-1] == COMMIT and ROLLBACK not in log, log
    # F10.b: transfer_order -> transfer_item -> giữ chỗ -> InventoryChanged -> TransferCreated
    order = node.first(INSERT_ORDER)
    item = node.first(INSERT_ITEM)
    reserve = node.index(reserved(SKU_A_PRODUCT_ID, QTY))
    inv_event = node.index(outbox(EventType.INVENTORY_CHANGED))
    created = node.index(outbox(EventType.TRANSFER_CREATED))
    assert order < item < reserve < inv_event < created < len(log) - 1, log
    # mỗi SKU một InventoryChanged, một TransferCreated cho phiếu (db.md 2.11)
    assert log.count(outbox(EventType.INVENTORY_CHANGED)) == 1, log
    assert log.count(outbox(EventType.TRANSFER_CREATED)) == 1, log


def test_notice_inbound_records_order_without_reserve(make_node: MakeNode) -> None:
    node = make_node(DC_NAM, results=notice_db())
    r = post(node, notice_body(TransferDirection.INBOUND))
    assert r.status_code == 200, r.text
    assert r.json() == {"status": TransferStatus.CREATED, "duplicate": False}

    log = node.log
    assert log[:2] == [BEGIN, norm(SQL_SET_APP_SOURCE)], log
    assert log[-1] == COMMIT, log
    node.first(INSERT_ORDER)
    node.first(INSERT_ITEM)
    # F10.b bước 3 chỉ áp dụng cho OUTBOUND
    assert not [e for e in log if e.startswith("stock.")], log
    assert outbox(EventType.INVENTORY_CHANGED) not in log, log
    assert log.count(outbox(EventType.TRANSFER_CREATED)) == 1, log


def test_notice_duplicate_returns_current_status(make_node: MakeNode) -> None:
    # Phiếu đã có ở kho và đã đi tiếp; F5: trả 200 duplicate kèm trạng thái hiện tại
    node = make_node(DC_BAC, results=notice_db(duplicate=True, existing=TransferStatus.SHIPPED))
    r = post(node, notice_body(TransferDirection.OUTBOUND))
    assert r.status_code == 200, r.text
    assert r.json() == {"status": TransferStatus.SHIPPED, "duplicate": True}

    log = node.log
    # F10.b bước 1: không ghi được transfer_order thì ROLLBACK, không giữ chỗ, không ghi sự kiện
    assert ROLLBACK in log and COMMIT not in log, log
    assert node.touched_stock() == [], log


def test_notice_insufficient_stock_rolls_back(make_node: MakeNode) -> None:
    error = AppError(ErrorCode.INSUFFICIENT_STOCK, "Không đủ hàng khả dụng")
    node = make_node(DC_BAC, results=notice_db(), reserve_error=error)
    r = post(node, notice_body(TransferDirection.OUTBOUND))
    assert r.status_code == 422, r.text
    assert r.json()["error_code"] == ErrorCode.INSUFFICIENT_STOCK

    log = node.log
    # F10.b bước 3: một dòng không đủ hàng thì ROLLBACK cả transaction (kể cả transfer_order)
    assert ROLLBACK in log and COMMIT not in log, log
    assert outbox(EventType.TRANSFER_CREATED) not in log, log


def test_notice_unknown_sku_400(make_node: MakeNode) -> None:
    """F5: lệnh ghi có SKU (trong body) không tồn tại trong danh mục -> 400 VALIDATION_ERROR."""

    def fk_violation(*_: Any) -> None:
        raise asyncpg.ForeignKeyViolationError(
            "insert or update on table transfer_item violates foreign key constraint"
        )

    node = make_node(DC_BAC, results=notice_db(item=fk_violation))
    r = post(node, notice_body(TransferDirection.OUTBOUND, UNKNOWN_PRODUCT_ID))
    assert r.status_code == 400, r.text
    body = r.json()
    assert body["error_code"] == ErrorCode.VALIDATION_ERROR
    assert set(body) == {"error_code", "message", "details"}

    # Lỗi xảy ra trong transaction: ROLLBACK, không giữ chỗ, không ghi sự kiện
    log = node.log
    assert ROLLBACK in log and COMMIT not in log, log
    assert node.touched_stock() == [], log
