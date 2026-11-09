"""/cancel và /sync-status CANCELLED ở kho – phần kho của TC07 (và TC06), docs/tests.md mục 4.

/cancel ở kho nguồn (F10.e): phiếu còn CREATED thì chuyển CANCELLED, nhả giữ chỗ (qty_reserved
Bắc về 0), ghi InventoryChanged và TransferCancelled; đã xuất thì 409 INVALID_STATE (TC06).
/sync-status CANCELLED ở kho đích (F10.g bước 2): bản INBOUND chuyển CANCELLED, không đụng tồn.

Test CI, không cần CSDL (connection giả ở saga_fakes.py). Đầu-cuối: tests/hq_api TC07.
Spec: F5, F6, F8, F10 (câu dẫn), F10.e, F10.g; db.md 2.7, 2.9–2.11, 3.2.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterator
from datetime import date
from uuid import UUID, uuid4

import pytest
from fixtures import SKU_A_PRODUCT_ID
from saga_fakes import (
    BEGIN,
    COMMIT,
    ROLLBACK,
    MakeNode,
    node_fixture,
    norm,
    outbox,
    released,
)
from services import stock

from common.constants import (
    DC_IDS,
    SQL_SET_APP_SOURCE,
    CancelReason,
    ErrorCode,
    EventType,
    TransferDirection,
    TransferStatus,
)
from common.errors import AppError
from common.ids import make_transfer_code
from common.models import CancelRequest, SyncStatusRequest

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS  # db.md 2.1
QTY = 10  # tests.md mục 2 – phiếu 2: Bắc → Trung, 10 thùng
CODE = make_transfer_code(date.today().year, 2)  # tests.md mục 2 – phiếu 2
OTHER_PRODUCT_ID = 7  # chỉ dùng trong connection giả, cho ca phiếu có 2 dòng hàng
REAL_RELEASE = getattr(stock, "release", None)  # hàm thật của Tuấn Minh, khi đã có

UPDATE = "update transfer_order"
STATUS = "select status from transfer_order"
ITEMS = "select product_id, qty_requested from transfer_item"
LINE = {"product_id": SKU_A_PRODUCT_ID, "qty_requested": QTY}

DECLINE = CancelRequest(reason=CancelReason.DECLINED).model_dump(mode="json")
SYNC_CANCELLED = SyncStatusRequest(status=TransferStatus.CANCELLED).model_dump(mode="json")


def cancel_path(transfer_id: UUID) -> str:
    return f"/transfers/{transfer_id}/cancel"  # F6


def sync_path(transfer_id: UUID) -> str:
    return f"/transfers/{transfer_id}/sync-status"  # F6


@pytest.fixture
def make_node(monkeypatch: pytest.MonkeyPatch) -> Iterator[MakeNode]:
    yield from node_fixture(monkeypatch)


@pytest.mark.skipif(REAL_RELEASE is None, reason="services/stock.py chưa có release (Tuấn Minh)")
def test_stock_release_signature_matches_router() -> None:
    """Khi stock.py đã có hàm thật: router gọi đúng chữ ký (đang là GIẢ ĐỊNH)."""
    inspect.signature(REAL_RELEASE).bind(None, DC_BAC, SKU_A_PRODUCT_ID, QTY)


def test_cancel_requires_internal_key(make_node: MakeNode) -> None:
    node = make_node(DC_BAC)
    r = node.post(cancel_path(uuid4()), DECLINE, key=None)
    assert r.status_code == 401, r.text
    assert r.json()["error_code"] == ErrorCode.UNAUTHORIZED
    assert node.log == []


def test_cancel_invalid_reason_400(make_node: MakeNode) -> None:
    node = make_node(DC_BAC)
    r = node.post(cancel_path(uuid4()), {"reason": "HET_CHO_CHUA"})
    assert r.status_code == 400, r.text
    assert r.json()["error_code"] == ErrorCode.VALIDATION_ERROR
    assert node.log == []


def test_cancel_created_releases_reservation(make_node: MakeNode) -> None:
    tid = uuid4()
    node = make_node(DC_BAC, results={UPDATE: CODE, ITEMS: [LINE]})
    r = node.post(cancel_path(tid), DECLINE)
    assert r.status_code == 200, r.text
    assert r.json() == {"status": TransferStatus.CANCELLED, "duplicate": False}

    log = node.log
    # F10: một transaction, lệnh đầu là SET LOCAL app.source = 'api'
    assert log[:2] == [BEGIN, norm(SQL_SET_APP_SOURCE)], log
    assert log[-1] == COMMIT and ROLLBACK not in log, log
    # F10.e bước 1: chỉ hủy bản OUTBOUND đang CREATED (db.md 3.2 quy tắc 2, 4), ghi lý do
    assert node.args(UPDATE) == (
        TransferStatus.CANCELLED,
        CancelReason.DECLINED,
        str(tid),
        TransferDirection.OUTBOUND,
        TransferStatus.CREATED,
    )
    # F10.e: hủy phiếu -> đọc dòng hàng -> nhả giữ chỗ -> InventoryChanged -> TransferCancelled
    order = node.first(UPDATE)
    items = node.first(ITEMS)
    release = node.index(released(SKU_A_PRODUCT_ID, QTY))
    inv_event = node.index(outbox(EventType.INVENTORY_CHANGED))
    cancelled = node.index(outbox(EventType.TRANSFER_CANCELLED))
    assert order < items < release < inv_event < cancelled < len(log) - 1, log

    # InventoryChanged mang đúng số liệu stock.release trả về (F8, db.md 2.11)
    assert node.events[0] == (
        EventType.INVENTORY_CHANGED,
        {"product_id": SKU_A_PRODUCT_ID, "qty_on_hand": 100, "qty_reserved": 0, "version": 3},
    )
    event_type, payload = node.events[-1]
    assert event_type == EventType.TRANSFER_CANCELLED
    assert payload["transfer_id"] == str(tid)
    assert payload["transfer_code"] == CODE
    assert payload["direction"] == TransferDirection.OUTBOUND
    assert payload["status"] == TransferStatus.CANCELLED
    assert payload["cancel_reason"] == CancelReason.DECLINED


def test_cancel_releases_every_line_by_product_id(make_node: MakeNode) -> None:
    lines = [
        {"product_id": OTHER_PRODUCT_ID, "qty_requested": 4},
        {"product_id": SKU_A_PRODUCT_ID, "qty_requested": 6},
    ]
    node = make_node(DC_BAC, results={UPDATE: CODE, ITEMS: lines})
    r = node.post(cancel_path(uuid4()), DECLINE)
    assert r.status_code == 200, r.text

    # Mỗi dòng nhả đúng qty_requested, theo product_id tăng dần (cùng thứ tự khóa với notice)
    assert [e for e in node.log if e.startswith("stock.")] == [
        released(SKU_A_PRODUCT_ID, 6),
        released(OTHER_PRODUCT_ID, 4),
    ]
    # F10.e: nhả hết các dòng (bước 2) rồi mới ghi sự kiện (bước 3)
    assert node.index(released(OTHER_PRODUCT_ID, 4)) < node.index(
        outbox(EventType.INVENTORY_CHANGED)
    ), node.log
    # Mỗi SKU một InventoryChanged, một TransferCancelled cho phiếu (db.md 2.11)
    inventory = [p["product_id"] for t, p in node.events if t == EventType.INVENTORY_CHANGED]
    assert inventory == [SKU_A_PRODUCT_ID, OTHER_PRODUCT_ID]
    assert node.log.count(outbox(EventType.TRANSFER_CANCELLED)) == 1, node.log


def test_cancel_retries_when_notice_commits_late(make_node: MakeNode) -> None:
    # UPDATE không thấy phiếu, nhưng notice commit ngay sau đó nên lệnh đọc thấy CREATED:
    # phải hủy lại và nhả giữ chỗ, không được trả CANCELLED khi phiếu còn CREATED
    updates = iter([None, CODE])
    results = {
        UPDATE: lambda *_: next(updates),
        STATUS: TransferStatus.CREATED.value,
        ITEMS: [LINE],
    }
    node = make_node(DC_BAC, results=results)
    r = node.post(cancel_path(uuid4()), DECLINE)
    assert r.status_code == 200, r.text
    assert r.json() == {"status": TransferStatus.CANCELLED, "duplicate": False}
    assert sum(e.startswith(UPDATE) for e in node.log) == 2, node.log
    assert node.touched_stock() == [
        released(SKU_A_PRODUCT_ID, QTY),
        outbox(EventType.INVENTORY_CHANGED),
        outbox(EventType.TRANSFER_CANCELLED),
    ], node.log


def test_cancel_already_cancelled_is_duplicate(make_node: MakeNode) -> None:
    # HQ gửi lại /cancel (F5): trả 200 duplicate, không nhả giữ chỗ lần nữa
    node = make_node(DC_BAC, results={UPDATE: None, STATUS: TransferStatus.CANCELLED.value})
    r = node.post(cancel_path(uuid4()), DECLINE)
    assert r.status_code == 200, r.text
    assert r.json() == {"status": TransferStatus.CANCELLED, "duplicate": True}
    assert node.touched_stock() == [], node.log


def test_cancel_unknown_transfer_returns_200(make_node: MakeNode) -> None:
    # F10.e bước 1: chưa có phiếu thì trả 200 và không làm gì (cờ duplicate: chờ trưởng nhóm chốt)
    node = make_node(DC_BAC, results={UPDATE: None, STATUS: None})
    r = node.post(cancel_path(uuid4()), DECLINE)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == TransferStatus.CANCELLED
    assert node.touched_stock() == [], node.log


@pytest.mark.parametrize("current", [TransferStatus.SHIPPED, TransferStatus.RECEIVED])
def test_cancel_after_ship_409(make_node: MakeNode, current: TransferStatus) -> None:
    # Phần kho của TC06: hàng đã rời kho thì kho nguồn từ chối hủy (db.md 3.2 quy tắc 4)
    node = make_node(DC_BAC, results={UPDATE: None, STATUS: current.value})
    r = node.post(cancel_path(uuid4()), DECLINE)
    assert r.status_code == 409, r.text
    assert r.json()["error_code"] == ErrorCode.INVALID_STATE
    assert node.touched_stock() == [], node.log


def test_cancel_release_error_rolls_back(make_node: MakeNode) -> None:
    # GIẢ ĐỊNH: lỗi của stock.release được trả nguyên (câu hỏi mở về 409 VERSION_CONFLICT)
    error = AppError(ErrorCode.VERSION_CONFLICT, "Hết 3 lần thử lại do xung đột version")
    node = make_node(DC_BAC, results={UPDATE: CODE, ITEMS: [LINE]}, release_error=error)
    r = node.post(cancel_path(uuid4()), DECLINE)
    assert r.status_code == 409, r.text
    assert r.json()["error_code"] == ErrorCode.VERSION_CONFLICT
    # Cả transaction ROLLBACK: phiếu vẫn CREATED, không có TransferCancelled
    assert ROLLBACK in node.log and COMMIT not in node.log, node.log
    assert outbox(EventType.TRANSFER_CANCELLED) not in node.log, node.log


def test_sync_cancelled_marks_inbound_without_touching_stock(make_node: MakeNode) -> None:
    tid = uuid4()
    row = {"transfer_code": CODE, "cancel_reason": None, "shipped_at": None, "received_at": None}
    node = make_node(DC_TRUNG, results={UPDATE: row})
    r = node.post(sync_path(tid), SYNC_CANCELLED)
    assert r.status_code == 200, r.text

    log = node.log
    assert log[:2] == [BEGIN, norm(SQL_SET_APP_SOURCE)], log
    assert log[-1] == COMMIT, log
    # F10.g bước 2: HQ đồng bộ CANCELLED xuống bản INBOUND đang CREATED ở kho đích
    assert node.args(UPDATE) == (
        TransferStatus.CANCELLED,
        str(tid),
        TransferDirection.INBOUND,
        TransferStatus.CREATED,
    )
    # Kho đích chưa giữ chỗ gì nên không nhả; chỉ một sự kiện phiếu (db.md 2.11)
    assert node.touched_stock() == [outbox(EventType.TRANSFER_CANCELLED)], log
    _, payload = node.events[-1]
    assert payload["direction"] == TransferDirection.INBOUND
    assert payload["status"] == TransferStatus.CANCELLED


def test_sync_cancelled_never_cancels_outbound(make_node: MakeNode) -> None:
    # Hủy bản OUTBOUND phải đi qua /cancel để nhả giữ chỗ; sync chỉ nhắm bản INBOUND
    node = make_node(DC_BAC, results={UPDATE: None})
    r = node.post(sync_path(uuid4()), SYNC_CANCELLED)
    assert r.status_code == 200, r.text
    assert node.args(UPDATE)[2] == TransferDirection.INBOUND
    assert node.touched_stock() == [], node.log
