"""POST /transfers/{id}/ship và /receive ở kho (F6, F10.c, F10.d) – test CI, không cần CSDL.

ship (kho nguồn): một transaction, SET LOCAL; UPDATE bản OUTBOUND CREATED -> SHIPPED; từng dòng
consume(TRANSFER_OUT, request_id tất định, ref_id = transfer_id), qty_shipped; InventoryChanged
từng SKU và TransferShipped; COMMIT; rồi mới gọi HQ /shipped. Đã SHIPPED/RECEIVED: 200 duplicate
và gửi lại callback; đã CANCELLED: 409; giữ chỗ thiếu: 422 và ROLLBACK; HQ không gọi được: 503.
receive (kho đích): như trên với bản INBOUND SHIPPED -> RECEIVED, add_stock(TRANSFER_IN),
qty_received, TransferReceived, HQ /received; chưa SHIPPED thì 409.
Spec: F5, F6, F8, F10 (câu dẫn), F10.c, F10.d; db.md 2.8, 2.9, 2.10, 2.11.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fixtures import SKU_A_PRODUCT_ID
from saga_fakes import (
    BEGIN,
    COMMIT,
    HQ_CALL,
    ROLLBACK,
    FakeHq,
    MakeNode,
    added,
    consumed,
    node_fixture,
    norm,
    outbox,
)

from common.constants import (
    DC_IDS,
    SQL_SET_APP_SOURCE,
    ErrorCode,
    EventType,
    MovementType,
    RefType,
    TransferDirection,
    TransferStatus,
)
from common.errors import AppError, error_body
from common.ids import transfer_movement_request_id
from common.models import ReceivedCallback, ShippedCallback

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS  # db.md 2.1
CODE = "TR-2026-000001"  # tests.md mục 2 – phiếu 1: Bắc → Nam, 50 thùng
OTHER_PRODUCT_ID = 7  # chỉ trong CSDL giả: phiếu 2 dòng
SHIPPED_AT = datetime(2026, 11, 20, 9, 5, 40, tzinfo=UTC)
RECEIVED_AT = datetime(2026, 11, 20, 9, 9, 10, tzinfo=UTC)
WHO = {"created_by": "nv-bac"}
# Thân callback lên HQ đúng model F7 (thời gian ISO 8601 theo cách pydantic tuần tự hóa)
SHIPPED_BODY = ShippedCallback(shipped_at=SHIPPED_AT).model_dump(mode="json")
RECEIVED_BODY = ReceivedCallback(received_at=RECEIVED_AT).model_dump(mode="json")

SHIP = "update transfer_order set status = $1, shipped_at = now()"
RECEIVE = "update transfer_order set status = $1, received_at = now()"
STATE = "select status, shipped_at, received_at from transfer_order"
LINES = "select line_no, product_id, qty_requested from transfer_item"
SET_SHIPPED = "update transfer_item set qty_shipped"
SET_RECEIVED = "update transfer_item set qty_received"

# Hai dòng, trả về KHÔNG theo product_id: router phải xử lý theo product_id tăng dần
TWO_LINES = [
    {"line_no": 2, "product_id": OTHER_PRODUCT_ID, "qty_requested": 5},
    {"line_no": 1, "product_id": SKU_A_PRODUCT_ID, "qty_requested": 50},
]


@pytest.fixture
def make_node(monkeypatch: pytest.MonkeyPatch) -> Iterator[MakeNode]:
    yield from node_fixture(monkeypatch)


def path(transfer_id: UUID, action: str) -> str:
    return f"/transfers/{transfer_id}/{action}"


def state(status: TransferStatus, shipped_at: Any = None, received_at: Any = None) -> dict:
    return {"status": str(status), "shipped_at": shipped_at, "received_at": received_at}


def ship_db(*, updated: bool, current: dict | None = None) -> dict[str, Any]:
    return {
        SHIP: {"transfer_code": CODE, "shipped_at": SHIPPED_AT} if updated else None,
        STATE: current,
        LINES: TWO_LINES,
        SET_SHIPPED: "UPDATE 1",
    }


def receive_db(*, updated: bool, current: dict | None = None) -> dict[str, Any]:
    row = {"transfer_code": CODE, "shipped_at": SHIPPED_AT, "received_at": RECEIVED_AT}
    return {
        RECEIVE: row if updated else None,
        STATE: current,
        LINES: TWO_LINES,
        SET_RECEIVED: "UPDATE 1",
    }


# ---------------------------------------------------------------------------
# ship (F10.c)
# ---------------------------------------------------------------------------
def test_ship_created_order_runs_f10c_in_one_transaction_then_calls_hq(
    make_node: MakeNode,
) -> None:
    tid = uuid4()
    hq = FakeHq()
    node = make_node(DC_BAC, results=ship_db(updated=True), hq=hq)
    r = node.client.post(path(tid, "ship"), json=WHO)  # WMS gọi, không cần X-Internal-Key
    assert r.status_code == 200, r.text
    assert r.json() == {"status": TransferStatus.SHIPPED, "duplicate": False}

    log = node.log
    assert log[:2] == [BEGIN, norm(SQL_SET_APP_SOURCE)], log
    # Bước 1: chỉ bản OUTBOUND còn CREATED
    assert node.args(SHIP) == (
        TransferStatus.SHIPPED, str(tid), TransferDirection.OUTBOUND, TransferStatus.CREATED
    )  # fmt: skip
    # Bước 2: trừ cả tồn và giữ chỗ theo product_id tăng dần, rồi ghi qty_shipped
    out = MovementType.TRANSFER_OUT
    first = node.index(consumed(out, SKU_A_PRODUCT_ID, 50))
    second = node.index(consumed(out, OTHER_PRODUCT_ID, 5))
    assert node.first(SHIP) < node.first(LINES) < first < second
    calls = node.conn.stock_calls
    assert [c["request_id"] for c in calls] == [
        transfer_movement_request_id(tid, out, 1),
        transfer_movement_request_id(tid, out, 2),
    ]
    assert {(c["movement_type"], c["ref_id"], c["created_by"]) for c in calls} == {
        (out, str(tid), "nv-bac")
    }
    assert log.count(next(e for e in log if e.startswith(SET_SHIPPED))) == 2
    # Bước 3: InventoryChanged từng SKU rồi TransferShipped, COMMIT, sau đó mới gọi HQ
    commit = log.index(COMMIT)
    shipped_event = node.index(outbox(EventType.TRANSFER_SHIPPED))
    assert log.count(outbox(EventType.INVENTORY_CHANGED)) == 2
    assert second < shipped_event < commit < node.first(HQ_CALL)
    assert ROLLBACK not in log
    inv = [p for e, p in node.events if e == EventType.INVENTORY_CHANGED]
    assert {str(p["request_id"]) for p in inv} == {str(c["request_id"]) for c in calls}
    assert {p["ref_type"] for p in inv} == {RefType.TRANSFER}
    _, shipped = node.events[-1]
    assert (shipped["status"], shipped["direction"]) == ("SHIPPED", "OUTBOUND")
    assert shipped["shipped_at"] == SHIPPED_AT.isoformat()
    # Callback /shipped (F6, F7) kèm shipped_at
    assert hq.calls == [(path(tid, "shipped"), SHIPPED_BODY, {})]


@pytest.mark.parametrize("current", [TransferStatus.SHIPPED, TransferStatus.RECEIVED])
def test_ship_again_is_duplicate_and_resends_callback(
    make_node: MakeNode, current: TransferStatus
) -> None:
    # F10.c bước 1: đã SHIPPED/RECEIVED thì 200 duplicate, không trừ kho lần nữa, gửi lại callback
    tid = uuid4()
    hq = FakeHq()
    node = make_node(
        DC_BAC, results=ship_db(updated=False, current=state(current, SHIPPED_AT)), hq=hq
    )
    r = node.client.post(path(tid, "ship"), json=WHO)
    assert r.status_code == 200, r.text
    assert r.json() == {"status": current, "duplicate": True}
    assert node.touched_stock() == []
    assert hq.calls == [(path(tid, "shipped"), SHIPPED_BODY, {})]


def test_ship_cancelled_order_is_409(make_node: MakeNode) -> None:
    hq = FakeHq()
    node = make_node(
        DC_BAC, results=ship_db(updated=False, current=state(TransferStatus.CANCELLED)), hq=hq
    )
    r = node.client.post(path(uuid4(), "ship"), json=WHO)
    assert r.status_code == 409, r.text
    assert r.json()["error_code"] == ErrorCode.INVALID_STATE
    assert node.touched_stock() == [] and hq.calls == []


def test_ship_unknown_order_is_404(make_node: MakeNode) -> None:
    hq = FakeHq()
    node = make_node(DC_BAC, results=ship_db(updated=False, current=None), hq=hq)
    r = node.client.post(path(uuid4(), "ship"), json=WHO)
    assert r.status_code == 404, r.text
    assert r.json()["error_code"] == ErrorCode.NOT_FOUND
    assert hq.calls == []


def test_ship_short_reservation_rolls_back_422(make_node: MakeNode) -> None:
    # F10.c bước 2 (spec mới): qty_reserved không đủ thì ROLLBACK cả transaction, 422
    hq = FakeHq()
    error = AppError(ErrorCode.INSUFFICIENT_STOCK, "Số lượng giữ chỗ không đủ")
    node = make_node(DC_BAC, results=ship_db(updated=True), hq=hq, consume_error=error)
    r = node.client.post(path(uuid4(), "ship"), json=WHO)
    assert r.status_code == 422, r.text
    assert r.json()["error_code"] == ErrorCode.INSUFFICIENT_STOCK
    assert ROLLBACK in node.log and COMMIT not in node.log
    assert outbox(EventType.TRANSFER_SHIPPED) not in node.log
    assert hq.calls == []


def test_ship_hq_unreachable_after_commit_is_503(make_node: MakeNode) -> None:
    # F6: hết lượt thử callback thì 503 DC_UNREACHABLE; dữ liệu kho đã COMMIT, bấm lại là duplicate
    hq = FakeHq({"/shipped": httpx.ConnectError("HQ chết")})
    node = make_node(DC_BAC, results=ship_db(updated=True), hq=hq)
    r = node.client.post(path(uuid4(), "ship"), json=WHO)
    assert r.status_code == 503, r.text
    assert r.json()["error_code"] == ErrorCode.DC_UNREACHABLE
    assert COMMIT in node.log and ROLLBACK not in node.log


def test_ship_hq_error_reply_is_returned_as_is(make_node: MakeNode) -> None:
    # F5: phản hồi 4xx/5xx của bên được gọi trả nguyên cho bên gọi
    body = error_body(ErrorCode.NOT_FOUND, "Không có phiếu")
    hq = FakeHq({"/shipped": httpx.Response(404, json=body)})
    node = make_node(DC_BAC, results=ship_db(updated=True), hq=hq)
    r = node.client.post(path(uuid4(), "ship"), json=WHO)
    assert (r.status_code, r.json()) == (404, body)


def test_ship_requires_created_by(make_node: MakeNode) -> None:
    node = make_node(DC_BAC, results=ship_db(updated=True), hq=FakeHq())
    r = node.client.post(path(uuid4(), "ship"), json={})
    assert r.status_code == 400, r.text
    assert node.log == []


# ---------------------------------------------------------------------------
# receive (F10.d)
# ---------------------------------------------------------------------------
def test_receive_shipped_order_runs_f10d_then_calls_hq(make_node: MakeNode) -> None:
    tid = uuid4()
    hq = FakeHq()
    node = make_node(DC_NAM, results=receive_db(updated=True), hq=hq)
    r = node.client.post(path(tid, "receive"), json={"created_by": "nv-nam"})
    assert r.status_code == 200, r.text
    assert r.json() == {"status": TransferStatus.RECEIVED, "duplicate": False}

    log = node.log
    assert log[:2] == [BEGIN, norm(SQL_SET_APP_SOURCE)], log
    # Bước 2: chỉ bản INBOUND đang SHIPPED
    assert node.args(RECEIVE) == (
        TransferStatus.RECEIVED, str(tid), TransferDirection.INBOUND, TransferStatus.SHIPPED
    )  # fmt: skip
    into = MovementType.TRANSFER_IN
    first = node.index(added(into, SKU_A_PRODUCT_ID, 50))
    second = node.index(added(into, OTHER_PRODUCT_ID, 5))
    calls = node.conn.stock_calls
    assert [c["request_id"] for c in calls] == [
        transfer_movement_request_id(tid, into, 1),
        transfer_movement_request_id(tid, into, 2),
    ]
    assert {(c["movement_type"], c["ref_id"]) for c in calls} == {(into, str(tid))}
    assert sum(e.startswith(SET_RECEIVED) for e in log) == 2
    received = node.index(outbox(EventType.TRANSFER_RECEIVED))
    assert first < second < received < log.index(COMMIT) < node.first(HQ_CALL)
    _, event = node.events[-1]
    assert (event["status"], event["direction"]) == ("RECEIVED", "INBOUND")
    assert event["received_at"] == RECEIVED_AT.isoformat()
    assert hq.calls == [(path(tid, "received"), RECEIVED_BODY, {})]


@pytest.mark.parametrize("current", [TransferStatus.CREATED, TransferStatus.CANCELLED])
def test_receive_before_shipped_is_409(make_node: MakeNode, current: TransferStatus) -> None:
    # F10.d bước 1: bản INBOUND chưa SHIPPED thì 409 INVALID_STATE
    hq = FakeHq()
    node = make_node(DC_NAM, results=receive_db(updated=False, current=state(current)), hq=hq)
    r = node.client.post(path(uuid4(), "receive"), json={"created_by": "nv-nam"})
    assert r.status_code == 409, r.text
    assert r.json()["error_code"] == ErrorCode.INVALID_STATE
    assert node.touched_stock() == [] and hq.calls == []


def test_receive_again_is_duplicate_and_resends_callback(make_node: MakeNode) -> None:
    tid = uuid4()
    hq = FakeHq()
    current = state(TransferStatus.RECEIVED, SHIPPED_AT, RECEIVED_AT)
    node = make_node(DC_NAM, results=receive_db(updated=False, current=current), hq=hq)
    r = node.client.post(path(tid, "receive"), json={"created_by": "nv-nam"})
    assert r.status_code == 200, r.text
    assert r.json() == {"status": TransferStatus.RECEIVED, "duplicate": True}
    assert node.touched_stock() == []
    assert hq.calls == [(path(tid, "received"), RECEIVED_BODY, {})]
