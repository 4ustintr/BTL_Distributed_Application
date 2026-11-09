"""/sync-status ở kho – phần kho của TC05 (docs/tests.md mục 4).

HQ đồng bộ SHIPPED xuống bản INBOUND ở kho đích (F10.c, F10.d bước 1) và RECEIVED xuống bản
OUTBOUND ở kho nguồn (hld.md Kịch bản 1; TC05: RECEIVED ở cả 2 transfer_order). Chỉ đi từ trạng
thái liền trước (db.md 3.2 quy tắc 2). Cặp (direction, status) khác bị bỏ qua và vẫn trả 200 (F6),
vì đổi trạng thái mà không chạy bước tồn kho đi kèm sẽ làm lệch tồn.
CANCELLED: test_transfer_cancel.py.
F6: /sync-status trả 200, không có thân phản hồi.

Test CI, không cần CSDL (connection giả ở saga_fakes.py).
Spec: F6, F8, F10 (câu dẫn); db.md 2.9, 2.11, 3.2.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from uuid import UUID, uuid4

import pytest
from saga_fakes import BEGIN, COMMIT, MakeNode, node_fixture, norm, outbox

from common.constants import (
    DC_IDS,
    SQL_SET_APP_SOURCE,
    ErrorCode,
    EventType,
    TransferDirection,
    TransferStatus,
)
from common.ids import make_transfer_code
from common.models import SyncStatusRequest

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS  # db.md 2.1
CODE = make_transfer_code(date.today().year, 1)  # tests.md mục 2 – phiếu 1: Bắc → Nam
SHIPPED_AT = datetime(2026, 11, 20, 9, 5, 40, tzinfo=UTC)  # giá trị mẫu cho connection giả
UPDATE = "update transfer_order"

# (kho nhận lệnh, trạng thái mới, bản phiếu ở kho đó, trạng thái liền trước, sự kiện, shipped_at)
FORWARD = {
    "shipped_xuong_kho_dich": (
        DC_NAM,
        TransferStatus.SHIPPED,
        TransferDirection.INBOUND,
        TransferStatus.CREATED,
        EventType.TRANSFER_SHIPPED,
        None,
    ),
    "received_xuong_kho_nguon": (
        DC_BAC,
        TransferStatus.RECEIVED,
        TransferDirection.OUTBOUND,
        TransferStatus.SHIPPED,
        EventType.TRANSFER_RECEIVED,
        SHIPPED_AT,  # kho nguồn đã ghi shipped_at khi xuất (F10.c)
    ),
}


def sync_path(transfer_id: UUID) -> str:
    return f"/transfers/{transfer_id}/sync-status"  # F6


def sync_body(status: TransferStatus) -> dict[str, str]:
    return SyncStatusRequest(status=status).model_dump(mode="json")


@pytest.fixture
def make_node(monkeypatch: pytest.MonkeyPatch) -> Iterator[MakeNode]:
    yield from node_fixture(monkeypatch)


def test_sync_status_requires_internal_key(make_node: MakeNode) -> None:
    node = make_node(DC_NAM)
    r = node.post(sync_path(uuid4()), sync_body(TransferStatus.SHIPPED), key=None)
    assert r.status_code == 401, r.text
    assert r.json()["error_code"] == ErrorCode.UNAUTHORIZED
    assert node.log == []


def test_sync_status_invalid_status_400(make_node: MakeNode) -> None:
    node = make_node(DC_NAM)
    r = node.post(sync_path(uuid4()), {"status": "LOST"})
    assert r.status_code == 400, r.text
    assert r.json()["error_code"] == ErrorCode.VALIDATION_ERROR
    assert node.log == []


@pytest.mark.parametrize(
    ("dc_id", "status", "direction", "prev", "event", "shipped_at"),
    FORWARD.values(),
    ids=FORWARD.keys(),
)
def test_sync_status_moves_forward(
    make_node: MakeNode,
    dc_id: int,
    status: TransferStatus,
    direction: TransferDirection,
    prev: TransferStatus,
    event: EventType,
    shipped_at: datetime | None,
) -> None:
    tid = uuid4()
    row = {
        "transfer_code": CODE,
        "cancel_reason": None,
        "shipped_at": shipped_at,
        "received_at": None,
    }
    node = make_node(dc_id, results={UPDATE: row})
    r = node.post(sync_path(tid), sync_body(status))
    assert r.status_code == 200, r.text
    assert r.content == b"", r.text  # F6: không có thân phản hồi

    log = node.log
    # F10: một transaction, lệnh đầu là SET LOCAL app.source = 'api'
    assert log[:2] == [BEGIN, norm(SQL_SET_APP_SOURCE)], log
    assert log[-1] == COMMIT, log
    # db.md 3.2 quy tắc 2: chỉ từ trạng thái liền trước, đúng bản phiếu ở kho này
    assert node.args(UPDATE) == (status, str(tid), direction, prev)
    # Không đụng tồn kho; đúng một sự kiện phiếu (db.md 2.11)
    assert node.touched_stock() == [outbox(event)], log
    _, payload = node.events[-1]
    assert payload["transfer_id"] == str(tid)
    assert payload["transfer_code"] == CODE
    assert payload["direction"] == direction
    assert payload["status"] == status
    # Thời điểm lấy từ bản phiếu ở kho, dạng chuỗi ISO 8601 (F5, F8)
    assert payload["shipped_at"] == (shipped_at.isoformat() if shipped_at else None)
    assert payload["received_at"] is None


@pytest.mark.parametrize("status", [TransferStatus.SHIPPED, TransferStatus.RECEIVED])
def test_sync_status_ignored_when_not_previous_state(
    make_node: MakeNode, status: TransferStatus
) -> None:
    # Lệnh đến muộn hoặc sai thứ tự: UPDATE có điều kiện không đổi dòng nào -> bỏ qua, vẫn 200
    node = make_node(DC_NAM, results={UPDATE: None})
    r = node.post(sync_path(uuid4()), sync_body(status))
    assert r.status_code == 200, r.text
    assert r.content == b"", r.text
    assert node.touched_stock() == [], node.log


def test_sync_status_created_does_nothing(make_node: MakeNode) -> None:
    # CREATED không có trạng thái liền trước nên không có gì để đồng bộ
    node = make_node(DC_NAM)
    r = node.post(sync_path(uuid4()), sync_body(TransferStatus.CREATED))
    assert r.status_code == 200, r.text
    assert r.content == b"", r.text
    assert node.log == []
