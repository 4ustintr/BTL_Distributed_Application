"""POST /transfers/{id}/decline ở kho đích (F6) – test CI, không cần CSDL.

WMS kho đích gọi; kho chuyển tiếp đồng bộ tới HQ /transfers/{id}/decline và trả nguyên kết quả của
HQ (status và thân). Không ghi CSDL kho: việc hủy do HQ điều phối (F10.g). Không gọi được HQ thì
503 DC_UNREACHABLE. Chờ HQ tối đa bằng chuỗi thử lại F5 của HQ (18 giây), không tự gửi lại.
Spec: F5, F6, F10.g.
"""

from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import httpx
import pytest
from saga_fakes import HQ_CALL, FakeHq, MakeNode, node_fixture

from common.constants import (
    DC_IDS,
    HTTP_MAX_RETRIES,
    HTTP_RETRY_INTERVAL_S,
    HTTP_TIMEOUT_S,
    CancelReason,
    ErrorCode,
    TransferStatus,
)
from common.errors import error_body

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS  # db.md 2.1
DECLINE = {"reason": CancelReason.DECLINED}
# Chuỗi thử lại F5 của HQ khi gọi /cancel kho nguồn: 5 × 2 + 4 × 2 = 18 giây
HQ_WORST_CASE_S = (HTTP_MAX_RETRIES + 1) * HTTP_TIMEOUT_S + HTTP_MAX_RETRIES * HTTP_RETRY_INTERVAL_S


@pytest.fixture
def make_node(monkeypatch: pytest.MonkeyPatch) -> Iterator[MakeNode]:
    yield from node_fixture(monkeypatch)


def test_decline_forwards_to_hq_and_returns_its_result(make_node: MakeNode) -> None:
    tid = uuid4()
    reply = {"status": TransferStatus.CANCELLED, "duplicate": False}
    hq = FakeHq({"/decline": httpx.Response(200, json=reply)})
    node = make_node(DC_TRUNG, hq=hq)
    r = node.client.post(f"/transfers/{tid}/decline", json=DECLINE)  # WMS, không cần khóa
    assert (r.status_code, r.json()) == (200, reply)
    [(called, body, kwargs)] = hq.calls
    assert (called, body) == (f"/transfers/{tid}/decline", DECLINE)
    # Chờ được kết quả thật của HQ, không tự gửi lại (HQ đã thử lại /cancel theo F5)
    assert kwargs == {"request_timeout": HQ_WORST_CASE_S, "max_retries": 0}
    assert [e for e in node.log if not e.startswith(HQ_CALL)] == []  # kho không ghi CSDL


def test_decline_after_ship_returns_hq_409_as_is(make_node: MakeNode) -> None:
    # F10.g bước 3, TC06: kho nguồn đã xuất -> HQ trả 409 INVALID_STATE, kho trả nguyên
    body = error_body(ErrorCode.INVALID_STATE, "Phiếu đã xuất", {"status": "SHIPPED"})
    node = make_node(DC_TRUNG, hq=FakeHq({"/decline": httpx.Response(409, json=body)}))
    r = node.client.post(f"/transfers/{uuid4()}/decline", json=DECLINE)
    assert (r.status_code, r.json()) == (409, body)


def test_decline_hq_unreachable_is_503(make_node: MakeNode) -> None:
    node = make_node(DC_TRUNG, hq=FakeHq({"/decline": httpx.ReadTimeout("HQ không trả lời")}))
    r = node.client.post(f"/transfers/{uuid4()}/decline", json=DECLINE)
    assert r.status_code == 503, r.text
    assert r.json()["error_code"] == ErrorCode.DC_UNREACHABLE


def test_decline_invalid_reason_400(make_node: MakeNode) -> None:
    hq = FakeHq()
    node = make_node(DC_TRUNG, hq=hq)
    r = node.client.post(f"/transfers/{uuid4()}/decline", json={"reason": "BUON"})
    assert r.status_code == 400, r.text
    assert hq.calls == []
