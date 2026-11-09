"""Router Saga ở HQ (F7, F10.f, F10.g, F9) – test CI, không cần CSDL.

Lưu trữ giả (FakeStore) mô phỏng transfer_summary, transfer_event: đổi trạng thái chỉ khi đang ở
trạng thái liền trước, ghi lịch sử chỉ khi đổi được (db.md 2.13, 3.2). Kho giả nằm sau
InternalClient thật (transport giả, không chờ giữa các lần thử) nên kiểm cả thử lại F5.
Câu SQL thật được kiểm ở TC05, TC07 (tích hợp).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fixtures import SKU_A_PRODUCT_ID
from routers import transfers

from common.constants import (
    DC_IDS,
    HTTP_MAX_RETRIES,
    CancelReason,
    DcStatus,
    ErrorCode,
    TransferDirection,
    TransferEventSource,
    TransferStatus,
)
from common.errors import error_body, install_error_handlers
from common.http import InternalClient
from common.ids import make_transfer_code

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS  # db.md 2.1
KEY = "khoa-test"
SHIPPED_AT = datetime(2026, 11, 20, 9, 5, 40, tzinfo=UTC)
RECEIVED_AT = datetime(2026, 11, 20, 9, 9, 10, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Lưu trữ giả
# ---------------------------------------------------------------------------
@dataclass
class FakeStore:
    dc_status: dict[int, DcStatus] = field(
        default_factory=lambda: dict.fromkeys(DC_IDS, DcStatus.ONLINE)
    )
    products: set[int] = field(default_factory=lambda: {SKU_A_PRODUCT_ID})
    rows: dict[UUID, dict[str, Any]] = field(default_factory=dict)
    events: list[tuple[UUID, str | None, str, str]] = field(default_factory=list)
    seq: int = 0

    async def dc_statuses(self, dc_ids: list[int]) -> dict[int, str]:
        return {dc: str(self.dc_status[dc]) for dc in dc_ids if dc in self.dc_status}

    async def missing_products(self, product_ids: list[int]) -> list[int]:
        return [p for p in product_ids if p not in self.products]

    async def create(
        self, transfer_id: UUID, from_dc_id: int, to_dc_id: int, items: list[dict[str, int]]
    ) -> str:
        self.seq += 1
        code = make_transfer_code(datetime.now(UTC).year, self.seq)
        now = datetime.now(UTC)
        self.rows[transfer_id] = {
            "transfer_id": transfer_id,
            "transfer_code": code,
            "from_dc_id": from_dc_id,
            "to_dc_id": to_dc_id,
            "status": "CREATED",
            "total_lines": len(items),
            "total_qty": sum(i["qty"] for i in items),
            "items": items,
            "cancel_reason": None,
            "cancelled_at": None,
            "last_sync_at": None,
            "created_at": now,
            "shipped_at": None,
            "received_at": None,
            "updated_at": now,
        }
        self.events.append((transfer_id, None, "CREATED", "HQ"))
        return code

    async def get(self, transfer_id: UUID) -> dict[str, Any] | None:
        row = self.rows.get(transfer_id)
        return None if row is None else dict(row)

    async def list(self, status: TransferStatus | None) -> list[dict[str, Any]]:
        return [dict(r) for r in self.rows.values() if status is None or r["status"] == status]

    async def transition(
        self,
        transfer_id: UUID,
        from_status: TransferStatus,
        to_status: TransferStatus,
        source: TransferEventSource,
        *,
        shipped_at: datetime | None = None,
        received_at: datetime | None = None,
        cancel_reason: CancelReason | None = None,
    ) -> bool:
        row = self.rows.get(transfer_id)
        if row is None or row["status"] != from_status:
            return False
        row["status"] = str(to_status)
        row["shipped_at"] = row["shipped_at"] or shipped_at
        row["received_at"] = row["received_at"] or received_at
        if to_status == TransferStatus.CANCELLED:
            row["cancel_reason"] = str(cancel_reason)
            row["cancelled_at"] = datetime.now(UTC)
        self.events.append((transfer_id, str(from_status), str(to_status), str(source)))
        return True

    async def mark_synced(self, transfer_id: UUID) -> None:
        self.rows[transfer_id]["last_sync_at"] = datetime.now(UTC)


# ---------------------------------------------------------------------------
# Kho giả sau InternalClient thật
# ---------------------------------------------------------------------------
Reply = httpx.Response | Exception | Callable[[dict[str, Any]], httpx.Response]


@dataclass
class FakeKho:
    dc_id: int
    replies: dict[str, Reply] = field(default_factory=dict)
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def handle(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else {}
        self.calls.append((request.url.path, body))
        for suffix, reply in self.replies.items():
            if request.url.path.endswith(suffix):
                if isinstance(reply, Exception):
                    raise reply
                if callable(reply):
                    return reply(body)
                return reply
        return httpx.Response(200, json={"status": "CREATED", "duplicate": False})

    def client(self) -> InternalClient:
        return InternalClient(
            f"http://kho{self.dc_id}",
            KEY,
            retry_interval=0,
            transport=httpx.MockTransport(self.handle),
        )

    def paths(self) -> list[str]:
        return [p for p, _ in self.calls]


@dataclass
class Hq:
    client: TestClient
    store: FakeStore
    kho: dict[int, FakeKho]


@pytest.fixture
def make_hq() -> Iterator[Callable[..., Hq]]:
    clients: list[TestClient] = []

    def factory(store: FakeStore | None = None, **replies: dict[str, Reply]) -> Hq:
        store = store or FakeStore()
        kho = {dc: FakeKho(dc, replies.get(f"dc{dc}", {})) for dc in DC_IDS}
        app = FastAPI()
        install_error_handlers(app)
        app.include_router(transfers.router)
        app.state.nodes = {dc: k.client() for dc, k in kho.items()}
        app.dependency_overrides[transfers.get_store] = lambda: store
        client = TestClient(app)
        clients.append(client)
        return Hq(client, store, kho)

    yield factory
    for c in clients:
        c.close()


def create_body(qty: int = 50, from_dc: int = DC_BAC, to_dc: int = DC_NAM) -> dict[str, Any]:
    return {"from_dc_id": from_dc, "to_dc_id": to_dc, "items": [{"product_id": 1, "qty": qty}]}


def network_error() -> httpx.ConnectError:
    return httpx.ConnectError("kho không trả lời")


def created(hq: Hq, **kw: Any) -> UUID:
    r = hq.client.post("/transfers", json=create_body(**kw))
    assert r.status_code == 201, r.text
    return UUID(r.json()["transfer_id"])


# ---------------------------------------------------------------------------
# POST /transfers (F10.f)
# ---------------------------------------------------------------------------
def test_create_writes_created_then_notices_source_before_destination(make_hq) -> None:
    hq = make_hq()
    r = hq.client.post("/transfers", json=create_body())
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == TransferStatus.CREATED
    assert body["transfer_code"] == make_transfer_code(datetime.now(UTC).year, 1)
    tid = UUID(body["transfer_id"])
    row = hq.store.rows[tid]
    assert (row["status"], row["from_dc_id"], row["to_dc_id"]) == ("CREATED", DC_BAC, DC_NAM)
    assert (row["items"], row["total_lines"], row["total_qty"]) == (
        [{"product_id": 1, "qty": 50}], 1, 50
    )  # fmt: skip
    assert hq.store.events == [(tid, None, "CREATED", "HQ")]
    # Notice tới kho nguồn trước (OUTBOUND), kho đích sau (INBOUND), cùng transfer_id và mã phiếu
    [(src_path, src)] = hq.kho[DC_BAC].calls
    [(dst_path, dst)] = hq.kho[DC_NAM].calls
    assert src_path == dst_path == "/transfers/notice"
    assert (src["direction"], dst["direction"]) == (
        TransferDirection.OUTBOUND, TransferDirection.INBOUND
    )  # fmt: skip
    for notice in (src, dst):
        assert notice["transfer_id"] == str(tid)
        assert notice["transfer_code"] == body["transfer_code"]
        assert (notice["from_dc_id"], notice["to_dc_id"]) == (DC_BAC, DC_NAM)
        assert notice["items"] == [{"line_no": 1, "product_id": 1, "qty": 50}]
    assert hq.kho[DC_TRUNG].calls == []


@pytest.mark.parametrize("status", [DcStatus.OFFLINE, DcStatus.RECOVERING])
def test_create_needs_both_dc_online(make_hq, status: DcStatus) -> None:
    store = FakeStore()
    store.dc_status[DC_NAM] = status
    hq = make_hq(store)
    r = hq.client.post("/transfers", json=create_body())
    assert r.status_code == 503, r.text
    assert r.json()["error_code"] == ErrorCode.DC_UNREACHABLE
    assert hq.store.rows == {} and hq.kho[DC_BAC].calls == []  # không tạo phiếu


@pytest.mark.parametrize(
    "items",
    [
        [{"product_id": 999, "qty": 5}],  # SKU không có trong danh mục (F5)
        [{"product_id": 1, "qty": 5}, {"product_id": 1, "qty": 6}],  # trùng SKU (db.md 2.10)
    ],
)
def test_create_rejects_bad_items_400(make_hq, items: list[dict[str, int]]) -> None:
    hq = make_hq()
    r = hq.client.post("/transfers", json={"from_dc_id": 1, "to_dc_id": 3, "items": items})
    assert r.status_code == 400, r.text
    assert r.json()["error_code"] == ErrorCode.VALIDATION_ERROR
    assert hq.store.rows == {}


def test_create_source_short_of_stock_cancels_422(make_hq) -> None:
    short = error_body(ErrorCode.INSUFFICIENT_STOCK, "Không đủ hàng", {"available": 20})
    hq = make_hq(dc1={"/transfers/notice": httpx.Response(422, json=short)})
    r = hq.client.post("/transfers", json=create_body(qty=500))
    assert (r.status_code, r.json()) == (422, short)
    [tid] = hq.store.rows
    row = hq.store.rows[tid]
    assert (row["status"], row["cancel_reason"]) == ("CANCELLED", "INSUFFICIENT_STOCK")
    assert hq.store.events[-1] == (tid, "CREATED", "CANCELLED", "HQ")
    assert hq.kho[DC_NAM].calls == []  # không gửi notice kho đích
    assert hq.kho[DC_BAC].paths() == ["/transfers/notice"]  # không cần /cancel


def test_create_source_unreachable_cancels_timeout_503(make_hq) -> None:
    hq = make_hq(dc1={"/transfers/notice": network_error()})
    r = hq.client.post("/transfers", json=create_body())
    assert r.status_code == 503, r.text
    assert r.json()["error_code"] == ErrorCode.DC_UNREACHABLE
    [tid] = hq.store.rows
    # F5: notice thử lại đủ 5 lần, rồi /cancel kho nguồn (an toàn kể cả khi kho chưa nhận notice)
    paths = hq.kho[DC_BAC].paths()
    assert paths[: HTTP_MAX_RETRIES + 1] == ["/transfers/notice"] * (HTTP_MAX_RETRIES + 1)
    assert paths[-1] == f"/transfers/{tid}/cancel"
    assert hq.kho[DC_BAC].calls[-1][1] == {"reason": CancelReason.TIMEOUT}
    row = hq.store.rows[tid]
    assert (row["status"], row["cancel_reason"]) == ("CANCELLED", "TIMEOUT")


def test_create_cancel_also_fails_keeps_created_503(make_hq) -> None:
    hq = make_hq(dc1={"/transfers/notice": network_error(), "/cancel": network_error()})
    r = hq.client.post("/transfers", json=create_body())
    assert r.status_code == 503, r.text
    [tid] = hq.store.rows
    assert hq.store.rows[tid]["status"] == "CREATED"  # chờ nút Hủy phiếu (F10.f bước 3)
    assert hq.store.events == [(tid, None, "CREATED", "HQ")]


def test_create_destination_unreachable_cancels_source(make_hq) -> None:
    hq = make_hq(dc3={"/transfers/notice": network_error()})
    r = hq.client.post("/transfers", json=create_body())
    assert r.status_code == 503, r.text
    [tid] = hq.store.rows
    assert hq.kho[DC_BAC].paths() == ["/transfers/notice", f"/transfers/{tid}/cancel"]
    row = hq.store.rows[tid]
    assert (row["status"], row["cancel_reason"]) == ("CANCELLED", "TIMEOUT")


def test_create_destination_error_reply_cancels_source_and_returns_it(make_hq) -> None:
    bad = error_body(ErrorCode.VALIDATION_ERROR, "SKU hoặc kho không có trong danh mục")
    hq = make_hq(dc3={"/transfers/notice": httpx.Response(400, json=bad)})
    r = hq.client.post("/transfers", json=create_body())
    assert (r.status_code, r.json()) == (400, bad)  # F10.f: trả nguyên lỗi của kho
    [tid] = hq.store.rows
    assert hq.store.rows[tid]["status"] == "CANCELLED"


# ---------------------------------------------------------------------------
# GET /transfers, GET /transfers/{id} (F7)
# ---------------------------------------------------------------------------
def test_list_returns_array_and_filters_by_status(make_hq) -> None:
    hq = make_hq()
    first = created(hq)
    second = created(hq, qty=10, to_dc=DC_TRUNG)
    hq.store.rows[second]["status"] = "SHIPPED"
    everything = hq.client.get("/transfers").json()
    assert isinstance(everything, list) and len(everything) == 2  # Dashboard cần mảng
    shipped = hq.client.get("/transfers", params={"status": "SHIPPED"}).json()
    assert [t["transfer_id"] for t in shipped] == [str(second)]
    one = hq.client.get(f"/transfers/{first}")
    assert one.status_code == 200 and one.json()["status"] == "CREATED"
    assert one.json()["items"] == [{"product_id": 1, "qty": 50}]


def test_get_unknown_transfer_404(make_hq) -> None:
    r = make_hq().client.get(f"/transfers/{uuid4()}")
    assert r.status_code == 404 and r.json()["error_code"] == ErrorCode.NOT_FOUND


# ---------------------------------------------------------------------------
# /shipped, /received (F7, F9) và đồng bộ xuống kho (F10.d bước 1, hld.md)
# ---------------------------------------------------------------------------
def test_shipped_moves_to_shipped_and_syncs_destination(make_hq) -> None:
    hq = make_hq()
    tid = created(hq)
    r = hq.client.post(f"/transfers/{tid}/shipped", json={"shipped_at": SHIPPED_AT.isoformat()})
    assert r.status_code == 200, r.text
    assert r.json() == {"status": "SHIPPED", "duplicate": False}
    row = hq.store.rows[tid]
    assert (row["status"], row["shipped_at"]) == ("SHIPPED", SHIPPED_AT)
    assert hq.store.events[-1] == (tid, "CREATED", "SHIPPED", "DC1")
    # HQ đồng bộ SHIPPED xuống bản INBOUND ở kho đích để kho đích nhận được hàng
    assert hq.kho[DC_NAM].calls[-1] == (f"/transfers/{tid}/sync-status", {"status": "SHIPPED"})
    assert row["last_sync_at"] is not None


def test_shipped_again_is_duplicate_without_new_history_but_resyncs(make_hq) -> None:
    hq = make_hq()
    tid = created(hq)
    payload = {"shipped_at": SHIPPED_AT.isoformat()}
    hq.client.post(f"/transfers/{tid}/shipped", json=payload)
    events = list(hq.store.events)
    r = hq.client.post(f"/transfers/{tid}/shipped", json=payload)
    assert r.json() == {"status": "SHIPPED", "duplicate": True}
    assert hq.store.events == events  # db.md 2.13: callback trùng không thêm lịch sử
    syncs = [c for c in hq.kho[DC_NAM].calls if c[0].endswith("/sync-status")]
    assert len(syncs) == 2  # đồng bộ lại, sửa được lần đồng bộ trước bị lỗi


def test_shipped_sync_failure_does_not_fail_callback(make_hq) -> None:
    hq = make_hq(dc3={"/sync-status": network_error()})
    tid = created(hq)
    r = hq.client.post(f"/transfers/{tid}/shipped", json={"shipped_at": SHIPPED_AT.isoformat()})
    assert r.status_code == 200, r.text
    assert hq.store.rows[tid]["status"] == "SHIPPED"
    assert hq.store.rows[tid]["last_sync_at"] is None


def test_shipped_on_cancelled_is_409(make_hq) -> None:
    hq = make_hq()
    tid = created(hq)
    hq.store.rows[tid]["status"] = "CANCELLED"
    r = hq.client.post(f"/transfers/{tid}/shipped", json={"shipped_at": SHIPPED_AT.isoformat()})
    assert r.status_code == 409 and r.json()["error_code"] == ErrorCode.INVALID_STATE


def test_received_moves_to_received_and_syncs_source(make_hq) -> None:
    hq = make_hq()
    tid = created(hq)
    hq.client.post(f"/transfers/{tid}/shipped", json={"shipped_at": SHIPPED_AT.isoformat()})
    r = hq.client.post(f"/transfers/{tid}/received", json={"received_at": RECEIVED_AT.isoformat()})
    assert r.json() == {"status": "RECEIVED", "duplicate": False}
    row = hq.store.rows[tid]
    assert (row["status"], row["received_at"]) == ("RECEIVED", RECEIVED_AT)
    assert hq.store.events[-1] == (tid, "SHIPPED", "RECEIVED", "DC3")
    # hld.md KB1, TC05: RECEIVED ở cả hai bản transfer_order -> đồng bộ xuống kho nguồn
    assert hq.kho[DC_BAC].calls[-1] == (f"/transfers/{tid}/sync-status", {"status": "RECEIVED"})


def test_received_before_shipped_is_409(make_hq) -> None:
    hq = make_hq()
    tid = created(hq)
    r = hq.client.post(f"/transfers/{tid}/received", json={"received_at": RECEIVED_AT.isoformat()})
    assert r.status_code == 409 and r.json()["error_code"] == ErrorCode.INVALID_STATE
    assert hq.store.rows[tid]["status"] == "CREATED"


# ---------------------------------------------------------------------------
# POST /transfers/{id}/decline (F10.g)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("reason", [CancelReason.DECLINED, CancelReason.TIMEOUT])
def test_decline_cancels_source_first_then_records_and_syncs(make_hq, reason) -> None:
    hq = make_hq(dc2={"/cancel": httpx.Response(200, json={"status": "CANCELLED"})})
    tid = created(hq, qty=10, to_dc=DC_TRUNG)
    hq.kho[DC_BAC].calls.clear()
    r = hq.client.post(f"/transfers/{tid}/decline", json={"reason": reason})
    assert r.status_code == 200, r.text
    assert r.json() == {"status": "CANCELLED", "duplicate": False}
    # Bước 1: kho nguồn quyết định (db.md 3.2 quy tắc 4)
    assert hq.kho[DC_BAC].calls == [(f"/transfers/{tid}/cancel", {"reason": reason})]
    row = hq.store.rows[tid]
    assert (row["status"], row["cancel_reason"]) == ("CANCELLED", str(reason))
    assert row["cancelled_at"] is not None
    assert hq.store.events[-1] == (tid, "CREATED", "CANCELLED", "HQ")
    # Bước 2: đồng bộ CANCELLED xuống kho đích
    assert hq.kho[DC_TRUNG].calls[-1] == (
        f"/transfers/{tid}/sync-status", {"status": "CANCELLED"}
    )  # fmt: skip


def test_decline_after_ship_is_409_and_keeps_transfer(make_hq) -> None:
    shipped = error_body(ErrorCode.INVALID_STATE, "Phiếu đã xuất", {"status": "SHIPPED"})
    hq = make_hq(dc1={"/cancel": httpx.Response(409, json=shipped)})
    tid = created(hq, qty=10, to_dc=DC_TRUNG)
    hq.store.rows[tid]["status"] = "SHIPPED"
    r = hq.client.post(f"/transfers/{tid}/decline", json={"reason": "DECLINED"})
    assert r.status_code == 409 and r.json()["error_code"] == ErrorCode.INVALID_STATE
    assert hq.store.rows[tid]["status"] == "SHIPPED"
    assert not any(p.endswith("/sync-status") for p in hq.kho[DC_TRUNG].paths())


def test_decline_other_source_error_is_returned_as_is(make_hq) -> None:
    # F10.g bước 3 (spec mới): chỉ INVALID_STATE là "đã xuất"; lỗi khác trả nguyên, phiếu giữ nguyên
    conflict = error_body(ErrorCode.VERSION_CONFLICT, "Xung đột version")
    hq = make_hq(dc1={"/cancel": httpx.Response(409, json=conflict)})
    tid = created(hq, qty=10, to_dc=DC_TRUNG)
    r = hq.client.post(f"/transfers/{tid}/decline", json={"reason": "DECLINED"})
    assert (r.status_code, r.json()) == (409, conflict)
    assert hq.store.rows[tid]["status"] == "CREATED"


def test_decline_source_unreachable_is_503(make_hq) -> None:
    hq = make_hq(dc1={"/cancel": network_error()})
    tid = created(hq, qty=10, to_dc=DC_TRUNG)
    r = hq.client.post(f"/transfers/{tid}/decline", json={"reason": "TIMEOUT"})
    assert r.status_code == 503 and r.json()["error_code"] == ErrorCode.DC_UNREACHABLE
    assert hq.store.rows[tid]["status"] == "CREATED"


def test_decline_already_cancelled_is_duplicate(make_hq) -> None:
    hq = make_hq()
    tid = created(hq, qty=10, to_dc=DC_TRUNG)
    hq.client.post(f"/transfers/{tid}/decline", json={"reason": "DECLINED"})
    events = list(hq.store.events)
    r = hq.client.post(f"/transfers/{tid}/decline", json={"reason": "DECLINED"})
    assert r.json() == {"status": "CANCELLED", "duplicate": True}
    assert hq.store.events == events


def test_decline_rejects_insufficient_stock_reason(make_hq) -> None:
    # F7, F10.g: lý do hủy từ kho đích là DECLINED, từ nút Hủy phiếu là TIMEOUT
    hq = make_hq()
    tid = created(hq)
    r = hq.client.post(f"/transfers/{tid}/decline", json={"reason": "INSUFFICIENT_STOCK"})
    assert r.status_code == 400 and r.json()["error_code"] == ErrorCode.VALIDATION_ERROR


def test_decline_unknown_transfer_404(make_hq) -> None:
    r = make_hq().client.post(f"/transfers/{uuid4()}/decline", json={"reason": "DECLINED"})
    assert r.status_code == 404
