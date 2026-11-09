"""Unit test Replay Worker (F9 bảng pending_txn, F10.i), hỗ trợ TC14, TC16, make seed. Không CSDL.

Kho giả bằng httpx.MockTransport trả đúng định dạng F6 /replay, /recovery/complete.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from workers import replay
from workers.replay import Outcome, PendingRow, ReplayWorker, classify, error_message

from common.constants import DcStatus, PendingTxnStatus, TxnType
from common.errors import error_body
from common.http import InternalClient

DC_TRUNG = 2
T0 = datetime(2026, 11, 8, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Dữ liệu giả
# ---------------------------------------------------------------------------
@dataclass
class Txn:
    txn_id: UUID
    dc_id: int
    txn_type: TxnType
    qty: int
    created_at: datetime
    status: PendingTxnStatus = PendingTxnStatus.PENDING
    attempts: int = 0
    error_msg: str | None = None
    product_id: int = 1
    created_by: str = "nv-trung"


@dataclass
class FakeReplayStore:
    dc_status: dict[int, DcStatus]
    txns: list[Txn] = field(default_factory=list)

    async def recovering_dcs(self) -> list[int]:
        return sorted(d for d, s in self.dc_status.items() if s == DcStatus.RECOVERING)

    async def pending(self, dc_id: int) -> list[PendingRow]:
        rows = [t for t in self.txns if t.dc_id == dc_id and t.status == PendingTxnStatus.PENDING]
        rows.sort(key=lambda t: t.created_at)
        return [PendingRow(t.txn_id, t.txn_type, t.product_id, t.qty, t.created_by) for t in rows]

    def _get(self, txn_id: UUID) -> Txn:
        return next(t for t in self.txns if t.txn_id == txn_id)

    async def mark_attempt(self, txn_id: UUID) -> None:
        self._get(txn_id).attempts += 1

    async def mark_applied(self, txn_id: UUID) -> bool:
        t = self._get(txn_id)
        if t.status != PendingTxnStatus.PENDING:
            return False
        t.status = PendingTxnStatus.APPLIED
        return True

    async def mark_rejected(self, txn_id: UUID, error_msg: str) -> bool:
        t = self._get(txn_id)
        if t.status != PendingTxnStatus.PENDING:
            return False
        t.status, t.error_msg = PendingTxnStatus.REJECTED, error_msg
        return True

    async def has_pending(self, dc_id: int) -> bool:
        return any(t.dc_id == dc_id and t.status == PendingTxnStatus.PENDING for t in self.txns)

    async def set_online(self, dc_id: int) -> bool:
        if self.dc_status[dc_id] != DcStatus.RECOVERING or await self.has_pending(dc_id):
            return False
        self.dc_status[dc_id] = DcStatus.ONLINE
        return True


class FakeNode:
    """Node API giả. replies[txn_id] là phản hồi /replay; mặc định 200 duplicate=false."""

    def __init__(self, dc_id: int = DC_TRUNG) -> None:
        self.dc_id = dc_id
        self.replies: dict[str, httpx.Response | Exception] = {}
        self.complete_reply: httpx.Response | Exception = httpx.Response(
            200, json={"mode": "ONLINE"}
        )
        self.replayed: list[dict[str, object]] = []
        self.complete_calls = 0
        self.delay = 0.0

    async def handler(self, request: httpx.Request) -> httpx.Response:
        if self.delay:
            await asyncio.sleep(self.delay)
        if request.url.path == "/replay":
            body = json.loads(request.content)
            self.replayed.append(body)
            reply = self.replies.get(
                body["request_id"], httpx.Response(200, json={"qty_on_hand": 0, "duplicate": False})
            )
        else:
            assert request.url.path == "/recovery/complete"
            self.complete_calls += 1
            reply = self.complete_reply
        if isinstance(reply, Exception):
            raise reply
        return reply

    def client(self) -> InternalClient:
        return InternalClient(
            f"http://node{self.dc_id}", "k", transport=httpx.MockTransport(self.handler)
        )


def ok(duplicate: bool) -> httpx.Response:
    return httpx.Response(200, json={"qty_on_hand": 55, "duplicate": duplicate})


def insufficient() -> httpx.Response:
    return httpx.Response(422, json=error_body("INSUFFICIENT_STOCK", "Không đủ hàng"))


def validation_error() -> httpx.Response:
    return httpx.Response(400, json=error_body("VALIDATION_ERROR", "SKU không có trong danh mục"))


def txn(txn_type: TxnType, qty: int, minute: int, dc_id: int = DC_TRUNG) -> Txn:
    return Txn(uuid4(), dc_id, txn_type, qty, T0 + timedelta(minutes=minute))


def worker_for(store: FakeReplayStore, *nodes: FakeNode) -> ReplayWorker:
    return ReplayWorker(store, {n.dc_id: n.client() for n in nodes})


# ---------------------------------------------------------------------------
# Phân loại phản hồi (F10.i bước 2)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("resp", "expected"),
    [
        (ok(False), Outcome.SENT),
        (ok(True), Outcome.APPLIED),
        (insufficient(), Outcome.REJECTED),
        (httpx.Response(500), Outcome.STOP),
        (httpx.Response(503, json=error_body("DC_UNREACHABLE", "x")), Outcome.STOP),
        (validation_error(), Outcome.REJECTED),
        (httpx.Response(400, text="<html>"), Outcome.STOP),
        (httpx.Response(401, json=error_body("UNAUTHORIZED", "x")), Outcome.STOP),
        (httpx.Response(409, json=error_body("VERSION_CONFLICT", "x")), Outcome.STOP),
        (httpx.Response(200, text="<html>"), Outcome.STOP),
    ],
)
def test_classify(resp: httpx.Response, expected: Outcome) -> None:
    assert classify(resp) is expected


def test_error_message_keeps_error_code() -> None:
    assert error_message(insufficient()).startswith("INSUFFICIENT_STOCK")
    assert error_message(httpx.Response(422, text="oops")) == "HTTP 422: oops"


# ---------------------------------------------------------------------------
# make seed: kho RECOVERING, không có PENDING -> /recovery/complete -> ONLINE
# ---------------------------------------------------------------------------
async def test_no_pending_completes_recovery() -> None:
    store = FakeReplayStore({1: DcStatus.RECOVERING, 2: DcStatus.RECOVERING, 3: DcStatus.ONLINE})
    nodes = [FakeNode(d) for d in (1, 2, 3)]
    await worker_for(store, *nodes).tick()
    assert store.dc_status == {1: DcStatus.ONLINE, 2: DcStatus.ONLINE, 3: DcStatus.ONLINE}
    assert [n.complete_calls for n in nodes] == [1, 1, 0]  # kho đã ONLINE thì không gọi


async def test_offline_dc_is_not_replayed() -> None:
    store = FakeReplayStore({DC_TRUNG: DcStatus.OFFLINE}, [txn(TxnType.INBOUND, 30, 1)])
    node = FakeNode()
    await worker_for(store, node).tick()
    assert node.replayed == [] and node.complete_calls == 0


async def test_complete_failure_keeps_recovering() -> None:
    store = FakeReplayStore({DC_TRUNG: DcStatus.RECOVERING})
    node = FakeNode()
    node.complete_reply = httpx.Response(503)
    await worker_for(store, node).tick()
    assert store.dc_status[DC_TRUNG] == DcStatus.RECOVERING


# ---------------------------------------------------------------------------
# Kịch bản 3 / TC14: gửi theo created_at, chờ CDC, rồi ONLINE
# ---------------------------------------------------------------------------
async def test_kb3_rows_sent_in_order_then_wait_for_cdc() -> None:
    t_in, t_out = txn(TxnType.INBOUND, 30, 1), txn(TxnType.OUTBOUND, 10, 2)
    store = FakeReplayStore({DC_TRUNG: DcStatus.RECOVERING}, [t_out, t_in])
    node = FakeNode()
    worker = worker_for(store, node)

    await worker.tick()
    assert [b["request_id"] for b in node.replayed] == [str(t_in.txn_id), str(t_out.txn_id)]
    assert node.replayed[0] == {
        "request_id": str(t_in.txn_id),
        "txn_type": "INBOUND",
        "product_id": 1,
        "qty": 30,
        "created_by": "nv-trung",
    }
    # 200 duplicate=false: vẫn PENDING chờ Consumer, chưa gọi complete
    assert {t.status for t in store.txns} == {PendingTxnStatus.PENDING}
    assert node.complete_calls == 0
    assert store.dc_status[DC_TRUNG] == DcStatus.RECOVERING

    # Vòng sau gửi lại (an toàn vì kho chặn trùng); kho trả duplicate=true -> APPLIED
    node.replies = {str(t.txn_id): ok(True) for t in (t_in, t_out)}
    await worker.tick()
    assert {t.status for t in store.txns} == {PendingTxnStatus.APPLIED}
    assert [t.attempts for t in (t_in, t_out)] == [2, 2]
    assert node.complete_calls == 1
    assert store.dc_status[DC_TRUNG] == DcStatus.ONLINE


async def test_consumer_applied_rows_then_complete() -> None:
    t_in = txn(TxnType.INBOUND, 30, 1)
    store = FakeReplayStore({DC_TRUNG: DcStatus.RECOVERING}, [t_in])
    node = FakeNode()
    worker = worker_for(store, node)
    await worker.tick()
    t_in.status = PendingTxnStatus.APPLIED  # Consumer thấy request_id trong InventoryChanged
    await worker.tick()
    assert len(node.replayed) == 1  # dòng đã APPLIED không gửi lại
    assert store.dc_status[DC_TRUNG] == DcStatus.ONLINE


# ---------------------------------------------------------------------------
# TC16: kho trả 422 -> REJECTED, error_msg chứa INSUFFICIENT_STOCK, dòng sau vẫn gửi
# ---------------------------------------------------------------------------
async def test_tc16_rejected_row_records_error_and_continues() -> None:
    t_bad, t_next = txn(TxnType.OUTBOUND, 30, 1), txn(TxnType.INBOUND, 5, 2)
    store = FakeReplayStore({DC_TRUNG: DcStatus.RECOVERING}, [t_bad, t_next])
    node = FakeNode()
    node.replies = {str(t_bad.txn_id): insufficient(), str(t_next.txn_id): ok(True)}
    await worker_for(store, node).tick()
    assert t_bad.status == PendingTxnStatus.REJECTED
    assert t_bad.error_msg is not None and "INSUFFICIENT_STOCK" in t_bad.error_msg
    assert t_next.status == PendingTxnStatus.APPLIED
    assert store.dc_status[DC_TRUNG] == DcStatus.ONLINE


# ---------------------------------------------------------------------------
# 400 VALIDATION_ERROR: dữ liệu lệnh sai, gửi lại vẫn sai -> REJECTED như 422 (F10.i, F9)
# ---------------------------------------------------------------------------
async def test_validation_error_rejects_row_and_continues() -> None:
    t_bad, t_next = txn(TxnType.OUTBOUND, 10, 1), txn(TxnType.INBOUND, 5, 2)
    store = FakeReplayStore({DC_TRUNG: DcStatus.RECOVERING}, [t_bad, t_next])
    node = FakeNode()
    node.replies = {
        str(t_bad.txn_id): validation_error(),
        str(t_next.txn_id): ok(True),
    }
    await worker_for(store, node).tick()
    assert t_bad.status == PendingTxnStatus.REJECTED
    assert t_bad.error_msg is not None and t_bad.error_msg.startswith("VALIDATION_ERROR")
    assert t_next.status == PendingTxnStatus.APPLIED  # không kẹt kho ở RECOVERING
    assert store.dc_status[DC_TRUNG] == DcStatus.ONLINE


# ---------------------------------------------------------------------------
# 4xx khác (401, 404, 409 VERSION_CONFLICT, 400 không phải VALIDATION_ERROR): dừng vòng, giữ
# PENDING, log ERROR – có thể tự hết hoặc do cấu hình, không được làm mất giao dịch (F10.i)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("status", "code"),
    [
        (401, "UNAUTHORIZED"),
        (404, "NOT_FOUND"),
        (409, "VERSION_CONFLICT"),
        (400, "SOMETHING_ELSE"),
    ],
)
async def test_other_4xx_logs_error_with_txn_and_code(
    status: int, code: str, caplog: pytest.LogCaptureFixture
) -> None:
    """4xx khác: giữ PENDING, dừng vòng, log ERROR kèm txn_id và error_code."""
    t1, t2 = txn(TxnType.INBOUND, 30, 1), txn(TxnType.OUTBOUND, 10, 2)
    store = FakeReplayStore({DC_TRUNG: DcStatus.RECOVERING}, [t1, t2])
    node = FakeNode()
    node.replies = {str(t1.txn_id): httpx.Response(status, json=error_body(code, "x"))}
    with caplog.at_level("ERROR", logger="hq-api.replay"):
        await worker_for(store, node).tick()
    assert (t1.status, t2.status) == (PendingTxnStatus.PENDING, PendingTxnStatus.PENDING)
    assert len(node.replayed) == 1 and node.complete_calls == 0
    errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1
    assert str(t1.txn_id) in errors[0] and code in errors[0]


# ---------------------------------------------------------------------------
# Lỗi mạng hoặc 5xx: dừng vòng, không nhảy sang dòng sau, không thử lại trong vòng
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "failure",
    [httpx.Response(503), httpx.ConnectError("refused"), httpx.ReadTimeout("slow")],
)
async def test_failure_stops_round_and_keeps_order(failure: httpx.Response | Exception) -> None:
    t1, t2 = txn(TxnType.INBOUND, 30, 1), txn(TxnType.OUTBOUND, 10, 2)
    store = FakeReplayStore({DC_TRUNG: DcStatus.RECOVERING}, [t1, t2])
    node = FakeNode()
    node.replies = {str(t1.txn_id): failure}
    await worker_for(store, node).tick()
    assert [b["request_id"] for b in node.replayed] == [str(t1.txn_id)]  # gửi đúng 1 lần
    assert (t1.status, t2.status) == (PendingTxnStatus.PENDING, PendingTxnStatus.PENDING)
    assert node.complete_calls == 0


async def test_round_time_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(replay, "REPLAY_ROUND_MAX_S", 0.05)
    store = FakeReplayStore({DC_TRUNG: DcStatus.RECOVERING}, [txn(TxnType.INBOUND, 30, 1)])
    node = FakeNode()
    node.delay = 1.0
    await worker_for(store, node).tick()  # không ném lỗi, dòng vẫn PENDING
    assert store.txns[0].status == PendingTxnStatus.PENDING
    assert store.dc_status[DC_TRUNG] == DcStatus.RECOVERING
