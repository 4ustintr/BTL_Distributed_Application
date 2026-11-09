"""Unit test Health Monitor (F9 bảng dc_status, F10.j), hỗ trợ TC14 và số đo M2. Không cần CSDL.

Kho thật được giả bằng httpx.MockTransport; dc_status giả trong bộ nhớ, cùng quy tắc UPDATE có
điều kiện như PgHealthStore.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx
import pytest
from workers import health
from workers.health import FailureResult, HealthMonitor, next_status_on_success, should_go_offline

from common.constants import DcStatus, NodeMode
from common.http import InternalClient

DC_BAC, DC_TRUNG, DC_NAM = 1, 2, 3


# ---------------------------------------------------------------------------
# Dữ liệu giả
# ---------------------------------------------------------------------------
@dataclass
class Row:
    status: DcStatus
    missed_count: int = 0
    node_mode: NodeMode | None = None
    heartbeats: int = 0
    status_changes: int = 0  # số lần ghi updated_at


class FakeHealthStore:
    def __init__(self, rows: dict[int, Row], pending: set[int] | None = None) -> None:
        self.rows = rows
        self.pending = pending or set()

    async def record_success(self, dc_id: int, mode: NodeMode) -> DcStatus | None:
        row = self.rows.get(dc_id)
        if row is None:
            return None
        row.missed_count, row.node_mode = 0, mode
        row.heartbeats += 1
        return row.status

    async def record_failure(self, dc_id: int) -> FailureResult | None:
        row = self.rows.get(dc_id)
        if row is None:
            return None
        row.missed_count += 1
        return FailureResult(row.status, row.missed_count)

    async def has_pending(self, dc_id: int) -> bool:
        return dc_id in self.pending

    async def transition(
        self, dc_id: int, from_status: tuple[DcStatus, ...], to_status: DcStatus
    ) -> bool:
        row = self.rows[dc_id]
        if row.status not in from_status:
            return False
        row.status = to_status
        row.status_changes += 1
        return True


class FakeNode:
    """Một Node API giả: "ONLINE", "RECOVERING", "DOWN" (từ chối kết nối), "DB_ERROR" (503)."""

    def __init__(self, dc_id: int, state: str = "ONLINE") -> None:
        self.dc_id = dc_id
        self.state = state
        self.calls = 0

    async def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        assert request.url.path == "/health"
        if self.state == "DOWN":
            raise httpx.ConnectError("connection refused", request=request)
        if self.state == "HANG":
            await asyncio.sleep(5)
        if self.state == "DB_ERROR":
            return httpx.Response(503, json={"dc_id": self.dc_id, "mode": "ONLINE", "db": "error"})
        if self.state == "GARBAGE":
            return httpx.Response(200, text="<html>")
        return httpx.Response(200, json={"dc_id": self.dc_id, "mode": self.state, "db": "ok"})

    def client(self) -> InternalClient:
        return InternalClient(
            f"http://node{self.dc_id}", "k", transport=httpx.MockTransport(self.handler)
        )


def monitor_for(store: FakeHealthStore, *nodes: FakeNode) -> HealthMonitor:
    return HealthMonitor(store, {n.dc_id: n.client() for n in nodes})


# ---------------------------------------------------------------------------
# Quy tắc F9 (hàm thuần)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("current", "mode", "has_pending", "expected"),
    [
        (DcStatus.OFFLINE, NodeMode.RECOVERING, False, DcStatus.RECOVERING),  # kho khởi động lại
        (DcStatus.OFFLINE, NodeMode.ONLINE, True, DcStatus.RECOVERING),  # còn PENDING
        (DcStatus.OFFLINE, NodeMode.ONLINE, False, DcStatus.ONLINE),  # chỉ mất mạng
        (DcStatus.RECOVERING, NodeMode.ONLINE, False, None),  # việc của Replay Worker
        (DcStatus.RECOVERING, NodeMode.RECOVERING, True, None),
        (DcStatus.ONLINE, NodeMode.ONLINE, False, None),
        (DcStatus.ONLINE, NodeMode.RECOVERING, False, DcStatus.RECOVERING),  # khởi động lại nhanh
        (DcStatus.ONLINE, NodeMode.RECOVERING, True, DcStatus.RECOVERING),
    ],
)
def test_next_status_on_success(
    current: DcStatus, mode: NodeMode, has_pending: bool, expected: DcStatus | None
) -> None:
    assert next_status_on_success(current, mode, has_pending) == expected


@pytest.mark.parametrize(
    ("current", "missed", "expected"),
    [
        (DcStatus.ONLINE, 2, False),
        (DcStatus.ONLINE, 3, True),
        (DcStatus.RECOVERING, 3, True),
        (DcStatus.OFFLINE, 3, False),
        (DcStatus.OFFLINE, 10, False),
    ],
)
def test_should_go_offline(current: DcStatus, missed: int, expected: bool) -> None:
    assert should_go_offline(current, missed) is expected


# ---------------------------------------------------------------------------
# probe: GET /health, timeout 1 giây, không thử lại (F10.j bước 1)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("ONLINE", NodeMode.ONLINE),
        ("RECOVERING", NodeMode.RECOVERING),
        ("DOWN", None),
        ("DB_ERROR", None),
        ("GARBAGE", None),
    ],
)
async def test_probe(state: str, expected: NodeMode | None) -> None:
    node = FakeNode(DC_TRUNG, state)
    assert await health.probe(node.client()) == expected
    assert node.calls == 1  # không thử lại


async def test_probe_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(health, "HEALTH_CHECK_TIMEOUT_S", 0.05)
    node = FakeNode(DC_TRUNG, "HANG")
    loop = asyncio.get_running_loop()
    started = loop.time()
    assert await health.probe(node.client()) is None
    assert loop.time() - started < 1


# ---------------------------------------------------------------------------
# Luồng của Kịch bản 3 / TC14: ONLINE -> OFFLINE -> RECOVERING
# ---------------------------------------------------------------------------
async def test_three_misses_mark_offline_once() -> None:
    store = FakeHealthStore({DC_TRUNG: Row(DcStatus.ONLINE)})
    node = FakeNode(DC_TRUNG, "DOWN")
    monitor = monitor_for(store, node)
    row = store.rows[DC_TRUNG]

    for _ in range(2):
        await monitor.tick()
    assert (row.status, row.missed_count) == (DcStatus.ONLINE, 2)

    await monitor.tick()  # lần lỗi thứ 3 (khoảng 6 giây, M2)
    assert (row.status, row.missed_count, row.status_changes) == (DcStatus.OFFLINE, 3, 1)

    await monitor.tick()  # đã OFFLINE: không ghi updated_at lần nữa
    assert (row.status, row.missed_count, row.status_changes) == (DcStatus.OFFLINE, 4, 1)


async def test_db_error_counts_as_miss() -> None:
    store = FakeHealthStore({DC_TRUNG: Row(DcStatus.RECOVERING)})
    monitor = monitor_for(store, FakeNode(DC_TRUNG, "DB_ERROR"))
    for _ in range(3):
        await monitor.tick()
    assert store.rows[DC_TRUNG].status == DcStatus.OFFLINE


async def test_success_resets_missed_count() -> None:
    store = FakeHealthStore({DC_TRUNG: Row(DcStatus.ONLINE)})
    node = FakeNode(DC_TRUNG, "DOWN")
    monitor = monitor_for(store, node)
    await monitor.tick()
    await monitor.tick()
    node.state = "ONLINE"
    await monitor.tick()
    row = store.rows[DC_TRUNG]
    assert (row.status, row.missed_count, row.node_mode) == (DcStatus.ONLINE, 0, NodeMode.ONLINE)


async def test_restarted_dc_goes_recovering() -> None:
    """KB3 bước 7: kho lên lại ở RECOVERING -> dc_status RECOVERING, ngừng nhận failover."""
    store = FakeHealthStore({DC_TRUNG: Row(DcStatus.OFFLINE, missed_count=9)})
    await monitor_for(store, FakeNode(DC_TRUNG, "RECOVERING")).tick()
    row = store.rows[DC_TRUNG]
    assert (row.status, row.missed_count, row.node_mode) == (
        DcStatus.RECOVERING,
        0,
        NodeMode.RECOVERING,
    )
    assert row.heartbeats == 1


async def test_network_blip_without_pending_goes_online() -> None:
    store = FakeHealthStore({DC_TRUNG: Row(DcStatus.OFFLINE)})
    await monitor_for(store, FakeNode(DC_TRUNG, "ONLINE")).tick()
    assert store.rows[DC_TRUNG].status == DcStatus.ONLINE


async def test_network_blip_with_pending_goes_recovering() -> None:
    store = FakeHealthStore({DC_TRUNG: Row(DcStatus.OFFLINE)}, pending={DC_TRUNG})
    await monitor_for(store, FakeNode(DC_TRUNG, "ONLINE")).tick()
    assert store.rows[DC_TRUNG].status == DcStatus.RECOVERING


async def test_fast_restart_online_to_recovering() -> None:
    """F9: kho khởi động lại nhanh hơn 3 chu kỳ (docker restart) -> ONLINE -> RECOVERING.

    Nhờ vậy Replay Worker thấy kho và gọi /recovery/complete, kho không kẹt ở RECOVERING.
    """
    store = FakeHealthStore({DC_TRUNG: Row(DcStatus.ONLINE)})
    node = FakeNode(DC_TRUNG, "DOWN")
    monitor = monitor_for(store, node)
    await monitor.tick()  # 1 lần lỗi, chưa tới OFFLINE
    node.state = "RECOVERING"
    await monitor.tick()
    row = store.rows[DC_TRUNG]
    assert (row.status, row.missed_count, row.node_mode, row.status_changes) == (
        DcStatus.RECOVERING,
        0,
        NodeMode.RECOVERING,
        1,
    )
    await monitor.tick()  # đã RECOVERING: không đổi thêm
    assert row.status_changes == 1


async def test_recovering_to_online_is_left_to_replay_worker() -> None:
    store = FakeHealthStore({DC_TRUNG: Row(DcStatus.RECOVERING)})
    await monitor_for(store, FakeNode(DC_TRUNG, "ONLINE")).tick()
    row = store.rows[DC_TRUNG]
    assert (row.status, row.node_mode) == (DcStatus.RECOVERING, NodeMode.ONLINE)


async def test_one_dead_dc_does_not_affect_others() -> None:
    store = FakeHealthStore({d: Row(DcStatus.ONLINE) for d in (DC_BAC, DC_TRUNG, DC_NAM)})
    nodes = (FakeNode(DC_BAC), FakeNode(DC_TRUNG, "DOWN"), FakeNode(DC_NAM))
    monitor = monitor_for(store, *nodes)
    for _ in range(3):
        await monitor.tick()
    assert [store.rows[d].status for d in (DC_BAC, DC_TRUNG, DC_NAM)] == [
        DcStatus.ONLINE,
        DcStatus.OFFLINE,
        DcStatus.ONLINE,
    ]
    assert store.rows[DC_BAC].heartbeats == 3


async def test_missing_dc_status_row_is_skipped() -> None:
    store = FakeHealthStore({DC_BAC: Row(DcStatus.ONLINE)})  # chưa seed kho 2
    await monitor_for(store, FakeNode(DC_BAC), FakeNode(DC_TRUNG, "DOWN")).tick()
    assert DC_TRUNG not in store.rows
    assert store.rows[DC_BAC].heartbeats == 1
