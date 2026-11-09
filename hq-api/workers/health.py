"""Health Monitor: mỗi 2 giây gọi GET /health từng kho – F9, F10.j (Nhật Khánh).

Chỉ UPDATE dc_status, không INSERT: seed tạo sẵn 3 dòng ở trạng thái RECOVERING (db.md 2.14).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

import asyncpg
import httpx
from fastapi import FastAPI
from pydantic import ValidationError

from common.constants import (
    HEALTH_CHECK_INTERVAL_S,
    HEALTH_CHECK_TIMEOUT_S,
    OFFLINE_MISSED_THRESHOLD,
    DcStatus,
    NodeMode,
    PendingTxnStatus,
)
from common.http import InternalClient
from common.models import NodeHealthResponse
from workers.loop import run_periodic

log = logging.getLogger("hq-api.health")

NODE_HEALTH_PATH = "/health"  # F6
SMALLINT_MAX = 32767  # db.md 2.14: missed_count SMALLINT – không để tràn khi kho sập lâu

# F9: các trạng thái được phép chuyển sang OFFLINE
OFFLINE_FROM: tuple[DcStatus, ...] = (DcStatus.ONLINE, DcStatus.RECOVERING)


# ---------------------------------------------------------------------------
# Quy tắc F9 (hàm thuần, test không cần CSDL)
# ---------------------------------------------------------------------------
def next_status_on_success(current: DcStatus, mode: NodeMode, has_pending: bool) -> DcStatus | None:
    """Bảng dc_status ở F9, các dòng Health Monitor kích hoạt khi health check thành công.

    - OFFLINE -> RECOVERING: kho báo RECOVERING hoặc còn pending_txn PENDING.
    - OFFLINE -> ONLINE: kho báo ONLINE và không còn PENDING (kho chỉ mất mạng).
    - ONLINE -> RECOVERING: kho báo RECOVERING (khởi động lại nhanh hơn 3 chu kỳ).
    RECOVERING -> ONLINE là việc của Replay Worker (F9, F10.i bước 3). Trả None = giữ nguyên.
    """
    if current == DcStatus.OFFLINE:
        if mode == NodeMode.RECOVERING or has_pending:
            return DcStatus.RECOVERING
        return DcStatus.ONLINE
    if current == DcStatus.ONLINE and mode == NodeMode.RECOVERING:
        return DcStatus.RECOVERING
    return None


def should_go_offline(current: DcStatus, missed_count: int) -> bool:
    """F10.j bước 3: đạt OFFLINE_MISSED_THRESHOLD lần lỗi liên tiếp và kho chưa OFFLINE."""
    return current != DcStatus.OFFLINE and missed_count >= OFFLINE_MISSED_THRESHOLD


async def probe(client: InternalClient) -> NodeMode | None:
    """F10.j bước 1: GET /health, timeout 1 giây, không thử lại. Thành công thì trả mode."""
    try:
        async with asyncio.timeout(HEALTH_CHECK_TIMEOUT_S):
            resp = await client.get(
                NODE_HEALTH_PATH, request_timeout=HEALTH_CHECK_TIMEOUT_S, max_retries=0
            )
    except (httpx.HTTPError, TimeoutError):
        return None
    # GIẢ ĐỊNH: chỉ 200 là thành công; 503 (CSDL kho lỗi, F6) tính là một lần thất bại.
    if resp.status_code != 200:
        return None
    try:
        return NodeHealthResponse.model_validate(resp.json()).mode
    except (ValueError, ValidationError):
        return None


# ---------------------------------------------------------------------------
# Truy cập dc_status
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FailureResult:
    status: DcStatus
    missed_count: int


class HealthStore(Protocol):
    async def record_success(self, dc_id: int, mode: NodeMode) -> DcStatus | None: ...
    async def record_failure(self, dc_id: int) -> FailureResult | None: ...
    async def has_pending(self, dc_id: int) -> bool: ...
    async def transition(
        self, dc_id: int, from_status: tuple[DcStatus, ...], to_status: DcStatus
    ) -> bool: ...


class PgHealthStore:
    """dc_status và pending_txn ở hq_db (db.md 2.12, 2.14)."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def record_success(self, dc_id: int, mode: NodeMode) -> DcStatus | None:
        """F10.j bước 2: missed_count = 0, ghi last_heartbeat_at và node_mode."""
        status = await self.pool.fetchval(
            """UPDATE dc_status
               SET missed_count = 0, last_heartbeat_at = now(), node_mode = $2
               WHERE dc_id = $1
               RETURNING status""",
            dc_id,
            mode,
        )
        return None if status is None else DcStatus(status)

    async def record_failure(self, dc_id: int) -> FailureResult | None:
        """F10.j bước 3: tăng missed_count."""
        row = await self.pool.fetchrow(
            """UPDATE dc_status
               SET missed_count = LEAST(missed_count + 1, $2)
               WHERE dc_id = $1
               RETURNING status, missed_count""",
            dc_id,
            SMALLINT_MAX,
        )
        if row is None:
            return None
        return FailureResult(DcStatus(row["status"]), row["missed_count"])

    async def has_pending(self, dc_id: int) -> bool:
        return bool(
            await self.pool.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pending_txn WHERE dc_id = $1 AND status = $2)",
                dc_id,
                PendingTxnStatus.PENDING,
            )
        )

    async def transition(
        self, dc_id: int, from_status: tuple[DcStatus, ...], to_status: DcStatus
    ) -> bool:
        """Đổi status có điều kiện trạng thái liền trước; ghi updated_at (db.md 2.14, M2)."""
        result = await self.pool.execute(
            """UPDATE dc_status SET status = $3, updated_at = now()
               WHERE dc_id = $1 AND status = ANY($2::varchar[])""",
            dc_id,
            [str(s) for s in from_status],
            to_status,
        )
        return result == "UPDATE 1"


# ---------------------------------------------------------------------------
# Health Monitor
# ---------------------------------------------------------------------------
class HealthMonitor:
    def __init__(self, store: HealthStore, nodes: Mapping[int, InternalClient]) -> None:
        self.store = store
        self.nodes = nodes

    async def tick(self) -> None:
        """Một chu kỳ: kiểm tra mọi kho song song, kho treo không làm chậm kho khác."""
        results = await asyncio.gather(
            *(self.check(dc_id) for dc_id in self.nodes), return_exceptions=True
        )
        for dc_id, result in zip(self.nodes, results, strict=True):
            if isinstance(result, Exception):
                log.error("Kho %s: lỗi khi cập nhật dc_status: %r", dc_id, result)

    async def check(self, dc_id: int) -> None:
        mode = await probe(self.nodes[dc_id])
        if mode is None:
            await self._on_failure(dc_id)
        else:
            await self._on_success(dc_id, mode)

    async def _on_success(self, dc_id: int, mode: NodeMode) -> None:
        current = await self.store.record_success(dc_id, mode)
        if current is None:
            log.warning("Kho %s chưa có dòng dc_status (chưa chạy make seed?)", dc_id)
            return
        has_pending = current == DcStatus.OFFLINE and await self.store.has_pending(dc_id)
        target = next_status_on_success(current, mode, has_pending)
        if target is not None and await self.store.transition(dc_id, (current,), target):
            log.info("Kho %s: %s -> %s (node_mode=%s)", dc_id, current, target, mode)

    async def _on_failure(self, dc_id: int) -> None:
        result = await self.store.record_failure(dc_id)
        if result is None:
            log.warning("Kho %s chưa có dòng dc_status (chưa chạy make seed?)", dc_id)
            return
        if should_go_offline(result.status, result.missed_count):
            if await self.store.transition(dc_id, OFFLINE_FROM, DcStatus.OFFLINE):
                log.warning(
                    "Kho %s: %s -> OFFLINE sau %d lần health check lỗi",
                    dc_id,
                    result.status,
                    result.missed_count,
                )


async def run(app: FastAPI) -> None:
    """Vòng lặp nền, main.py chạy bằng asyncio.create_task và hủy khi tắt app."""
    monitor = HealthMonitor(PgHealthStore(app.state.pool), app.state.nodes)
    await run_periodic(HEALTH_CHECK_INTERVAL_S, monitor.tick, log)
