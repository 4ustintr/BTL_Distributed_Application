"""Replay Worker: mỗi 2 giây đẩy pending_txn về kho đang RECOVERING – F9, F10.i (Nhật Khánh).

Không gọi HTTP khi đang giữ transaction: đọc danh sách, trả kết nối, gọi /replay, rồi mới UPDATE.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Protocol
from uuid import UUID

import asyncpg
import httpx
from fastapi import FastAPI
from pydantic import ValidationError

from common.constants import (
    REPLAY_INTERVAL_S,
    REPLAY_ROUND_MAX_S,
    DcStatus,
    ErrorCode,
    PendingTxnStatus,
    TxnType,
)
from common.http import InternalClient
from common.models import ReplayRequest, ReplayResponse
from workers.loop import run_periodic

log = logging.getLogger("hq-api.replay")

NODE_REPLAY_PATH = "/replay"  # F6
NODE_RECOVERY_COMPLETE_PATH = "/recovery/complete"  # F6
SMALLINT_MAX = 32767  # db.md 2.12: attempts SMALLINT – dòng chờ CDC được gửi lại mỗi vòng
ERROR_MSG_MAX = 500  # error_msg là TEXT; cắt bớt khi kho trả thân lỗi không đúng dạng F5

# GIẢ ĐỊNH: không thử lại trong một vòng. F10.i: lỗi mạng thì dừng vòng, vòng sau (2 giây) gửi
# lại; thử lại kiểu F5 (5 lần × 2 giây) sẽ giữ một vòng tới 10 giây cho mỗi dòng.
REPLAY_MAX_RETRIES = 0


class Outcome(Enum):
    """Kết quả gửi một dòng tới /replay (F10.i bước 2)."""

    SENT = "sent"  # 200, duplicate=false: sang dòng kế, chờ Consumer chuyển APPLIED
    APPLIED = "applied"  # 200, duplicate=true: chuyển thẳng APPLIED
    REJECTED = "rejected"  # 422 hoặc 400 VALIDATION_ERROR: chuyển REJECTED, ghi error_msg
    STOP = "stop"  # lỗi mạng, 5xx: dừng vòng của kho này


@dataclass(frozen=True)
class PendingRow:
    txn_id: UUID
    txn_type: TxnType
    product_id: int
    qty: int
    created_by: str | None


def error_message(resp: httpx.Response) -> str:
    """error_msg từ thân lỗi F5: "INSUFFICIENT_STOCK: <message>" (TC16 tìm error_code)."""
    try:
        body = resp.json()
        return f"{body['error_code']}: {body.get('message', '')}"[:ERROR_MSG_MAX]
    except (ValueError, KeyError, TypeError):
        return f"HTTP {resp.status_code}: {resp.text}"[:ERROR_MSG_MAX]


def error_code_of(resp: httpx.Response) -> str | None:
    """error_code trong thân lỗi F5; None nếu thân không đúng dạng."""
    try:
        code = resp.json()["error_code"]
    except (ValueError, KeyError, TypeError):
        return None
    return code if isinstance(code, str) else None


def classify(resp: httpx.Response) -> Outcome:
    """F10.i bước 2: phân loại phản hồi của kho."""
    if resp.status_code == 200:
        try:
            replay = ReplayResponse.model_validate(resp.json())
        except (ValueError, ValidationError):
            return Outcome.STOP  # không chắc kho đã áp dụng hay chưa: giữ PENDING, gửi lại sau
        return Outcome.APPLIED if replay.duplicate else Outcome.SENT
    if resp.status_code == 422:
        return Outcome.REJECTED
    # F10.i: 400 VALIDATION_ERROR là dữ liệu lệnh sai (vd. SKU không có ở kho), gửi lại vẫn sai
    if resp.status_code == 400 and error_code_of(resp) == ErrorCode.VALIDATION_ERROR:
        return Outcome.REJECTED
    # Lỗi mạng, 5xx và các 4xx khác (401, 409 VERSION_CONFLICT…) có thể tự hết hoặc do cấu hình:
    # dừng vòng, giữ PENDING, vòng sau gửi lại (F10.i)
    return Outcome.STOP


# ---------------------------------------------------------------------------
# Truy cập pending_txn, dc_status
# ---------------------------------------------------------------------------
class ReplayStore(Protocol):
    async def recovering_dcs(self) -> list[int]: ...
    async def pending(self, dc_id: int) -> list[PendingRow]: ...
    async def mark_attempt(self, txn_id: UUID) -> None: ...
    async def mark_applied(self, txn_id: UUID) -> bool: ...
    async def mark_rejected(self, txn_id: UUID, error_msg: str) -> bool: ...
    async def has_pending(self, dc_id: int) -> bool: ...
    async def set_online(self, dc_id: int) -> bool: ...


class PgReplayStore:
    """pending_txn và dc_status ở hq_db (db.md 2.12, 2.14)."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def recovering_dcs(self) -> list[int]:
        rows = await self.pool.fetch(
            "SELECT dc_id FROM dc_status WHERE status = $1 ORDER BY dc_id", DcStatus.RECOVERING
        )
        return [r["dc_id"] for r in rows]

    async def pending(self, dc_id: int) -> list[PendingRow]:
        """F10.i bước 1: các dòng PENDING theo thứ tự created_at."""
        rows = await self.pool.fetch(
            """SELECT txn_id, txn_type, product_id, qty, created_by FROM pending_txn
               WHERE dc_id = $1 AND status = $2
               ORDER BY created_at, txn_id""",
            dc_id,
            PendingTxnStatus.PENDING,
        )
        return [
            PendingRow(
                r["txn_id"], TxnType(r["txn_type"]), r["product_id"], r["qty"], r["created_by"]
            )
            for r in rows
        ]

    async def mark_attempt(self, txn_id: UUID) -> None:
        """attempts: số lần đã gửi tới kho, chỉ để theo dõi (db.md 2.12)."""
        await self.pool.execute(
            "UPDATE pending_txn SET attempts = LEAST(attempts + 1, $2) WHERE txn_id = $1",
            txn_id,
            SMALLINT_MAX,
        )

    async def mark_applied(self, txn_id: UUID) -> bool:
        """F9: PENDING -> APPLIED khi kho trả duplicate: true."""
        result = await self.pool.execute(
            """UPDATE pending_txn SET status = $2, applied_at = now(), updated_at = now()
               WHERE txn_id = $1 AND status = $3""",
            txn_id,
            PendingTxnStatus.APPLIED,
            PendingTxnStatus.PENDING,
        )
        return result == "UPDATE 1"

    async def mark_rejected(self, txn_id: UUID, error_msg: str) -> bool:
        """F9: PENDING -> REJECTED khi kho trả 422."""
        result = await self.pool.execute(
            """UPDATE pending_txn SET status = $2, error_msg = $3, updated_at = now()
               WHERE txn_id = $1 AND status = $4""",
            txn_id,
            PendingTxnStatus.REJECTED,
            error_msg,
            PendingTxnStatus.PENDING,
        )
        return result == "UPDATE 1"

    async def has_pending(self, dc_id: int) -> bool:
        return bool(
            await self.pool.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pending_txn WHERE dc_id = $1 AND status = $2)",
                dc_id,
                PendingTxnStatus.PENDING,
            )
        )

    async def set_online(self, dc_id: int) -> bool:
        """F9: RECOVERING -> ONLINE. Kiểm tra lại "không còn PENDING" ngay trong lệnh UPDATE."""
        result = await self.pool.execute(
            """UPDATE dc_status SET status = $2, updated_at = now()
               WHERE dc_id = $1 AND status = $3
                 AND NOT EXISTS (SELECT 1 FROM pending_txn WHERE dc_id = $1 AND status = $4)""",
            dc_id,
            DcStatus.ONLINE,
            DcStatus.RECOVERING,
            PendingTxnStatus.PENDING,
        )
        return result == "UPDATE 1"


# ---------------------------------------------------------------------------
# Replay Worker
# ---------------------------------------------------------------------------
class ReplayWorker:
    def __init__(self, store: ReplayStore, nodes: Mapping[int, InternalClient]) -> None:
        self.store = store
        self.nodes = nodes

    async def tick(self) -> None:
        """Một vòng, tối đa REPLAY_ROUND_MAX_S giây (F11). Các kho chạy song song."""
        try:
            async with asyncio.timeout(REPLAY_ROUND_MAX_S):
                dcs = [dc for dc in await self.store.recovering_dcs() if dc in self.nodes]
                results = await asyncio.gather(
                    *(self.replay_dc(dc) for dc in dcs), return_exceptions=True
                )
        except TimeoutError:
            log.warning("Vòng replay vượt %.0f giây, dừng; vòng sau gửi tiếp", REPLAY_ROUND_MAX_S)
            return
        for dc_id, result in zip(dcs, results, strict=True):
            if isinstance(result, Exception):
                log.error("Kho %s: lỗi trong vòng replay: %r", dc_id, result)

    async def replay_dc(self, dc_id: int) -> None:
        client = self.nodes[dc_id]
        # Bước 1–2: gửi lần lượt; dòng chưa APPLIED được gửi lại ở vòng sau (kho chặn trùng)
        for row in await self.store.pending(dc_id):
            outcome = await self._send(dc_id, client, row)
            if outcome is Outcome.STOP:
                return  # giữ đúng thứ tự: không nhảy sang dòng sau
        # Bước 3: hết PENDING thì gọi /recovery/complete; 200 thì chuyển ONLINE
        if await self.store.has_pending(dc_id):
            return
        try:
            resp = await client.post(NODE_RECOVERY_COMPLETE_PATH, max_retries=REPLAY_MAX_RETRIES)
        except httpx.HTTPError as exc:
            log.warning("Kho %s: gọi /recovery/complete lỗi mạng: %r", dc_id, exc)
            return
        if resp.status_code != 200:
            log.warning("Kho %s: /recovery/complete trả %s", dc_id, resp.status_code)
            return
        if await self.store.set_online(dc_id):
            log.info("Kho %s: RECOVERING -> ONLINE", dc_id)

    async def _send(self, dc_id: int, client: InternalClient, row: PendingRow) -> Outcome:
        body = ReplayRequest(
            request_id=row.txn_id,  # db.md 2.12: txn_id = request_id do WMS sinh
            txn_type=row.txn_type,
            product_id=row.product_id,
            qty=row.qty,
            created_by=row.created_by,
        ).model_dump(mode="json")
        await self.store.mark_attempt(row.txn_id)
        try:
            resp = await client.post(NODE_REPLAY_PATH, json=body, max_retries=REPLAY_MAX_RETRIES)
        except httpx.HTTPError as exc:
            log.warning("Kho %s: replay %s lỗi mạng (%r), dừng vòng", dc_id, row.txn_id, exc)
            return Outcome.STOP
        outcome = classify(resp)
        if outcome is Outcome.APPLIED:
            await self.store.mark_applied(row.txn_id)
        elif outcome is Outcome.REJECTED:
            msg = error_message(resp)
            await self.store.mark_rejected(row.txn_id, msg)
            log.warning("Kho %s: replay %s bị từ chối: %s", dc_id, row.txn_id, msg)
        elif 400 <= resp.status_code < 500:
            # 4xx còn lại (401, 404, 409…): giữ PENDING và dừng vòng; log ERROR để người xem (F10.i)
            log.error(
                "Kho %s: replay txn_id=%s trả %s error_code=%s, giữ PENDING, dừng vòng",
                dc_id,
                row.txn_id,
                resp.status_code,
                error_code_of(resp),
            )
        elif outcome is Outcome.STOP:
            log.warning("Kho %s: replay %s trả %s, dừng vòng", dc_id, row.txn_id, resp.status_code)
        return outcome


async def run(app: FastAPI) -> None:
    """Vòng lặp nền, main.py chạy bằng asyncio.create_task và hủy khi tắt app."""
    worker = ReplayWorker(PgReplayStore(app.state.pool), app.state.nodes)
    await run_periodic(REPLAY_INTERVAL_S, worker.tick, log)
