"""POST /failover/dc/{dc_id}/inbound, /outbound – F7, F10.h (Nhật Khánh).

WMS gọi khi không gọi được Node API của kho (F9, chế độ FAILOVER). Không cần X-Internal-Key.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Request

from common.constants import DcStatus, ErrorCode, PendingTxnStatus, TxnType
from common.errors import AppError
from common.models import FailoverRequest, FailoverResponse

router = APIRouter()


def provisional_on_hand(
    qty_on_hand: int, pending: Iterable[Mapping[str, Any]], exclude_txn_id: UUID | None = None
) -> int:
    """Tồn tạm tính (db.md 2.4) = qty_on_hand + Σ nhập PENDING − Σ xuất PENDING.

    exclude_txn_id: bỏ dòng của chính request_id đang gửi lại, để lần gửi trùng không bị
    trừ hai lần rồi nhận 422 oan (F5: gửi lại phải an toàn).
    """
    total = qty_on_hand
    for row in pending:
        if exclude_txn_id is not None and row["txn_id"] == exclude_txn_id:
            continue
        total += row["qty"] if row["txn_type"] == TxnType.INBOUND else -row["qty"]
    return total


def signed_qty(txn_type: TxnType, qty: int) -> int:
    return qty if txn_type == TxnType.INBOUND else -qty


async def record_pending(
    pool: asyncpg.Pool, dc_id: int, txn_type: TxnType, body: FailoverRequest
) -> FailoverResponse:
    """F10.h, đúng thứ tự 4 bước, trong một transaction."""
    async with pool.acquire() as conn, conn.transaction():
        # Bước 1: dc_status khác OFFLINE thì 409 DC_NOT_OFFLINE
        status = await conn.fetchval("SELECT status FROM dc_status WHERE dc_id = $1", dc_id)
        if status is None:
            # GIẢ ĐỊNH: dc_id không có trong dc_status -> 400 VALIDATION_ERROR
            raise AppError(ErrorCode.VALIDATION_ERROR, "Không có kho", {"dc_id": dc_id})
        if status != DcStatus.OFFLINE:
            raise AppError(
                ErrorCode.DC_NOT_OFFLINE,
                "HQ vẫn thấy kho còn sống, không nhận ghi hộ",
                {"dc_id": dc_id, "status": status},
            )

        # Bước 2: đọc inventory_snapshot và pending_txn PENDING của kho; không khóa dòng
        if not await conn.fetchval("SELECT 1 FROM product WHERE product_id = $1", body.product_id):
            # F5: 400 VALIDATION_ERROR khi SKU không tồn tại trong danh mục
            raise AppError(
                ErrorCode.VALIDATION_ERROR, "SKU không tồn tại", {"product_id": body.product_id}
            )
        snapshot = await conn.fetchrow(
            """SELECT qty_on_hand, qty_reserved FROM inventory_snapshot
               WHERE dc_id = $1 AND product_id = $2""",
            dc_id,
            body.product_id,
        )
        pending = await conn.fetch(
            """SELECT txn_id, txn_type, qty FROM pending_txn
               WHERE dc_id = $1 AND product_id = $2 AND status = $3""",
            dc_id,
            body.product_id,
            PendingTxnStatus.PENDING,
        )

        # Bước 3: khả dụng tạm tính = tồn tạm tính − qty_reserved; xuất không đủ thì 422
        # GIẢ ĐỊNH: chưa có dòng snapshot (kho chưa từng phát sự kiện) thì coi tồn 0.
        on_hand = snapshot["qty_on_hand"] if snapshot else 0
        reserved = snapshot["qty_reserved"] if snapshot else 0
        base = provisional_on_hand(on_hand, pending, exclude_txn_id=body.request_id)
        available = base - reserved
        if txn_type == TxnType.OUTBOUND and available < body.qty:
            raise AppError(  # thoát khỏi conn.transaction() -> ROLLBACK
                ErrorCode.INSUFFICIENT_STOCK,
                "Khả dụng tạm tính không đủ",
                {
                    "dc_id": dc_id,
                    "product_id": body.product_id,
                    "available": available,
                    "qty": body.qty,
                },
            )

        # Bước 4: INSERT ... ON CONFLICT (txn_id) DO NOTHING, rồi COMMIT
        txn_status = await conn.fetchval(
            """INSERT INTO pending_txn (txn_id, dc_id, txn_type, product_id, qty, created_by)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (txn_id) DO NOTHING
               RETURNING status""",
            body.request_id,
            dc_id,
            txn_type,
            body.product_id,
            body.qty,
            body.created_by,
        )
        if txn_status is None:  # gửi trùng request_id: trả trạng thái hiện tại (F5)
            txn_status = await conn.fetchval(
                "SELECT status FROM pending_txn WHERE txn_id = $1", body.request_id
            )

    txn_status = PendingTxnStatus(txn_status)
    # GIẢ ĐỊNH: provisional_qty = tồn tạm tính sau lệnh này (F7: 35 + 30 = 65), không phải
    # khả dụng. Dòng đã APPLIED/REJECTED không còn được cộng.
    pending_delta = signed_qty(txn_type, body.qty) if txn_status == PendingTxnStatus.PENDING else 0
    return FailoverResponse(
        txn_id=body.request_id, status=txn_status, provisional_qty=base + pending_delta
    )


@router.post("/failover/dc/{dc_id}/inbound")
async def failover_inbound(dc_id: int, body: FailoverRequest, request: Request) -> FailoverResponse:
    """F7: 200 {"txn_id", "status": "PENDING", "provisional_qty"}; 409 DC_NOT_OFFLINE."""
    return await record_pending(request.app.state.pool, dc_id, TxnType.INBOUND, body)


@router.post("/failover/dc/{dc_id}/outbound")
async def failover_outbound(
    dc_id: int, body: FailoverRequest, request: Request
) -> FailoverResponse:
    """F7: 200; 409 DC_NOT_OFFLINE; 422 nếu khả dụng tạm tính không đủ."""
    return await record_pending(request.app.state.pool, dc_id, TxnType.OUTBOUND, body)
