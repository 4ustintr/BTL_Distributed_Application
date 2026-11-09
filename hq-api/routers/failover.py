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


def signed_qty(txn_type: TxnType | str, qty: int) -> int:
    return qty if txn_type == TxnType.INBOUND else -qty


def provisional_on_hand(qty_on_hand: int, pending: Iterable[Mapping[str, Any]]) -> int:
    """Tồn tạm tính (db.md 2.4) = qty_on_hand + Σ nhập PENDING − Σ xuất PENDING."""
    return qty_on_hand + sum(signed_qty(row["txn_type"], row["qty"]) for row in pending)


async def read_provisional(
    conn: asyncpg.Connection, dc_id: int, product_id: int
) -> tuple[int, int]:
    """(tồn tạm tính, qty_reserved) từ inventory_snapshot và pending_txn PENDING của kho.

    Chưa có dòng snapshot (kho chưa từng phát sự kiện) thì coi tồn 0 (Minh Danh chốt mục 13).
    """
    snapshot = await conn.fetchrow(
        """SELECT qty_on_hand, qty_reserved FROM inventory_snapshot
           WHERE dc_id = $1 AND product_id = $2""",
        dc_id,
        product_id,
    )
    pending = await conn.fetch(
        """SELECT txn_type, qty FROM pending_txn
           WHERE dc_id = $1 AND product_id = $2 AND status = $3""",
        dc_id,
        product_id,
        PendingTxnStatus.PENDING,
    )
    on_hand = snapshot["qty_on_hand"] if snapshot else 0
    reserved = snapshot["qty_reserved"] if snapshot else 0
    return provisional_on_hand(on_hand, pending), reserved


async def duplicate_response(conn: asyncpg.Connection, txn_id: UUID) -> FailoverResponse | None:
    """F10.h bước 1: txn_id đã có thì trả trạng thái hiện tại và provisional_qty tính lại.

    Không kiểm tra dc_status, không kiểm tra lại hạn mức. Tính theo kho và SKU của dòng đã có.
    """
    row = await conn.fetchrow(
        "SELECT status, dc_id, product_id FROM pending_txn WHERE txn_id = $1", txn_id
    )
    if row is None:
        return None
    provisional, _ = await read_provisional(conn, row["dc_id"], row["product_id"])
    return FailoverResponse(
        txn_id=txn_id,
        status=PendingTxnStatus(row["status"]),
        provisional_qty=provisional,
        duplicate=True,
    )


async def record_pending(
    pool: asyncpg.Pool, dc_id: int, txn_type: TxnType, body: FailoverRequest
) -> FailoverResponse:
    """F10.h, đúng thứ tự 5 bước, trong một transaction."""
    async with pool.acquire() as conn, conn.transaction():
        # Bước 1: kiểm tra trùng txn_id (= request_id) trước mọi kiểm tra khác
        duplicate = await duplicate_response(conn, body.request_id)
        if duplicate is not None:
            return duplicate

        # Bước 2: dc_status khác OFFLINE thì 409 DC_NOT_OFFLINE
        status = await conn.fetchval("SELECT status FROM dc_status WHERE dc_id = $1", dc_id)
        if status is None:  # dc_id lạ -> 400 (Minh Danh chốt mục 13)
            raise AppError(ErrorCode.VALIDATION_ERROR, "Không có kho", {"dc_id": dc_id})
        if status != DcStatus.OFFLINE:
            raise AppError(
                ErrorCode.DC_NOT_OFFLINE,
                "HQ vẫn thấy kho còn sống, không nhận ghi hộ",
                {"dc_id": dc_id, "status": status},
            )

        # Bước 3: đọc inventory_snapshot và pending_txn PENDING của kho; không khóa dòng
        if not await conn.fetchval("SELECT 1 FROM product WHERE product_id = $1", body.product_id):
            # F5: lệnh ghi có SKU trong body không tồn tại -> 400 VALIDATION_ERROR
            raise AppError(
                ErrorCode.VALIDATION_ERROR, "SKU không tồn tại", {"product_id": body.product_id}
            )
        provisional, reserved = await read_provisional(conn, dc_id, body.product_id)

        # Bước 4: khả dụng tạm tính = tồn tạm tính − qty_reserved; xuất không đủ thì 422
        available = provisional - reserved
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

        # Bước 5: INSERT ... ON CONFLICT (txn_id) DO NOTHING, rồi COMMIT
        inserted = await conn.fetchval(
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
        if inserted is None:
            # Chốt chặn cuối: lệnh trùng chen vào giữa bước 1 và bước 5 -> trả như gửi trùng
            duplicate = await duplicate_response(conn, body.request_id)
            assert duplicate is not None
            return duplicate

    # provisional_qty = tồn tạm tính sau lệnh này (F7: 35 + 30 = 65; Minh Danh chốt mục 10)
    return FailoverResponse(
        txn_id=body.request_id,
        status=PendingTxnStatus(inserted),
        provisional_qty=provisional + signed_qty(txn_type, body.qty),
    )


@router.post("/failover/dc/{dc_id}/inbound")
async def failover_inbound(dc_id: int, body: FailoverRequest, request: Request) -> FailoverResponse:
    """F7: 200 {"txn_id", "status", "provisional_qty", "duplicate"}; 409 DC_NOT_OFFLINE."""
    return await record_pending(request.app.state.pool, dc_id, TxnType.INBOUND, body)


@router.post("/failover/dc/{dc_id}/outbound")
async def failover_outbound(
    dc_id: int, body: FailoverRequest, request: Request
) -> FailoverResponse:
    """F7: 200; 409 DC_NOT_OFFLINE; 422 nếu khả dụng tạm tính không đủ."""
    return await record_pending(request.app.state.pool, dc_id, TxnType.OUTBOUND, body)
