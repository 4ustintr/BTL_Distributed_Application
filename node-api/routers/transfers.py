"""notice, ship, receive, decline, cancel, sync-status – F6, F10.b–e (Minh Hiếu)."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Request
from services import stock

from common.constants import (
    SQL_SET_APP_SOURCE,
    TRANSFER_PREV_STATUS,
    AggregateType,
    CancelReason,
    ErrorCode,
    EventType,
    TransferDirection,
    TransferStatus,
)
from common.errors import AppError
from common.ids import inventory_aggregate_id
from common.models import (
    CancelRequest,
    SyncStatusRequest,
    TransferNoticeRequest,
    TransferStatusResponse,
)

router = APIRouter()

# GIẢ ĐỊNH chữ ký services/stock.py (Tuấn Minh chưa viết, chờ chốt):
#   reserve(conn, dc_id, product_id, qty) -> dòng RETURNING version, qty_on_hand, qty_reserved
#     (db.md 2.7); thiếu hàng: AppError(INSUFFICIENT_STOCK);
#     hết 3 lần thử: AppError(VERSION_CONFLICT) – notice trả 409, F6 và F10.f chưa nói tới
#   release(conn, dc_id, product_id, qty) -> như reserve, giảm qty_reserved (F10.e bước 2)
#   write_outbox(conn, dc_id, aggregate_type, aggregate_id, event_type, payload): tự sinh event_id,
#     điền event_id, event_type, source_dc, occurred_at = clock_timestamp() vào payload (F8)
#   Cả ba chạy trên conn được truyền, trong transaction của router, không tự COMMIT.

SQL_INSERT_ORDER = """
INSERT INTO transfer_order (transfer_id, transfer_code, from_dc_id, to_dc_id, direction)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (transfer_code) DO NOTHING
RETURNING transfer_id
"""

SQL_INSERT_ITEM = """
INSERT INTO transfer_item (transfer_id, line_no, product_id, qty_requested)
VALUES ($1, $2, $3, $4)
"""

SQL_ORDER_STATUS = "SELECT status FROM transfer_order WHERE transfer_code = $1"

# F10.e bước 1 – chỉ bản OUTBOUND còn CREATED mới hủy được (db.md 3.2 quy tắc 2, 4)
SQL_CANCEL_ORDER = """
UPDATE transfer_order SET status = $1, cancel_reason = $2
WHERE transfer_id = $3 AND direction = $4 AND status = $5
RETURNING transfer_code
"""

SQL_OUTBOUND_STATUS = "SELECT status FROM transfer_order WHERE transfer_id = $1 AND direction = $2"

SQL_ORDER_ITEMS = "SELECT product_id, qty_requested FROM transfer_item WHERE transfer_id = $1"

# /sync-status – chỉ đi từ trạng thái liền trước, đúng bản phiếu ở kho này (db.md 3.2 quy tắc 2)
SQL_SYNC_ORDER = """
UPDATE transfer_order SET status = $1
WHERE transfer_id = $2 AND direction = $3 AND status = $4
RETURNING transfer_code, cancel_reason, shipped_at, received_at
"""

# GIẢ ĐỊNH: /sync-status chỉ nhận 3 bước không kèm thay đổi tồn kho; cặp khác bỏ qua, vẫn 200 (F6).
# Đổi bản OUTBOUND sang SHIPPED/CANCELLED hay bản INBOUND sang RECEIVED phải qua ship, cancel,
# receive để trừ kho, nhả giữ chỗ, tăng tồn (F10.c–e).
SYNC_DIRECTION = {
    TransferStatus.SHIPPED: TransferDirection.INBOUND,  # kho nguồn đã xuất (F10.d bước 1)
    TransferStatus.RECEIVED: TransferDirection.OUTBOUND,  # kho đích đã nhận (hld.md, TC05)
    TransferStatus.CANCELLED: TransferDirection.INBOUND,  # kho nguồn đã hủy (F10.g bước 2)
}

TRANSFER_EVENT = {
    TransferStatus.CREATED: EventType.TRANSFER_CREATED,
    TransferStatus.SHIPPED: EventType.TRANSFER_SHIPPED,
    TransferStatus.RECEIVED: EventType.TRANSFER_RECEIVED,
    TransferStatus.CANCELLED: EventType.TRANSFER_CANCELLED,
}


class DuplicateNotice(Exception):
    """transfer_code đã có ở kho: thoát khối transaction để ROLLBACK (F10.b bước 1)."""


def iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


async def write_inventory_event(
    conn: asyncpg.Connection, dc_id: int, product_id: int, row: Mapping[str, Any]
) -> None:
    """InventoryChanged của một SKU; số liệu lấy từ RETURNING của stock (F8, db.md 2.11)."""
    # GIẢ ĐỊNH: InventoryChanged khi giữ chỗ hoặc nhả giữ chỗ không có request_id (db.md 3.4)
    await stock.write_outbox(
        conn,
        dc_id,
        AggregateType.INVENTORY,
        inventory_aggregate_id(dc_id, product_id),
        EventType.INVENTORY_CHANGED,
        {
            "product_id": product_id,
            "qty_on_hand": row["qty_on_hand"],
            "qty_reserved": row["qty_reserved"],
            "version": row["version"],
        },
    )


async def write_transfer_event(
    conn: asyncpg.Connection,
    dc_id: int,
    transfer_id: UUID,
    transfer_code: str,
    direction: TransferDirection,
    status: TransferStatus,
    *,
    cancel_reason: CancelReason | str | None = None,
    shipped_at: datetime | None = None,
    received_at: datetime | None = None,
) -> None:
    """Sự kiện phiếu Transfer* ứng với trạng thái mới của phiếu (F8, db.md 2.11)."""
    await stock.write_outbox(
        conn,
        dc_id,
        AggregateType.TRANSFER,
        str(transfer_id),
        TRANSFER_EVENT[status],
        {
            "transfer_id": str(transfer_id),
            "transfer_code": transfer_code,
            "direction": direction,
            "status": status,
            "shipped_at": iso(shipped_at),
            "received_at": iso(received_at),
            "cancel_reason": cancel_reason,
        },
    )


@router.post("/transfers/notice", response_model=TransferStatusResponse)
async def notice(body: TransferNoticeRequest, request: Request) -> TransferStatusResponse:
    """F6, F10.b: kho nguồn ghi phiếu OUTBOUND và giữ chỗ; kho đích ghi phiếu INBOUND."""
    dc_id: int = request.app.state.dc_id
    async with request.app.state.pool.acquire() as conn:
        try:
            async with conn.transaction():
                await conn.execute(SQL_SET_APP_SOURCE)

                inserted = await conn.fetchval(
                    SQL_INSERT_ORDER,
                    body.transfer_id,
                    body.transfer_code,
                    body.from_dc_id,
                    body.to_dc_id,
                    body.direction,
                )
                if inserted is None:
                    raise DuplicateNotice

                await conn.executemany(
                    SQL_INSERT_ITEM,
                    [(body.transfer_id, i.line_no, i.product_id, i.qty) for i in body.items],
                )

                if body.direction == TransferDirection.OUTBOUND:
                    for item in sorted(body.items, key=lambda i: i.product_id):
                        row = await stock.reserve(conn, dc_id, item.product_id, item.qty)
                        await write_inventory_event(conn, dc_id, item.product_id, row)

                await write_transfer_event(
                    conn,
                    dc_id,
                    body.transfer_id,
                    body.transfer_code,
                    body.direction,
                    TransferStatus.CREATED,
                )
        except DuplicateNotice:
            status = await conn.fetchval(SQL_ORDER_STATUS, body.transfer_code)
            return TransferStatusResponse(status=status, duplicate=True)
        return TransferStatusResponse(status=TransferStatus.CREATED)


@router.post("/transfers/{transfer_id}/cancel", response_model=TransferStatusResponse)
async def cancel(
    transfer_id: UUID, body: CancelRequest, request: Request
) -> TransferStatusResponse:
    """F6, F10.e: kho nguồn hủy phiếu còn CREATED và nhả giữ chỗ (giao dịch bù của Saga)."""
    dc_id: int = request.app.state.dc_id
    cancel_args = (
        TransferStatus.CANCELLED,
        body.reason,
        transfer_id,
        TransferDirection.OUTBOUND,
        TransferStatus.CREATED,
    )
    async with request.app.state.pool.acquire() as conn, conn.transaction():
        await conn.execute(SQL_SET_APP_SOURCE)

        # Bước 1: không cập nhật được thì xem bản OUTBOUND đang ở trạng thái nào
        code = await conn.fetchval(SQL_CANCEL_ORDER, *cancel_args)
        while code is None:
            status = await conn.fetchval(
                SQL_OUTBOUND_STATUS, transfer_id, TransferDirection.OUTBOUND
            )
            if status in (TransferStatus.SHIPPED, TransferStatus.RECEIVED):
                raise AppError(
                    ErrorCode.INVALID_STATE,
                    "Phiếu đã xuất, kho nguồn không hủy được",
                    {"status": status},
                )
            if status != TransferStatus.CREATED:
                # Đã hủy là gửi trùng (F5). GIẢ ĐỊNH: chưa có phiếu thì duplicate = false
                duplicate = status == TransferStatus.CANCELLED
                return TransferStatusResponse(status=TransferStatus.CANCELLED, duplicate=duplicate)
            # Notice vừa commit sau lệnh UPDATE: hủy lại, không trả CANCELLED khi phiếu còn
            # CREATED. Vòng lặp dừng vì trạng thái chỉ đi tới (db.md 3.2 quy tắc 2).
            code = await conn.fetchval(SQL_CANCEL_ORDER, *cancel_args)

        # Bước 2: nhả giữ chỗ từng dòng, theo product_id tăng dần như lúc giữ (notice)
        items = sorted(
            await conn.fetch(SQL_ORDER_ITEMS, transfer_id), key=lambda i: i["product_id"]
        )
        rows = [
            await stock.release(conn, dc_id, i["product_id"], i["qty_requested"]) for i in items
        ]

        # Bước 3
        for item, row in zip(items, rows, strict=True):
            await write_inventory_event(conn, dc_id, item["product_id"], row)
        await write_transfer_event(
            conn,
            dc_id,
            transfer_id,
            code,
            TransferDirection.OUTBOUND,
            TransferStatus.CANCELLED,
            cancel_reason=body.reason,
        )
    return TransferStatusResponse(status=TransferStatus.CANCELLED)


@router.post("/transfers/{transfer_id}/sync-status")
async def sync_status(transfer_id: UUID, body: SyncStatusRequest, request: Request) -> None:
    """F6: HQ đồng bộ trạng thái phiếu xuống kho; không tiến được thì bỏ qua, vẫn trả 200."""
    direction = SYNC_DIRECTION.get(body.status)
    if direction is None:  # CREATED: không có trạng thái liền trước để đồng bộ
        return
    dc_id: int = request.app.state.dc_id
    async with request.app.state.pool.acquire() as conn, conn.transaction():
        await conn.execute(SQL_SET_APP_SOURCE)
        row = await conn.fetchrow(
            SQL_SYNC_ORDER,
            body.status,
            transfer_id,
            direction,
            TRANSFER_PREV_STATUS[body.status],
        )
        if row is None:  # đã đồng bộ, đến sai thứ tự, hoặc kho chưa có phiếu
            return
        await write_transfer_event(
            conn,
            dc_id,
            transfer_id,
            row["transfer_code"],
            direction,
            body.status,
            cancel_reason=row["cancel_reason"],
            shipped_at=row["shipped_at"],
            received_at=row["received_at"],
        )