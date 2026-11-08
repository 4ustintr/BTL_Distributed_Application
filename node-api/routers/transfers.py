"""notice, ship, receive, decline, cancel, sync-status – F6, F10.b–e (Minh Hiếu)."""

from __future__ import annotations

from fastapi import APIRouter, Request
from services import stock

from common.constants import (
    SQL_SET_APP_SOURCE,
    AggregateType,
    EventType,
    TransferDirection,
    TransferStatus,
)
from common.ids import inventory_aggregate_id
from common.models import TransferNoticeRequest, TransferStatusResponse

router = APIRouter()

# GIẢ ĐỊNH chữ ký services/stock.py (Tuấn Minh chưa viết, chờ chốt):
#   reserve(conn, dc_id, product_id, qty) -> dòng RETURNING version, qty_on_hand, qty_reserved
#     (db.md 2.7); thiếu hàng: AppError(INSUFFICIENT_STOCK);
#     hết 3 lần thử: AppError(VERSION_CONFLICT) – notice trả 409, F6 và F10.f chưa nói tới
#   write_outbox(conn, dc_id, aggregate_type, aggregate_id, event_type, payload): tự sinh event_id,
#     điền event_id, event_type, source_dc, occurred_at = clock_timestamp() vào payload (F8)
#   Cả hai chạy trên conn được truyền, trong transaction của router, không tự COMMIT.

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


class DuplicateNotice(Exception):
    """transfer_code đã có ở kho: thoát khối transaction để ROLLBACK (F10.b bước 1)."""


@router.post("/transfers/notice", response_model=TransferStatusResponse)
async def notice(body: TransferNoticeRequest, request: Request) -> TransferStatusResponse:
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

                        # GIẢ ĐỊNH: InventoryChanged khi giữ chỗ không có request_id
                        await stock.write_outbox(
                            conn,
                            dc_id,
                            AggregateType.INVENTORY,
                            inventory_aggregate_id(dc_id, item.product_id),
                            EventType.INVENTORY_CHANGED,
                            {
                                "product_id": item.product_id,
                                "qty_on_hand": row["qty_on_hand"],
                                "qty_reserved": row["qty_reserved"],
                                "version": row["version"],
                            },
                        )

                await stock.write_outbox(
                    conn,
                    dc_id,
                    AggregateType.TRANSFER,
                    str(body.transfer_id),
                    EventType.TRANSFER_CREATED,
                    {
                        "transfer_id": str(body.transfer_id),
                        "transfer_code": body.transfer_code,
                        "direction": body.direction,
                        "status": TransferStatus.CREATED,
                        "shipped_at": None,
                        "received_at": None,
                        "cancel_reason": None,
                    },
                )
        except DuplicateNotice:
            status = await conn.fetchval(SQL_ORDER_STATUS, body.transfer_code)
            return TransferStatusResponse(status=status, duplicate=True)
        return TransferStatusResponse(status=TransferStatus.CREATED)
