"""GET /inventory, POST /inbound, /outbound, /replay – F6, F10.a (Tuấn Minh).

GET /health nằm ở main.py. /replay làm ở việc riêng.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import APIRouter, Request
from services import stock

from common.constants import SQL_SET_APP_SOURCE, ErrorCode, NodeMode
from common.errors import AppError
from common.models import (
    InboundRequest,
    InventoryItem,
    InventoryResponse,
    OutboundRequest,
    StockChangeResponse,
)

router = APIRouter()


@router.get("/inventory", response_model=InventoryResponse)
async def get_inventory(request: Request, product_id: int | None = None) -> InventoryResponse:
    """F6: tồn của kho; không có product_id thì trả mọi dòng. Không bị chặn khi RECOVERING."""
    state = request.app.state
    async with state.pool.acquire() as conn:
        if product_id is not None and not await stock.product_exists(conn, product_id):
            raise AppError(
                ErrorCode.NOT_FOUND, "SKU không có trong danh mục", {"product_id": product_id}
            )
        rows = await stock.list_inventory(conn, state.dc_id, product_id)
    return InventoryResponse(
        items=[
            InventoryItem(
                product_id=r["product_id"],
                qty_on_hand=r["qty_on_hand"],
                qty_reserved=r["qty_reserved"],
                available=r["qty_on_hand"] - r["qty_reserved"],
                version=r["version"],
            )
            for r in rows
        ]
    )


async def _apply(
    request: Request,
    body: InboundRequest | OutboundRequest,
    op: Callable[..., Awaitable[stock.StockResult]],
) -> StockChangeResponse:
    """F10.a: SET LOCAL, kiểm trùng, đổi tồn + sổ, outbox, COMMIT. Không gọi HTTP trong đây."""
    state = request.app.state
    if state.mode == NodeMode.RECOVERING:  # bước 1: lệnh từ WMS bị từ chối khi đang đồng bộ
        raise AppError(ErrorCode.DC_RECOVERING, "Kho đang đồng bộ, chưa nhận giao dịch")

    async with state.pool.acquire() as conn:
        try:
            async with conn.transaction():
                await conn.execute(SQL_SET_APP_SOURCE)
                if not await stock.movement_exists(conn, body.request_id):  # bước 1
                    if not await stock.product_exists(conn, body.product_id):
                        raise AppError(
                            ErrorCode.VALIDATION_ERROR,
                            "SKU không có trong danh mục",
                            {"product_id": body.product_id},
                        )
                    result = await op(  # bước 2–3
                        conn,
                        dc_id=state.dc_id,
                        product_id=body.product_id,
                        qty=body.qty,
                        request_id=body.request_id,
                        created_by=body.created_by,
                        ref_type=body.ref_type,
                    )
                    await stock.write_outbox(  # bước 4
                        conn,
                        dc_id=state.dc_id,
                        product_id=body.product_id,
                        qty_on_hand=result.qty_on_hand,
                        qty_reserved=result.qty_reserved,
                        version=result.version,
                        request_id=body.request_id,
                        ref_type=body.ref_type,
                    )
                    return StockChangeResponse(
                        qty_on_hand=result.qty_on_hand, version=result.version
                    )
        except stock.DuplicateRequest:  # bước 3: transaction đã ROLLBACK
            pass
        # Trùng ở bước 1 hoặc bước 3: trả số hiện tại, không đổi gì
        current: Any = await stock.current_stock(conn, state.dc_id, body.product_id)
        assert current is not None  # có dòng sổ nghĩa là đã có dòng inventory (khóa ngoại)
        return StockChangeResponse(
            qty_on_hand=current.qty_on_hand, version=current.version, duplicate=True
        )


@router.post("/inbound", response_model=StockChangeResponse)
async def inbound(request: Request, body: InboundRequest) -> StockChangeResponse:
    """F6, F10.a: nhập hàng."""
    return await _apply(request, body, stock.add_stock)


@router.post("/outbound", response_model=StockChangeResponse)
async def outbound(request: Request, body: OutboundRequest) -> StockChangeResponse:
    """F6, F10.a: xuất hàng."""
    return await _apply(request, body, stock.consume)
