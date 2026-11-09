"""GET /inventory, POST /inbound, /outbound, /replay – F6, F10.a (Tuấn Minh).

GET /health nằm ở main.py.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any

from fastapi import APIRouter, Request
from services import stock

from common.constants import (
    SQL_SET_APP_SOURCE,
    AggregateType,
    ErrorCode,
    EventType,
    NodeMode,
    RefType,
    TxnType,
)
from common.errors import AppError
from common.ids import inventory_aggregate_id
from common.models import (
    InboundRequest,
    InventoryItem,
    InventoryResponse,
    OutboundRequest,
    ReplayRequest,
    ReplayResponse,
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
    body: InboundRequest | OutboundRequest | ReplayRequest,
    op: Callable[..., Awaitable[stock.StockResult]],
    ref_type: RefType,
    *,
    from_wms: bool = True,
) -> StockChangeResponse:
    """F10.a: SET LOCAL, kiểm trùng, đổi tồn + sổ, outbox, COMMIT. Không gọi HTTP trong đây.

    from_wms=False cho /replay: HQ đẩy lại giao dịch ghi hộ, nhận ở cả hai chế độ.
    """
    state = request.app.state
    # bước 1: lệnh từ WMS bị từ chối khi đang đồng bộ (/replay vẫn chạy)
    if from_wms and state.mode == NodeMode.RECOVERING:
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
                        ref_type=ref_type,
                    )
                    await stock.write_outbox(  # bước 4
                        conn,
                        state.dc_id,
                        AggregateType.INVENTORY,
                        inventory_aggregate_id(state.dc_id, body.product_id),
                        EventType.INVENTORY_CHANGED,
                        {
                            "product_id": body.product_id,
                            "qty_on_hand": result.qty_on_hand,
                            "qty_reserved": result.qty_reserved,
                            "version": result.version,
                            "request_id": body.request_id,
                            "ref_type": ref_type,
                        },
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
    return await _apply(request, body, stock.add_stock, body.ref_type)


@router.post("/outbound", response_model=StockChangeResponse)
async def outbound(request: Request, body: OutboundRequest) -> StockChangeResponse:
    """F6, F10.a: xuất hàng."""
    consume = partial(stock.consume, lock_mode=request.app.state.lock_mode)
    return await _apply(request, body, consume, body.ref_type)


@router.post("/replay", response_model=ReplayResponse)
async def replay(request: Request, body: ReplayRequest) -> ReplayResponse:
    """F6, F10.a: HQ đẩy lại giao dịch ghi hộ (pending_txn); request_id = txn_id; X-Internal-Key."""
    if body.txn_type == TxnType.INBOUND:
        op = stock.add_stock
    else:
        op = partial(stock.consume, lock_mode=request.app.state.lock_mode)
    done = await _apply(request, body, op, RefType.REPLAY, from_wms=False)
    return ReplayResponse(qty_on_hand=done.qty_on_hand, duplicate=done.duplicate)
