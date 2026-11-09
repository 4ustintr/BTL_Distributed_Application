"""F7/demo.md: verify conservation only when all DCs can be read, at rest."""

from __future__ import annotations

import asyncio

import asyncpg
from fastapi import APIRouter, Request

from common.constants import DC_IDS, FDW_QUERY_TIMEOUT_S, ErrorCode, MovementType, RefType
from common.errors import AppError
from common.models import InvariantResponse

from .dashboard import FDW_ERRORS, ProductId, require_product

router = APIRouter()


async def _totals(pool: asyncpg.Pool, dc_id: int, product_id: int) -> asyncpg.Record:
    if dc_id not in DC_IDS:
        raise ValueError("Unknown DC schema")
    async with asyncio.timeout(FDW_QUERY_TIMEOUT_S), pool.acquire() as connection:
        return await connection.fetchrow(
            f"""SELECT
                 (SELECT COALESCE(SUM(qty_on_hand), 0) FROM dc{dc_id}.inventory
                  WHERE product_id = $1) AS stock,
                 COALESCE(-SUM(qty_change) FILTER
                   (WHERE movement_type = $2 AND ref_type IN ($3, $4)), 0) AS shipped_out,
                 COALESCE(SUM(qty_change) FILTER (WHERE movement_type = $5), 0) AS received_in
                FROM dc{dc_id}.stock_movement WHERE product_id = $1""",
            product_id,
            MovementType.OUTBOUND,
            RefType.ISSUE,
            RefType.REPLAY,
            MovementType.INBOUND,
        )


@router.get("/invariant", response_model=InvariantResponse)
async def invariant(request: Request, product_id: ProductId = 1) -> InvariantResponse:
    pool = request.app.state.pool
    await require_product(pool, product_id)
    # Wait for all tasks so no borrowed connections leak when one DC fails.
    results = await asyncio.gather(
        *(_totals(pool, dc_id, product_id) for dc_id in DC_IDS),
        return_exceptions=True,
    )
    failed = []
    for dc_id, result in zip(DC_IDS, results, strict=True):
        if isinstance(result, FDW_ERRORS):
            failed.append(dc_id)
        elif isinstance(result, BaseException):
            raise result
    if failed:
        raise AppError(
            ErrorCode.DC_UNREACHABLE,
            "Không thể kiểm tra bất biến khi kho mất kết nối",
            {"dc_ids": failed},
        )
    stock = sum(row["stock"] for row in results)
    shipped_out = sum(row["shipped_out"] for row in results)
    received_in = sum(row["received_in"] for row in results)
    in_transit = await pool.fetchval(
        "SELECT COALESCE(SUM(qty_in_transit), 0) FROM v_in_transit WHERE product_id = $1",
        product_id,
    )
    return InvariantResponse(
        stock=stock,
        in_transit=in_transit,
        shipped_out=shipped_out,
        received_in=received_in,
        diff=stock + in_transit + shipped_out - received_in,
    )
