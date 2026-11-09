"""F7/db.md 2.4: snapshot reads and independently bounded FDW reads."""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated, Any

import asyncpg
from fastapi import APIRouter, Query, Request

from common.constants import (
    DC_IDS,
    FDW_QUERY_TIMEOUT_S,
    DashboardMode,
    DcStatus,
    ErrorCode,
    InventorySource,
    PendingTxnStatus,
    TxnType,
)
from common.errors import AppError
from common.models import (
    DashboardInventoryItem,
    DashboardInventoryResponse,
    DcStatusItem,
    PendingTxn,
)

router = APIRouter()
log = logging.getLogger("hq-api.dashboard")
ProductId = Annotated[int, Query(gt=0)]
FDW_ERRORS = (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError)


async def require_product(pool: asyncpg.Pool, product_id: int) -> None:
    if not await pool.fetchval("SELECT 1 FROM product WHERE product_id = $1", product_id):
        raise AppError(ErrorCode.NOT_FOUND, "Không có SKU", {"product_id": product_id})


async def fetch_live_inventory(pool: asyncpg.Pool, dc_id: int, product_id: int) -> asyncpg.Record:
    if dc_id not in DC_IDS:
        raise ValueError("Unknown DC schema")
    # Only the fixed whitelist above enters an SQL identifier. Values remain parameters.
    async with asyncio.timeout(FDW_QUERY_TIMEOUT_S), pool.acquire() as connection:
        return await connection.fetchrow(
            f"""SELECT COALESCE(SUM(qty_on_hand), 0)::bigint AS qty_on_hand,
                       COALESCE(SUM(qty_reserved), 0)::bigint AS qty_reserved,
                       clock_timestamp() AS as_of
                FROM dc{dc_id}.inventory WHERE product_id = $1""",
            product_id,
        )


async def _live_or_none(pool: asyncpg.Pool, dc_id: int, product_id: int) -> asyncpg.Record | None:
    try:
        return await fetch_live_inventory(pool, dc_id, product_id)
    except FDW_ERRORS as error:
        log.warning("FDW dc%s unavailable: %s", dc_id, error)
        return None


@router.get("/dashboard/inventory", response_model=DashboardInventoryResponse)
async def inventory(
    request: Request,
    product_id: ProductId = 1,
    mode: DashboardMode = DashboardMode.SNAPSHOT,
) -> DashboardInventoryResponse:
    pool = request.app.state.pool
    await require_product(pool, product_id)
    # One local statement gives a consistent snapshot + pending view, avoiding double counts
    # while the Consumer atomically updates inventory_snapshot and pending_txn.
    rows = await pool.fetch(
        """SELECT d.dc_id, COALESCE(s.qty_on_hand, 0) AS qty_on_hand,
                  COALESCE(s.qty_reserved, 0) AS qty_reserved, s.synced_at,
                  COALESCE(st.status, $2) AS status,
                  COALESCE(t.qty_in_transit, 0) AS qty_in_transit,
                  COALESCE(p.pending_count, 0) AS pending_count,
                  COALESCE(p.delta, 0) AS delta
           FROM dc d
           LEFT JOIN inventory_snapshot s ON s.dc_id = d.dc_id AND s.product_id = $1
           LEFT JOIN dc_status st ON st.dc_id = d.dc_id
           LEFT JOIN v_in_transit t ON t.dc_id = d.dc_id AND t.product_id = $1
           LEFT JOIN (
               SELECT dc_id, COUNT(*) AS pending_count,
                      SUM(CASE WHEN txn_type = $4 THEN qty ELSE -qty END) AS delta
               FROM pending_txn WHERE product_id = $1 AND status = $3 GROUP BY dc_id
           ) p ON p.dc_id = d.dc_id
           ORDER BY d.dc_id""",
        product_id,
        DcStatus.RECOVERING,
        PendingTxnStatus.PENDING,
        TxnType.INBOUND,
    )
    items: list[DashboardInventoryItem] = []
    candidates: list[int] = []
    for row in rows:
        offline_realtime = mode == DashboardMode.REALTIME and row["status"] == DcStatus.OFFLINE
        provisional = (
            not offline_realtime
            and row["status"] in (DcStatus.OFFLINE, DcStatus.RECOVERING)
            and bool(row["pending_count"])
        )
        source = InventorySource.SNAPSHOT
        if row["status"] == DcStatus.OFFLINE:
            source = InventorySource.OFFLINE
        if provisional:
            source = InventorySource.PROVISIONAL
        items.append(
            DashboardInventoryItem(
                dc_id=row["dc_id"],
                qty_on_hand=row["qty_on_hand"] + (row["delta"] if provisional else 0),
                qty_reserved=row["qty_reserved"],
                qty_in_transit=row["qty_in_transit"],
                source=source,
                as_of=row["synced_at"],
            )
        )
        if mode == DashboardMode.REALTIME and not provisional and source != InventorySource.OFFLINE:
            candidates.append(len(items) - 1)
    results = await asyncio.gather(
        *(_live_or_none(pool, items[index].dc_id, product_id) for index in candidates)
    )
    for index, live in zip(candidates, results, strict=True):
        if live is None:
            items[index].source = InventorySource.OFFLINE
        else:
            items[index].qty_on_hand = live["qty_on_hand"]
            items[index].qty_reserved = live["qty_reserved"]
            items[index].as_of = live["as_of"]
            items[index].source = InventorySource.REALTIME
    return DashboardInventoryResponse(items=items)


@router.get("/dc-status", response_model=list[DcStatusItem])
async def dc_status(request: Request) -> list[dict[str, Any]]:
    rows = await request.app.state.pool.fetch(
        "SELECT dc_id, status, updated_at FROM dc_status ORDER BY dc_id"
    )
    return [dict(row) for row in rows]


@router.get("/pending-txn", response_model=list[PendingTxn])
async def pending_transactions(
    request: Request, dc_id: Annotated[int | None, Query()] = None
) -> list[dict[str, Any]]:
    if dc_id is not None and dc_id not in DC_IDS:
        raise AppError(ErrorCode.VALIDATION_ERROR, "Mã kho không hợp lệ", {"dc_id": dc_id})
    rows = await request.app.state.pool.fetch(
        """SELECT * FROM pending_txn WHERE ($1::smallint IS NULL OR dc_id = $1)
           ORDER BY created_at, txn_id""",
        dc_id,
    )
    return [dict(row) for row in rows]
