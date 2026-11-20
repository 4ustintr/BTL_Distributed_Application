"""Tự phục hồi chặng SHIPPED từ trạng thái đã commit ở HQ và các kho."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping

import asyncpg
from fastapi import FastAPI
from routers.transfers import PgTransferStore, record_shipment, sync_status

from common.constants import (
    DC_IDS,
    FDW_QUERY_TIMEOUT_S,
    HTTP_RETRY_INTERVAL_S,
    TransferDirection,
    TransferStatus,
)
from common.http import InternalClient
from workers.loop import run_periodic

log = logging.getLogger("hq-api.shipments")


class ShipmentRecovery:
    def __init__(self, pool: asyncpg.Pool, nodes: Mapping[int, InternalClient]) -> None:
        self.pool = pool
        self.store = PgTransferStore(pool)
        self.nodes = nodes

    async def tick(self) -> None:
        rows = await self.pool.fetch(
            """SELECT transfer_id, status, from_dc_id, to_dc_id FROM transfer_summary
               WHERE status IN ($1, $2)""",
            TransferStatus.CREATED,
            TransferStatus.SHIPPED,
        )
        by_dc: dict[int, list[asyncpg.Record]] = {dc_id: [] for dc_id in DC_IDS}
        for row in rows:
            dc_id = (
                row["from_dc_id"] if row["status"] == TransferStatus.CREATED else row["to_dc_id"]
            )
            by_dc[dc_id].append(row)
        active = [(dc_id, candidates) for dc_id, candidates in by_dc.items() if candidates]
        results = await asyncio.gather(
            *(self.reconcile_dc(dc_id, candidates) for dc_id, candidates in active),
            return_exceptions=True,
        )
        for (dc_id, _), result in zip(active, results, strict=True):
            if isinstance(result, Exception):
                log.warning("Kho %s: chưa đối soát được phiếu xuất: %r", dc_id, result)

    async def reconcile_dc(self, dc_id: int, candidates: list[asyncpg.Record]) -> None:
        # dc_id chỉ lấy từ DC_IDS, danh sách ID phiếu truyền bằng tham số.
        async with asyncio.timeout(FDW_QUERY_TIMEOUT_S):
            orders = await self.pool.fetch(
                f"""SELECT transfer_id, direction, status, shipped_at FROM dc{dc_id}.transfer_order
                    WHERE transfer_id = ANY($1::uuid[])""",
                [row["transfer_id"] for row in candidates],
            )
        by_id = {order["transfer_id"]: order for order in orders}
        client = self.nodes.get(dc_id)
        for row in candidates:
            transfer_id = row["transfer_id"]
            order = by_id.get(transfer_id)
            if order is None:
                continue
            if row["status"] == TransferStatus.CREATED:
                if (
                    order["direction"] == TransferDirection.OUTBOUND
                    and order["status"] in (TransferStatus.SHIPPED, TransferStatus.RECEIVED)
                    and order["shipped_at"] is not None
                ):
                    if await record_shipment(self.store, transfer_id, dc_id, order["shipped_at"]):
                        log.info("Phiếu %s: phục hồi SHIPPED từ kho %s", transfer_id, dc_id)
            elif (
                client is not None
                and order["direction"] == TransferDirection.INBOUND
                and order["status"] == TransferStatus.CREATED
            ):
                # 200 rỗng có thể bỏ qua. Vòng sau đọc lại trạng thái thật qua FDW.
                await sync_status(
                    self.store, client, transfer_id, TransferStatus.SHIPPED, max_retries=0
                )


async def run(app: FastAPI) -> None:
    worker = ShipmentRecovery(app.state.pool, app.state.nodes)
    await run_periodic(HTTP_RETRY_INTERVAL_S, worker.tick, log)
