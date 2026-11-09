"""Tiện ích chung cho test Saga điều chuyển TC05, TC07 (docs/tests.md mục 4).

Địa chỉ hệ thống thật và make reset ở tests/fixtures.py (integration_target, make_reset).
Tồn kho và transfer_order ở kho đọc qua postgres_fdw (schema dc1..dc3, spec F3), nên test chỉ
cần một kết nối tới hq_db bằng role hq_app.
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import asyncpg
from fixtures import Target, integration_target, make_reset

from common.constants import DC_IDS, TransferStatus

__all__ = ["DC_BAC", "DC_NAM", "DC_TRUNG", "Target", "make_reset", "stock", "target", "wait_order"]

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS
WAIT_S = 10.0


def target() -> Target:
    return integration_target()


async def stock(db: asyncpg.Pool, dc_id: int, product_id: int) -> tuple[int, int]:
    """(tồn, khả dụng) của một SKU ở kho, đọc qua FDW."""
    row = await db.fetchrow(
        f"SELECT qty_on_hand, qty_reserved FROM dc{dc_id}.inventory "
        "WHERE dc_id = $1 AND product_id = $2",
        dc_id,
        product_id,
    )
    assert row is not None, f"kho {dc_id} chưa có dòng inventory cho SKU {product_id}"
    return row["qty_on_hand"], row["qty_on_hand"] - row["qty_reserved"]


async def wait_order(
    db: asyncpg.Pool, dc_id: int, transfer_id: UUID, status: TransferStatus
) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WAIT_S
    while True:
        row = await db.fetchrow(
            f"SELECT status, cancel_reason FROM dc{dc_id}.transfer_order WHERE transfer_id = $1",
            transfer_id,
        )
        found = dict(row) if row else {}
        if found.get("status") == status or loop.time() >= deadline:
            return found
        await asyncio.sleep(0.5)
