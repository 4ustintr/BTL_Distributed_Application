"""TC12: wait for HQ to mark dc2 OFFLINE, then the realtime read must take < 1 second."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from fixtures import SKU_A_PRODUCT_ID, integration_target, make, make_reset

from common.constants import DC_IDS, DashboardMode, DcStatus, InventorySource

pytestmark = pytest.mark.integration
DC_BAC, DC_TRUNG, DC_NAM = DC_IDS
SEED_STOCK = {DC_BAC: 100, DC_TRUNG: 40, DC_NAM: 10}


async def wait_for(http: httpx.AsyncClient, url: str, ready: Callable[[Any], bool]) -> None:
    async with asyncio.timeout(30):
        while True:
            response = await http.get(url)
            response.raise_for_status()
            if ready(response.json()):
                return
            await asyncio.sleep(0.2)


async def test_tc12_dead_dc_does_not_block_realtime_read() -> None:
    make_reset()
    t = integration_target()
    async with httpx.AsyncClient(timeout=15) as http:
        # HTTP seed commits may precede CDC delivery; keep a known snapshot before killing PG.
        await wait_for(
            http,
            f"{t.hq}/dashboard/inventory",
            lambda data: {row["dc_id"]: row["qty_on_hand"] for row in data["items"]} == SEED_STOCK,
        )
        make("kill-dc2")
        try:
            await wait_for(
                http,
                f"{t.hq}/dc-status",
                lambda rows: any(
                    row["dc_id"] == DC_TRUNG and row["status"] == DcStatus.OFFLINE for row in rows
                ),
            )
            params = {"mode": DashboardMode.REALTIME, "product_id": SKU_A_PRODUCT_ID}
            started = time.monotonic()
            response = await http.get(f"{t.hq}/dashboard/inventory", params=params)
            elapsed = time.monotonic() - started
            assert response.status_code == 200, response.text
            assert elapsed < 1.0, f"OFFLINE read took {elapsed:.3f}s"
            items = {item["dc_id"]: item for item in response.json()["items"]}
            assert items[DC_TRUNG]["source"] == InventorySource.OFFLINE
            assert items[DC_TRUNG]["qty_on_hand"] == SEED_STOCK[DC_TRUNG]
            for dc_id in (DC_BAC, DC_NAM):
                assert items[dc_id]["source"] == InventorySource.REALTIME
                assert items[dc_id]["qty_on_hand"] == SEED_STOCK[dc_id]
            print(f"TC12 shared stack: {elapsed:.3f}s")
        finally:
            make("start-dc2")
            make("restart-connector-dc2")
            await wait_for(
                http,
                f"{t.hq}/dc-status",
                lambda rows: any(
                    row["dc_id"] == DC_TRUNG and row["status"] == DcStatus.ONLINE for row in rows
                ),
            )
