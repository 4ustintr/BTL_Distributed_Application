"""TC13: pause dc2 on the shared stack; keep the live warehouses' results."""

import asyncio
from time import monotonic

import asyncpg
import httpx
import pytest
from cdc_helpers import make, reset_stack, wait_status

from common.constants import DcStatus, InventorySource

pytestmark = [pytest.mark.integration, pytest.mark.optional]


async def test_tc13_timeout() -> None:
    t = await reset_stack()
    db = await asyncpg.connect(t.hq_db)
    try:
        await wait_status(db, 2, DcStatus.ONLINE)
        try:
            await asyncio.to_thread(make, "pause-dc2")
            async with httpx.AsyncClient(base_url=t.hq, timeout=8) as http:
                started = monotonic()
                response = await http.get("/dashboard/inventory?mode=realtime")
                duration = monotonic() - started
            assert response.status_code == 200, response.text
            # 0.5s tolerance accounts for driver cancellation after the F11 deadline.
            assert 2.5 <= duration <= 5.5, f"Hung-DC read took {duration:.3f}s"
            assert [row["source"] for row in response.json()["items"]] == [
                InventorySource.REALTIME,
                InventorySource.OFFLINE,
                InventorySource.REALTIME,
            ]
            print(f"TC13 shared stack: {duration:.3f}s")
        finally:
            await asyncio.to_thread(make, "unpause-dc2")
            await wait_status(db, 2, DcStatus.ONLINE)
    finally:
        await db.close()
