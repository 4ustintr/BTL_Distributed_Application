"""TC12: kill dc2 on the shared stack, wait OFFLINE, then measure the realtime read."""

import asyncio
from time import monotonic

import asyncpg
import httpx
import pytest
from cdc_helpers import kafka_probe, make, reset_stack, wait_caught_up, wait_status

from common.constants import DcStatus, InventorySource

pytestmark = pytest.mark.integration


async def test_tc12_offline() -> None:
    t = await reset_stack()
    db = await asyncpg.connect(t.hq_db)
    probe = kafka_probe()
    try:
        await wait_caught_up(probe)
        await wait_status(db, 2, DcStatus.ONLINE)
        async with httpx.AsyncClient(base_url=t.hq, timeout=8) as http:
            baseline = (await http.get("/dashboard/inventory")).json()["items"]
            try:
                await asyncio.to_thread(make, "kill-dc2")
                await wait_status(db, 2, DcStatus.OFFLINE)
                started = monotonic()
                response = await http.get("/dashboard/inventory?mode=realtime")
                duration = monotonic() - started
                assert response.status_code == 200, response.text
                assert duration < 1, f"OFFLINE read took {duration:.3f}s"
                items = response.json()["items"]
                assert [row["source"] for row in items] == [
                    InventorySource.REALTIME,
                    InventorySource.OFFLINE,
                    InventorySource.REALTIME,
                ]
                assert items[1]["qty_on_hand"] == baseline[1]["qty_on_hand"]
                assert items[1]["as_of"] == baseline[1]["as_of"]
                assert (await http.get("/invariant")).status_code == 503
                print(f"TC12 shared stack: {duration:.3f}s")
            finally:
                await asyncio.to_thread(make, "start-dc2")
                await asyncio.to_thread(make, "restart-connector-dc2")
                await wait_status(db, 2, DcStatus.ONLINE)
    finally:
        await asyncio.to_thread(probe.close)
        await db.close()
