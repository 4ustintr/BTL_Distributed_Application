"""TC12/13 with a physically killed/paused test PG; invoke verify-faults.ps1.

Normal suite runs skip these two probes when the expected external fault is absent.
The deterministic connection-refused/hanging-server tests run in the normal suite.
"""

import os
from time import monotonic

import asyncpg
import httpx
import pytest
from fastapi import FastAPI
from routers import dashboard, invariant

from common.errors import install_error_handlers

pytestmark = pytest.mark.integration


def require_fault():
    if "chung-hq" not in os.environ.get("HQ_DB_URL", "") or os.environ.get("DC_ID") != "2":
        pytest.skip("External faults are enabled explicitly by verify-faults.ps1")


async def probe():
    pool = await asyncpg.create_pool(os.environ["HQ_DB_URL"], min_size=0)
    app = FastAPI()
    app.state.pool = pool
    install_error_handlers(app)
    app.include_router(dashboard.router)
    app.include_router(invariant.router)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            started = monotonic()
            response = await client.get("/dashboard/inventory?mode=realtime")
            elapsed = monotonic() - started
            assert response.status_code == 200
            assert [item["source"] for item in response.json()["items"]] == [
                "realtime",
                "offline",
                "realtime",
            ]
            assert (await client.get("/invariant")).status_code == 503
            return elapsed
    finally:
        await pool.close()


async def test_tc12_physically_killed_pg():
    require_fault()
    elapsed = await probe()
    print(f"TC12 actual SIGKILL: {elapsed:.3f}s")
    assert elapsed < 1


@pytest.mark.optional
async def test_tc13_physically_paused_pg():
    require_fault()
    elapsed = await probe()
    print(f"TC13 actual pause: {elapsed:.3f}s")
    assert 2.5 <= elapsed <= 5.5
