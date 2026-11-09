"""TC12/TC13 and F7 read endpoints, without requiring the other owners' workers."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from fastapi import FastAPI
from routers import dashboard, invariant

from common.errors import install_error_handlers


@pytest.fixture
def read_app():
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(dashboard.router)
    app.include_router(invariant.router)
    app.state.pool = Mock()
    return app


async def test_routes_are_registered(read_app):
    paths = set(read_app.openapi()["paths"])
    assert {"/dashboard/inventory", "/dc-status", "/pending-txn", "/invariant"} <= paths


@pytest.mark.parametrize("query", ["mode=wrong", "product_id=0", "product_id=no"])
async def test_bad_query_uses_f5_errors(read_app, query):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=read_app), base_url="http://test"
    ) as client:
        response = await client.get(f"/dashboard/inventory?{query}")
    assert response.status_code == 400
    assert response.json()["error_code"] == "VALIDATION_ERROR"


async def test_unknown_product_is_not_zero_stock(read_app):
    read_app.state.pool.fetchval = AsyncMock(return_value=None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=read_app), base_url="http://test"
    ) as client:
        response = await client.get("/dashboard/inventory?product_id=999")
    assert response.status_code == 404


@pytest.mark.parametrize(
    "mode,quantity,source",
    [
        ("snapshot", 55, "provisional"),
        ("realtime", 35, "offline"),
    ],
)
async def test_offline_realtime_never_queries_fdw_even_with_pending(
    read_app,
    monkeypatch,
    mode,
    quantity,
    source,
):
    read_app.state.pool.fetchval = AsyncMock(return_value=1)
    read_app.state.pool.fetch = AsyncMock(
        return_value=[
            {
                "dc_id": 2,
                "qty_on_hand": 35,
                "qty_reserved": 0,
                "qty_in_transit": 0,
                "synced_at": datetime.now(UTC),
                "status": "OFFLINE",
                "pending_count": 2,
                "delta": 20,
            }
        ]
    )
    live = AsyncMock(side_effect=AssertionError("OFFLINE must not acquire a FDW connection"))
    monkeypatch.setattr(dashboard, "fetch_live_inventory", live)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=read_app),
        base_url="http://test",
    ) as client:
        response = await client.get(f"/dashboard/inventory?mode={mode}")
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert (item["qty_on_hand"], item["source"]) == (quantity, source)
    live.assert_not_awaited()
