"""Router đọc của HQ (F7): /dashboard/inventory, /invariant – lỗi F5, SKU lạ; không cần CSDL."""

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
