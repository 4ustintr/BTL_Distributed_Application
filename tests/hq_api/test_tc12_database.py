"""Real FDW reads through the production routers, including independent timeouts."""

import asyncio
from datetime import UTC, datetime
from time import monotonic
from uuid import UUID

import httpx
import pytest
from chung_support import cdc_db as isolated_database  # noqa: F401
from fastapi import FastAPI
from routers import dashboard, invariant

from common.errors import install_error_handlers

pytestmark = pytest.mark.integration


@pytest.fixture
async def read_client(cdc_db):
    pool, _ = cdc_db
    app = FastAPI()
    app.state.pool = pool
    install_error_handlers(app)
    app.include_router(dashboard.router)
    app.include_router(invariant.router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


async def test_tc12_live_stock_and_invariant(read_client):
    response = await read_client.get("/dashboard/inventory?mode=realtime")
    assert response.status_code == 200
    assert [item["qty_on_hand"] for item in response.json()["items"]] == [100, 40, 10]
    assert {item["source"] for item in response.json()["items"]} == {"realtime"}
    response = await read_client.get("/invariant")
    assert response.json() == {
        "stock": 150,
        "in_transit": 0,
        "shipped_out": 0,
        "received_in": 150,
        "diff": 0,
    }


async def test_tc12_refused_dc_preserves_other_results_under_one_second(read_client, cdc_db):
    _, admin = cdc_db
    # An unused loopback port gives the same FDW connection-refused path as a killed PG.
    await admin.execute("ALTER SERVER srv_dc2 OPTIONS (SET host '127.0.0.1', SET port '1')")
    started = monotonic()
    response = await read_client.get("/dashboard/inventory?mode=realtime")
    assert monotonic() - started < 1
    assert response.status_code == 200
    assert [item["source"] for item in response.json()["items"]] == [
        "realtime",
        "offline",
        "realtime",
    ]
    assert response.json()["items"][1]["qty_on_hand"] == 40
    response = await read_client.get("/invariant")
    assert response.status_code == 503
    assert response.json()["error_code"] == "DC_UNREACHABLE"


@pytest.mark.optional
async def test_tc13_hanging_dc_bounded_and_other_results_retained(read_client, cdc_db):
    _, admin = cdc_db
    # Accept TCP but never answer PostgreSQL's startup packet: deterministic hung node.
    writers = []

    async def hang(reader, writer):
        writers.append(writer)
        await reader.read()

    server = await asyncio.start_server(hang, "0.0.0.0", 0)
    port = server.sockets[0].getsockname()[1]
    await admin.execute(
        f"ALTER SERVER srv_dc2 OPTIONS (SET host 'chung-runner', SET port '{port}')"
    )
    try:
        started = monotonic()
        response = await read_client.get("/dashboard/inventory?mode=realtime")
        duration = monotonic() - started
        assert 2.5 <= duration <= 5.5
        assert [item["source"] for item in response.json()["items"]] == [
            "realtime",
            "offline",
            "realtime",
        ]
    finally:
        server.close()
        await server.wait_closed()
        for writer in writers:
            writer.close()
            await writer.wait_closed()


async def test_tc12_provisional_uses_only_pending_transactions(read_client, cdc_db, demo_ids):
    _, admin = cdc_db
    await admin.execute("UPDATE dc_status SET status = 'RECOVERING' WHERE dc_id = 2")
    await admin.execute(
        """INSERT INTO pending_txn (txn_id, dc_id, txn_type, product_id, qty, status)
           VALUES ($1, 2, 'INBOUND', 1, 30, 'PENDING'),
                  ($2, 2, 'OUTBOUND', 1, 10, 'PENDING'),
                  ($3, 2, 'OUTBOUND', 1, 100, 'REJECTED')""",
        UUID(demo_ids["REQ_KB3_IN"]),
        UUID(demo_ids["REQ_KB3_OUT"]),
        UUID(demo_ids["REQ_KB3_OUT_BIG"]),
    )
    for mode in ("snapshot", "realtime"):
        response = await read_client.get(f"/dashboard/inventory?mode={mode}")
        item = response.json()["items"][1]
        assert item["qty_on_hand"] == 60
        assert item["source"] == "provisional"
    pending = (await read_client.get("/pending-txn?dc_id=2")).json()
    assert len(pending) == 3
    assert {item["status"] for item in pending} == {"PENDING", "REJECTED"}
    statuses = (await read_client.get("/dc-status")).json()
    assert statuses[1]["status"] == "RECOVERING"
    assert datetime.fromisoformat(statuses[1]["updated_at"]).tzinfo is not None


async def test_tc12_in_transit_and_external_movements(read_client, cdc_db, demo_ids):
    _, admin = cdc_db
    await admin.execute(
        """INSERT INTO transfer_summary
           (transfer_id, transfer_code, from_dc_id, to_dc_id, status, items, created_at)
           VALUES ($1, 'TR-2026-000001', 1, 3, 'SHIPPED',
                   '[{"product_id":1,"qty":50}]', $2)""",
        UUID(demo_ids["REQ_KB3_IN"]),
        datetime.now(UTC),
    )
    items = (await read_client.get("/dashboard/inventory")).json()["items"]
    assert [item["qty_in_transit"] for item in items] == [0, 0, 50]
    # The deliberately unbalanced fixture must be reported, rather than forced to zero.
    assert (await read_client.get("/invariant")).json()["diff"] == 50
