"""KB3 concurrency against PostgreSQL, without the demo stack or make reset.

Set KB3_TEST_POSTGRES_DSN to the postgres database of a disposable cluster
with wal_level=logical, use its postgres superuser, and put psql on PATH.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import os
import subprocess
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from fastapi import FastAPI
from fixtures import DEMO_IDS, SKU_A_PRODUCT_ID
from routers import failover
from workers.health import HealthMonitor, PgHealthStore
from workers.replay import PgReplayStore, ReplayWorker

from common.constants import DcStatus, ErrorCode, LockMode, NodeMode, PendingTxnStatus, TxnType
from common.errors import install_error_handlers
from common.http import InternalClient
from common.models import FailoverResponse

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]
DC_TRUNG = 2
REQ_OUT = DEMO_IDS["REQ_KB3_OUT"]
REQ_OTHER = DEMO_IDS["REQ_KB3_OUT_BIG"]


@asynccontextmanager
async def migrated_db(dsn: str, migration: str, *options: str) -> AsyncIterator[asyncpg.Pool]:
    name = f"kb3_test_{uuid4().hex}"
    admin = await asyncpg.connect(dsn, timeout=10)
    try:
        await admin.execute(f"CREATE DATABASE {name}")
        try:
            url = urlsplit(dsn)
            db_dsn = url._replace(path=f"/{name}").geturl()
            psql_env = {
                **os.environ,
                "PGHOST": url.hostname or "localhost",
                "PGPORT": str(url.port or 5432),
                "PGUSER": unquote(url.username or ""),
                "PGDATABASE": name,
                "PGSSLMODE": parse_qs(url.query).get("sslmode", ["prefer"])[0],
            }
            if url.password is not None:
                psql_env["PGPASSWORD"] = unquote(url.password)
            result = await asyncio.to_thread(
                subprocess.run,
                ["psql", "-X", *options, "-f", str(ROOT / "db" / "migrations" / migration)],
                env=psql_env,
                timeout=15,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            assert result.returncode == 0, result.stderr
            async with asyncpg.create_pool(db_dsn, min_size=1, max_size=5, timeout=10) as pool:
                await pool.execute(
                    """INSERT INTO dc (dc_id, dc_code, dc_name, region)
                       VALUES (1, 'DC1', 'Bac', 'BAC'), (2, 'DC2', 'Trung', 'TRUNG');
                       INSERT INTO product_category (category_id, category_code, category_name)
                       VALUES (1, 'TEST', 'Test');
                       INSERT INTO product (product_id, sku, product_name, category_id, unit)
                       VALUES (1, 'SKU-A', 'Test', 1, 'box');"""
                )
                yield pool
        finally:
            await admin.execute(f"DROP DATABASE {name}")
    finally:
        await admin.close()


@pytest.fixture
def pg_dsn() -> str:
    dsn = os.environ.get("KB3_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("KB3_TEST_POSTGRES_DSN: cần PostgreSQL riêng cho test KB3")
    return dsn


@pytest.fixture
async def hq(pg_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    async with migrated_db(pg_dsn, "001_hq.sql") as pool:
        await pool.execute(
            """INSERT INTO dc_status (dc_id, status) VALUES (1, $1), (2, $1)""",
            DcStatus.OFFLINE,
        )
        await pool.execute(
            """INSERT INTO inventory_snapshot (dc_id, product_id, qty_on_hand)
               VALUES (1, $1, 35), (2, $1, 35)""",
            SKU_A_PRODUCT_ID,
        )
        yield pool


@pytest.fixture
async def hq_http(hq: asyncpg.Pool) -> AsyncIterator[httpx.AsyncClient]:
    app = FastAPI()
    app.state.pool = hq
    install_error_handlers(app)
    app.include_router(failover.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://hq") as http:
        yield http


async def outbound(
    http: httpx.AsyncClient, request_id: str, dc_id: int = DC_TRUNG, *, qty: int = 30
) -> httpx.Response:
    return await http.post(
        f"/failover/dc/{dc_id}/outbound",
        json={
            "request_id": request_id,
            "product_id": SKU_A_PRODUCT_ID,
            "qty": qty,
            "created_by": "kb3-test",
        },
    )


def hold_provisional_read(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[asyncio.Future[int], asyncio.Event]:
    """Đỗ request đầu ngay sau khi đọc tồn tạm, trước INSERT và COMMIT."""
    parked = asyncio.get_running_loop().create_future()
    release = asyncio.Event()
    original = failover.read_provisional

    async def gated_read(conn: asyncpg.Connection, dc_id: int, product_id: int) -> tuple[int, int]:
        result = await original(conn, dc_id, product_id)
        if not parked.done():
            parked.set_result(conn.get_server_pid())
            await release.wait()
        return result

    monkeypatch.setattr(failover, "read_provisional", gated_read)
    return parked, release


async def wait_for_db_lock_or_result(
    hq: asyncpg.Pool, task: asyncio.Task[Any], blocker_pid: int
) -> None:
    """Chỉ nhả bên giữ khóa khi PG xác nhận đã có bên chờ hoặc code cũ đã chạy qua."""
    while not task.done():
        blocked = await hq.fetchval(
            """SELECT EXISTS (
                   SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))
               )""",
            blocker_pid,
        )
        if blocked:
            return
        await asyncio.sleep(0.01)


@pytest.mark.parametrize("same_id", [False, True], ids=["distinct-ids", "same-id"])
async def test_concurrent_outbound(
    hq: asyncpg.Pool, hq_http: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, same_id: bool
) -> None:
    parked, release = hold_provisional_read(monkeypatch)
    async with asyncio.timeout(10), asyncio.TaskGroup() as tasks:
        first = tasks.create_task(outbound(hq_http, REQ_OUT))
        blocker = await parked
        second = tasks.create_task(outbound(hq_http, REQ_OUT if same_id else REQ_OTHER))
        await wait_for_db_lock_or_result(hq, second, blocker)
        release.set()

    assert first.result().status_code == 200
    assert first.result().json()["provisional_qty"] == 5
    response = second.result()
    if same_id:
        assert response.status_code == 200
        assert response.json()["txn_id"] == first.result().json()["txn_id"] == REQ_OUT
        assert response.json()["provisional_qty"] == 5
        assert response.json()["duplicate"] != first.result().json()["duplicate"]
    else:
        assert response.status_code == 422, response.text
        assert response.json()["error_code"] == ErrorCode.INSUFFICIENT_STOCK
        assert response.json()["details"]["available"] == 5
    assert await hq.fetchval("SELECT count(*) FROM pending_txn") == 1


async def test_temporary_stock_uses_one_snapshot(
    hq: asyncpg.Pool, hq_http: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Consumer áp dụng lệnh xuất 10 đúng lúc HQ đọc snapshot 35."""
    assert (await outbound(hq_http, REQ_OUT, qty=10)).status_code == 200
    parked, release = asyncio.Event(), asyncio.Event()

    def gate_snapshot(query_method):
        async def query(conn, sql, *args, **kwargs):
            result = await query_method(conn, sql, *args, **kwargs)
            if "inventory_snapshot" in sql and not parked.is_set():
                parked.set()
                await release.wait()
            return result

        return query

    monkeypatch.setattr(asyncpg.Connection, "fetchrow", gate_snapshot(asyncpg.Connection.fetchrow))
    monkeypatch.setattr(asyncpg.Connection, "fetch", gate_snapshot(asyncpg.Connection.fetch))
    async with asyncio.timeout(10), asyncio.TaskGroup() as tasks:
        request = tasks.create_task(outbound(hq_http, REQ_OTHER))
        await parked.wait()
        async with hq.acquire() as conn, conn.transaction():
            await conn.execute("UPDATE inventory_snapshot SET qty_on_hand = 25 WHERE dc_id = 2")
            await conn.execute(
                "UPDATE pending_txn SET status = $2 WHERE txn_id = $1 AND status = $3",
                REQ_OUT,
                PendingTxnStatus.APPLIED,
                PendingTxnStatus.PENDING,
            )
        release.set()

    response = request.result()
    assert response.status_code == 422, response.text
    assert response.json()["details"]["available"] == 25


async def test_replay_orders_by_admission_when_requests_overlap(
    hq: asyncpg.Pool, hq_http: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    await hq.execute("UPDATE inventory_snapshot SET qty_on_hand = 5 WHERE dc_id = 2")
    parked, release = asyncio.Event(), asyncio.Event()
    original = failover.duplicate_response

    async def duplicate_response(conn: asyncpg.Connection, txn_id: UUID) -> FailoverResponse | None:
        response = await original(conn, txn_id)
        if str(txn_id) == REQ_OUT and not parked.is_set():
            parked.set()
            await release.wait()
        return response

    monkeypatch.setattr(failover, "duplicate_response", duplicate_response)
    inbound_id = DEMO_IDS["REQ_KB3_IN"]
    async with asyncio.timeout(10), asyncio.TaskGroup() as tasks:
        issue = tasks.create_task(outbound(hq_http, REQ_OUT))
        await parked.wait()
        receipt = await hq_http.post(
            "/failover/dc/2/inbound",
            json={
                "request_id": inbound_id,
                "product_id": SKU_A_PRODUCT_ID,
                "qty": 30,
                "created_by": "kb3-test",
            },
        )
        assert receipt.status_code == 200
        release.set()

    assert issue.result().status_code == 200
    pending = await PgReplayStore(hq).pending(DC_TRUNG)
    assert [str(row.txn_id) for row in pending] == [inbound_id, REQ_OUT]


@pytest.mark.parametrize("mode", [NodeMode.ONLINE, NodeMode.RECOVERING])
async def test_request_admitted_before_handoff(
    hq: asyncpg.Pool, hq_http: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, mode: NodeMode
) -> None:
    parked, release = hold_provisional_read(monkeypatch)
    store = PgReplayStore(hq)
    monitor = HealthMonitor(PgHealthStore(hq), {})
    calls = []

    async def node(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/replay":
            return httpx.Response(200, json={"qty_on_hand": 5, "duplicate": True})
        assert request.url.path == "/recovery/complete"
        return httpx.Response(200, json={"mode": "ONLINE"})

    async with InternalClient("http://node", "test", transport=httpx.MockTransport(node)) as client:
        worker = ReplayWorker(store, {DC_TRUNG: client})

        async def recover() -> None:
            await monitor._on_success(DC_TRUNG, mode)
            await worker.tick()

        async with asyncio.timeout(10), asyncio.TaskGroup() as tasks:
            admitted = tasks.create_task(outbound(hq_http, REQ_OUT))
            blocker = await parked
            recovery = tasks.create_task(recover())
            await wait_for_db_lock_or_result(hq, recovery, blocker)
            release.set()

    assert admitted.result().status_code == 200
    assert await hq.fetchval("SELECT status FROM dc_status WHERE dc_id = 2") == DcStatus.ONLINE
    assert await hq.fetchval("SELECT status FROM pending_txn") == PendingTxnStatus.APPLIED
    assert calls == ["/replay", "/recovery/complete"]


@pytest.mark.parametrize("mode", [NodeMode.ONLINE, NodeMode.RECOVERING])
async def test_handoff_checks_empty_queue_before_new_request(
    hq: asyncpg.Pool, hq_http: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, mode: NodeMode
) -> None:
    parked = asyncio.get_running_loop().create_future()
    release = asyncio.Event()
    original = asyncpg.Connection.fetchval

    async def fetchval(conn, query, *args, **kwargs):
        result = await original(conn, query, *args, **kwargs)
        if "SELECT EXISTS (SELECT 1 FROM pending_txn" in query and not parked.done():
            assert result is False
            parked.set_result(conn.get_server_pid())
            await release.wait()
        return result

    monkeypatch.setattr(asyncpg.Connection, "fetchval", fetchval)
    monitor = HealthMonitor(PgHealthStore(hq), {})
    async with asyncio.timeout(10), asyncio.TaskGroup() as tasks:
        tasks.create_task(monitor._on_success(DC_TRUNG, mode))
        blocker = await parked
        request = tasks.create_task(outbound(hq_http, REQ_OUT))
        await wait_for_db_lock_or_result(hq, request, blocker)
        # A different warehouse can still accept a request while this one is locked.
        assert (await outbound(hq_http, REQ_OTHER, dc_id=1)).status_code == 200
        release.set()

    response = request.result()
    assert response.status_code == 409, response.text
    assert response.json()["error_code"] == ErrorCode.DC_NOT_OFFLINE
    expected = DcStatus.ONLINE if mode == NodeMode.ONLINE else DcStatus.RECOVERING
    assert await hq.fetchval("SELECT status FROM dc_status WHERE dc_id = 2") == expected
    assert not await hq.fetchval("SELECT EXISTS (SELECT 1 FROM pending_txn WHERE dc_id = 2)")


async def test_replay_rechecks_queue_after_status_lock(hq: asyncpg.Pool) -> None:
    """Vòng replay cũ hoàn tất khi HQ vừa qua một đợt OFFLINE -> RECOVERING mới."""
    await hq.execute("UPDATE dc_status SET status = $1 WHERE dc_id = 2", DcStatus.RECOVERING)
    store = PgReplayStore(hq)
    async with asyncio.timeout(10), asyncio.TaskGroup() as tasks:
        async with hq.acquire() as conn, conn.transaction():
            await conn.execute("UPDATE dc_status SET status = $1 WHERE dc_id = 2", DcStatus.OFFLINE)
            await conn.execute(
                """INSERT INTO pending_txn (txn_id, dc_id, txn_type, product_id, qty)
                   VALUES ($1, 2, $3, $2, 30)""",
                REQ_OUT,
                SKU_A_PRODUCT_ID,
                TxnType.OUTBOUND,
            )
            await conn.execute(
                "UPDATE dc_status SET status = $1 WHERE dc_id = 2", DcStatus.RECOVERING
            )
            finish = tasks.create_task(store.set_online(DC_TRUNG))
            await wait_for_db_lock_or_result(hq, finish, conn.get_server_pid())

    assert finish.result() is False
    assert await hq.fetchval("SELECT status FROM dc_status WHERE dc_id = 2") == DcStatus.RECOVERING
    assert await hq.fetchval("SELECT status FROM pending_txn") == PendingTxnStatus.PENDING


@pytest.mark.parametrize("actual_stock", [35, 5], ids=["duplicate-replay", "stale-snapshot"])
async def test_replay_uses_real_node_stock(
    hq: asyncpg.Pool,
    hq_http: httpx.AsyncClient,
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    actual_stock: int,
) -> None:
    node_path = ROOT / "node-api"
    monkeypatch.syspath_prepend(str(node_path))
    spec = importlib.util.spec_from_file_location(
        "kb3_node_inventory", node_path / "routers/inventory.py"
    )
    assert spec is not None and spec.loader is not None
    inventory = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(inventory)
    recovery = importlib.import_module("recovery")
    async with migrated_db(pg_dsn, "002_dc.sql", "-v", "dc_id=2") as node_db:
        await node_db.execute(
            "INSERT INTO inventory (dc_id, product_id, qty_on_hand) VALUES (2, $1, $2)",
            SKU_A_PRODUCT_ID,
            actual_stock,
        )
        node = FastAPI()
        node.state.pool = node_db
        node.state.dc_id = DC_TRUNG
        node.state.mode = NodeMode.RECOVERING
        node.state.lock_mode = LockMode.OPTIMISTIC
        install_error_handlers(node)
        node.include_router(inventory.router)
        node.include_router(recovery.router)

        assert (await outbound(hq_http, REQ_OUT)).status_code == 200
        await HealthMonitor(PgHealthStore(hq), {})._on_success(DC_TRUNG, NodeMode.RECOVERING)
        async with InternalClient(
            "http://node", "test", transport=httpx.ASGITransport(node)
        ) as client:
            worker = ReplayWorker(PgReplayStore(hq), {DC_TRUNG: client})
            await worker.tick()
            if actual_stock == 35:
                assert (
                    await hq.fetchval("SELECT status FROM pending_txn") == PendingTxnStatus.PENDING
                )
                await worker.tick()  # no Kafka in this test: Node returns duplicate on retry

        status = PendingTxnStatus.APPLIED if actual_stock == 35 else PendingTxnStatus.REJECTED
        pending = await hq.fetchrow("SELECT status, attempts, error_msg FROM pending_txn")
        assert pending["status"] == status
        if status == PendingTxnStatus.REJECTED:
            assert pending["attempts"] == 1
            assert ErrorCode.INSUFFICIENT_STOCK in pending["error_msg"]
        else:
            assert pending["attempts"] == 2
        assert await hq.fetchval("SELECT status FROM dc_status WHERE dc_id = 2") == DcStatus.ONLINE
        assert node.state.mode == NodeMode.ONLINE
        assert await node_db.fetchval("SELECT qty_on_hand FROM inventory") == 5
        movement_count = int(actual_stock == 35)
        assert await node_db.fetchval("SELECT count(*) FROM stock_movement") == movement_count
        assert await node_db.fetchval("SELECT count(*) FROM outbox_event") == movement_count
