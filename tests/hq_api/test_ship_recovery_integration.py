"""Phục hồi SHIPPED trên stack test riêng. Mỗi ca gọi make reset."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import TypeVar
from urllib.parse import urlsplit
from uuid import UUID

import asyncpg
import httpx
import pytest
from confluent_kafka import Producer
from fixtures import SKU_A_PRODUCT_ID, docker
from saga_helpers import DC_BAC, DC_NAM, Target, make_reset, stock, target, wait_order

from common.constants import (
    INTERNAL_KEY_HEADER,
    TOPIC_TRANSFER,
    ErrorCode,
    EventType,
    MovementType,
    TransferStatus,
)

pytestmark = pytest.mark.integration
HQ_API = "hq-api"
KAFKA_EXTERNAL_PORT = 29092  # F2
ACTOR = {"created_by": "ship-recovery-test"}
T = TypeVar("T")


async def wait_for(fetch: Callable[[], Awaitable[T]], ready: Callable[[T], bool]) -> T:
    async with asyncio.timeout(25):
        while True:
            result = await fetch()
            if ready(result):
                return result
            await asyncio.sleep(0.25)


async def create(http: httpx.AsyncClient, t: Target) -> UUID:
    result = await http.post(
        f"{t.hq}/transfers",
        json={
            "from_dc_id": DC_BAC,
            "to_dc_id": DC_NAM,
            "items": [{"product_id": SKU_A_PRODUCT_ID, "qty": 50}],
        },
    )
    assert result.status_code == 201, result.text
    return UUID(result.json()["transfer_id"])


async def summary(db: asyncpg.Pool, tid: UUID) -> asyncpg.Record:
    row = await db.fetchrow(
        "SELECT status, shipped_at, last_sync_at FROM transfer_summary WHERE transfer_id = $1",
        tid,
    )
    assert row is not None
    return row


async def movements(db: asyncpg.Pool, tid: UUID) -> dict[tuple[int, str], int]:
    rows = await db.fetch(
        """SELECT dc_id, movement_type, qty_change FROM dc1.stock_movement WHERE ref_id = $1
           UNION ALL
           SELECT dc_id, movement_type, qty_change FROM dc3.stock_movement WHERE ref_id = $1""",
        str(tid),
    )
    assert len(rows) == len({(r["dc_id"], r["movement_type"]) for r in rows}), rows
    return {(r["dc_id"], r["movement_type"]): r["qty_change"] for r in rows}


def publish_late_outbox_event(t: Target, tid: UUID) -> None:
    payload = docker(
        "exec", "pg-dc1", "psql", "-X", "-U", "postgres", "-d", "dc1", "-Atc",
        f"SELECT payload FROM outbox_event WHERE aggregate_id = '{tid}' "
        f"AND event_type = '{EventType.TRANSFER_SHIPPED}'",
    ).strip()  # fmt: skip
    event = json.loads(payload)
    assert event["transfer_id"] == str(tid)
    producer = Producer(
        {
            "bootstrap.servers": f"{urlsplit(t.hq).hostname}:{KAFKA_EXTERNAL_PORT}",
            "message.timeout.ms": 5000,
        }
    )
    delivery = []
    producer.produce(
        TOPIC_TRANSFER,
        key=str(tid),
        value=payload,
        on_delivery=lambda error, _: delivery.append(error),
    )
    assert producer.flush(8) == 0
    assert delivery == [None]


async def receive_and_verify(
    http: httpx.AsyncClient, t: Target, db: asyncpg.Pool, tid: UUID
) -> None:
    assert (await wait_order(db, DC_NAM, tid, TransferStatus.SHIPPED))[
        "status"
    ] == TransferStatus.SHIPPED
    shipped = await summary(db, tid)
    assert shipped["status"] == TransferStatus.SHIPPED
    assert await stock(db, DC_NAM, SKU_A_PRODUCT_ID) == (10, 10)
    assert await movements(db, tid) == {(DC_BAC, MovementType.TRANSFER_OUT): -50}

    received = await http.post(f"{t.nodes[DC_NAM]}/transfers/{tid}/receive", json=ACTOR)
    assert received.status_code == 200, received.text
    assert received.json() == {"status": TransferStatus.RECEIVED, "duplicate": False}
    assert (await wait_order(db, DC_BAC, tid, TransferStatus.RECEIVED))[
        "status"
    ] == TransferStatus.RECEIVED

    await asyncio.to_thread(publish_late_outbox_event, t, tid)
    headers = {INTERNAL_KEY_HEADER: t.internal_key}
    late = await http.post(
        f"{t.hq}/transfers/{tid}/shipped",
        json={"shipped_at": shipped["shipped_at"].isoformat()},
        headers=headers,
    )
    assert late.json() == {"status": TransferStatus.RECEIVED, "duplicate": True}
    manual_retry = await http.post(f"{t.nodes[DC_BAC]}/transfers/{tid}/ship", json=ACTOR)
    assert manual_retry.json() == {"status": TransferStatus.RECEIVED, "duplicate": True}
    skipped = await http.post(
        f"{t.nodes[DC_NAM]}/transfers/{tid}/sync-status",
        json={"status": TransferStatus.SHIPPED},
        headers=headers,
    )
    assert skipped.status_code == 200 and skipped.content == b""

    assert (await summary(db, tid))["status"] == TransferStatus.RECEIVED
    assert (await wait_order(db, DC_NAM, tid, TransferStatus.RECEIVED))[
        "status"
    ] == TransferStatus.RECEIVED
    history = await db.fetch(
        "SELECT from_status, to_status FROM transfer_event WHERE transfer_id = $1", tid
    )
    assert len(history) == 3
    assert {(row["from_status"], row["to_status"]) for row in history} == {
        (None, TransferStatus.CREATED),
        (TransferStatus.CREATED, TransferStatus.SHIPPED),
        (TransferStatus.SHIPPED, TransferStatus.RECEIVED),
    }
    ledger = {
        (DC_BAC, MovementType.TRANSFER_OUT): -50,
        (DC_NAM, MovementType.TRANSFER_IN): 50,
    }
    assert await movements(db, tid) == ledger
    assert await stock(db, DC_BAC, SKU_A_PRODUCT_ID) == (50, 50)
    assert await stock(db, DC_NAM, SKU_A_PRODUCT_ID) == (60, 60)
    await wait_for(
        lambda: db.fetch(
            "SELECT dc_id, qty_on_hand FROM inventory_snapshot WHERE product_id = $1",
            SKU_A_PRODUCT_ID,
        ),
        lambda rows: {r["dc_id"]: r["qty_on_hand"] for r in rows} == {1: 50, 2: 40, 3: 60},
    )
    invariant = await http.get(f"{t.hq}/invariant", params={"product_id": SKU_A_PRODUCT_ID})
    assert invariant.status_code == 200 and invariant.json()["diff"] == 0
    print(f"{tid}: RECEIVED, Bắc -50 x1, Nam +50 x1, snapshot 50/40/60, DIFF=0")


async def test_recovers_when_north_commits_without_hq_callback() -> None:
    make_reset()
    t = target()
    async with httpx.AsyncClient(timeout=30) as http, asyncpg.create_pool(t.hq_db) as db:
        tid = await create(http, t)
        await asyncio.to_thread(docker, "stop", HQ_API)
        try:
            ship = await http.post(f"{t.nodes[DC_BAC]}/transfers/{tid}/ship", json=ACTOR)
            assert ship.status_code == 503 and ship.json()["error_code"] == ErrorCode.DC_UNREACHABLE
            assert (await summary(db, tid))["status"] == TransferStatus.CREATED
            assert (await wait_order(db, DC_BAC, tid, TransferStatus.SHIPPED))[
                "status"
            ] == TransferStatus.SHIPPED
            assert await movements(db, tid) == {(DC_BAC, MovementType.TRANSFER_OUT): -50}
            print(f"{tid}: ship=503, Bắc SHIPPED có một TRANSFER_OUT, HQ CREATED")
        finally:
            await asyncio.to_thread(docker, "start", HQ_API)
        # Chờ Nam tự tiến trước khi gửi bất kỳ callback hay thao tác Xuất lại nào.
        await receive_and_verify(http, t, db, tid)


def block_nam_update(tid: UUID) -> None:
    docker(
        "exec", "pg-dc3", "psql", "-X", "-U", "postgres", "-d", "dc3", "-v", "ON_ERROR_STOP=1",
        "-c", f"""
        CREATE FUNCTION skip_test_ship() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.transfer_id = '{tid}' AND NEW.status = 'SHIPPED' THEN
                RETURN NULL;
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER skip_test_ship BEFORE UPDATE ON transfer_order
        FOR EACH ROW EXECUTE FUNCTION skip_test_ship();
        """,
    )  # fmt: skip


def unblock_nam_update() -> None:
    docker(
        "exec", "pg-dc3", "psql", "-X", "-U", "postgres", "-d", "dc3", "-v", "ON_ERROR_STOP=1",
        "-c", "DROP TRIGGER skip_test_ship ON transfer_order; DROP FUNCTION skip_test_ship();",
    )  # fmt: skip


async def test_retries_destination_after_hq_restart_even_if_sync_returned_200() -> None:
    make_reset()
    t = target()
    async with httpx.AsyncClient(timeout=25) as http, asyncpg.create_pool(t.hq_db) as db:
        tid = await create(http, t)
        await asyncio.to_thread(block_nam_update, tid)
        try:
            ship = await http.post(f"{t.nodes[DC_BAC]}/transfers/{tid}/ship", json=ACTOR)
            assert ship.status_code == 200, ship.text
            first = await wait_for(
                lambda: summary(db, tid), lambda row: row["last_sync_at"] is not None
            )
            assert first["status"] == TransferStatus.SHIPPED
            assert (await wait_order(db, DC_NAM, tid, TransferStatus.CREATED))[
                "status"
            ] == TransferStatus.CREATED
            premature = await http.post(f"{t.nodes[DC_NAM]}/transfers/{tid}/receive", json=ACTOR)
            assert premature.status_code == 409
            assert await stock(db, DC_NAM, SKU_A_PRODUCT_ID) == (10, 10)
            print(f"{tid}: HQ SHIPPED + last_sync_at, Nam CREATED sau HTTP 200 bỏ qua")

            await asyncio.to_thread(docker, "restart", HQ_API)
            restarted: datetime = await db.fetchval("SELECT clock_timestamp()")
            await wait_for(
                lambda: summary(db, tid),
                lambda row: row["last_sync_at"] is not None and row["last_sync_at"] > restarted,
            )
            assert (await wait_order(db, DC_NAM, tid, TransferStatus.CREATED))[
                "status"
            ] == TransferStatus.CREATED
            print(f"{tid}: HQ restart, worker đã thử lại, Nam vẫn CREATED khi trigger còn bật")
        finally:
            await asyncio.to_thread(unblock_nam_update)
        await receive_and_verify(http, t, db, tid)
