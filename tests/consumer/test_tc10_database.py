"""TC10/TC11: test projection SQL against actual PostgreSQL 16."""

import asyncio

import asyncpg
import pytest
from chung_support import cdc_db as isolated_database  # noqa: F401
from chung_support import make_event

from consumer.processing import InventoryStore

pytestmark = pytest.mark.integration


async def test_tc10_duplicate_does_not_change_snapshot_or_processed_count(cdc_db, demo_ids):
    pool, admin = cdc_db
    event = make_event(demo_ids)
    store = InventoryStore(pool)
    await store.apply(event, b"2:1")
    before = await admin.fetchrow("SELECT * FROM inventory_snapshot WHERE dc_id = 2")
    assert await store.apply(event, b"2:1") is None
    assert await admin.fetchval("SELECT COUNT(*) FROM processed_event") == 1
    assert await admin.fetchrow("SELECT * FROM inventory_snapshot WHERE dc_id = 2") == before


@pytest.mark.optional
@pytest.mark.parametrize("old_version", [7, 8])
async def test_tc11_old_event_does_not_replace_new_snapshot(cdc_db, demo_ids, old_version):
    pool, admin = cdc_db
    store = InventoryStore(pool)
    await store.apply(make_event(demo_ids, version=8, qty=55), b"2:1")
    before = await admin.fetchrow("SELECT * FROM inventory_snapshot WHERE dc_id = 2")
    await store.apply(
        make_event(demo_ids, version=old_version, qty=65, request_key="REQ_KB3_IN"), b"2:1"
    )
    assert await admin.fetchrow("SELECT * FROM inventory_snapshot WHERE dc_id = 2") == before
    assert await admin.fetchval("SELECT COUNT(*) FROM processed_event") == 2


async def test_tc10_concurrent_delivery_has_one_effect(cdc_db, demo_ids):
    pool, admin = cdc_db
    store = InventoryStore(pool)
    event = make_event(demo_ids)
    results = await asyncio.gather(*(store.apply(event, b"2:1") for _ in range(5)))
    assert sum(value is not None for value in results) == 1
    assert await admin.fetchval("SELECT COUNT(*) FROM processed_event") == 1
    assert await admin.fetchval("SELECT qty_on_hand FROM inventory_snapshot WHERE dc_id = 2") == 35


async def test_tc10_failure_rolls_back_dedup_and_projection(cdc_db, demo_ids):
    pool, admin = cdc_db
    event = make_event(demo_ids).model_copy(update={"product_id": 999})
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await InventoryStore(pool).apply(event, b"2:999")
    assert await admin.fetchval("SELECT COUNT(*) FROM processed_event") == 0


async def test_tc10_replay_projection_and_applied_are_atomic(cdc_db, demo_ids):
    pool, admin = cdc_db
    event = make_event(demo_ids, version=8, qty=55, request_key="REQ_KB3_IN")
    await admin.execute(
        """INSERT INTO pending_txn (txn_id, dc_id, txn_type, product_id, qty)
           VALUES ($1, 2, 'INBOUND', 1, 30)""",
        event.request_id,
    )
    await InventoryStore(pool).apply(event, b"2:1")
    assert await admin.fetchval("SELECT status FROM pending_txn") == "APPLIED"
    assert await admin.fetchval("SELECT qty_on_hand FROM inventory_snapshot WHERE dc_id = 2") == 55


async def test_tc10_stale_replay_still_marks_pending_applied(cdc_db, demo_ids):
    pool, admin = cdc_db
    store = InventoryStore(pool)
    await store.apply(make_event(demo_ids, version=9, qty=55), b"2:1")
    event = make_event(demo_ids, version=8, qty=65, request_key="REQ_KB3_IN")
    await admin.execute(
        """INSERT INTO pending_txn (txn_id, dc_id, txn_type, product_id, qty)
           VALUES ($1, 2, 'INBOUND', 1, 30)""",
        event.request_id,
    )
    await store.apply(event, b"2:1")
    assert await admin.fetchval("SELECT status FROM pending_txn") == "APPLIED"
    assert await admin.fetchval("SELECT qty_on_hand FROM inventory_snapshot WHERE dc_id = 2") == 55
