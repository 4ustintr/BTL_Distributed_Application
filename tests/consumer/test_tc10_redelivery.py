"""TC10: make reset, insert dc1 outbox, reset the real hq-consumer group to earliest."""

import asyncio
from uuid import UUID

import asyncpg
import pytest
from stack_helpers import (
    command,
    kafka_probe,
    node_db_url,
    reset_offsets,
    reset_stack,
    wait_caught_up,
    wait_for,
)

from common.constants import AggregateType, EventType, RefType

pytestmark = pytest.mark.integration


async def test_tc10_redelivery(demo_ids) -> None:
    t = await reset_stack()
    db = await asyncpg.connect(t.hq_db)
    probe = kafka_probe()
    try:
        event_id = UUID(demo_ids["REQ_KB2_OUT"])
        node = await asyncpg.connect(node_db_url(1))
        try:
            # Test emitter only. All schema/roles/connector/topic are from infra/.
            inserted = await node.execute(
                """INSERT INTO outbox_event
                   (event_id, aggregate_type, aggregate_id, event_type, payload)
                   SELECT $1::uuid, $2::varchar, '1:1', $3::varchar, jsonb_build_object(
                     'event_id', ($1::uuid)::text, 'event_type', $3::varchar, 'source_dc', 1,
                     'occurred_at', clock_timestamp(), 'product_id', 1,
                     'qty_on_hand', qty_on_hand, 'qty_reserved', qty_reserved,
                     'version', version, 'request_id', ($1::uuid)::text, 'ref_type', $4::text)
                   FROM inventory WHERE dc_id = 1 AND product_id = 1""",
                event_id,
                AggregateType.INVENTORY,
                EventType.INVENTORY_CHANGED,
                RefType.RECEIPT,
            )
            assert inserted == "INSERT 0 1", "make reset must seed dc1 inventory"
        finally:
            await node.close()
        await wait_for(
            lambda: db.fetchval("SELECT 1 FROM processed_event WHERE event_id = $1", event_id),
            bool,
        )
        await wait_caught_up(probe)
        before = await db.fetch("SELECT * FROM inventory_snapshot ORDER BY dc_id, product_id")
        count = await db.fetchval("SELECT COUNT(*) FROM processed_event")
        try:
            await asyncio.to_thread(command, "docker", "stop", "hq-consumer")
            await asyncio.to_thread(reset_offsets)
        finally:
            await asyncio.to_thread(command, "docker", "start", "hq-consumer")
        await wait_caught_up(probe)
        assert await db.fetchval("SELECT COUNT(*) FROM processed_event") == count
        assert (
            await db.fetch("SELECT * FROM inventory_snapshot ORDER BY dc_id, product_id") == before
        )
    finally:
        await asyncio.to_thread(probe.close)
        await db.close()
