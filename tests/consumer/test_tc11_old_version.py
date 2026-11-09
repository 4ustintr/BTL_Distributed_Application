"""TC11: an older Kafka event must not replace the current snapshot on the shared stack."""

import asyncio
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest
from confluent_kafka import Producer
from stack_helpers import kafka_bootstrap, kafka_probe, reset_stack, wait_caught_up, wait_for

from common.constants import TOPIC_INVENTORY, EventType
from common.ids import inventory_aggregate_id
from common.models import InventoryEventPayload

pytestmark = [pytest.mark.integration, pytest.mark.optional]


async def test_tc11_old_version(demo_ids) -> None:
    t = await reset_stack()
    db = await asyncpg.connect(t.hq_db)
    probe = kafka_probe()
    try:
        await wait_caught_up(probe)
        before = await db.fetchrow(
            "SELECT * FROM inventory_snapshot WHERE dc_id = 1 AND product_id = 1"
        )
        assert before is not None, "make reset must seed the shared snapshot"
        # First advance through Kafka, then deliver an older version with a different event_id.
        producer = Producer({"bootstrap.servers": kafka_bootstrap()})
        base = {
            "event_type": EventType.INVENTORY_CHANGED,
            "source_dc": 1,
            "product_id": 1,
            "occurred_at": datetime.now(UTC),
            "qty_reserved": before["qty_reserved"],
        }
        newer = InventoryEventPayload(
            **base,
            event_id=UUID(demo_ids["REQ_RACE_1"]),
            qty_on_hand=before["qty_on_hand"],
            version=before["last_version"] + 2,
        )
        older = newer.model_copy(
            update={
                "event_id": UUID(demo_ids["REQ_RACE_2"]),
                "version": before["last_version"] + 1,
                "qty_on_hand": 0,
                "qty_reserved": 0,
            }
        )
        key = inventory_aggregate_id(1, 1)
        producer.produce(TOPIC_INVENTORY, key=key, value=newer.model_dump_json())
        assert await asyncio.to_thread(producer.flush, 10) == 0
        await wait_for(
            lambda: db.fetchval(
                "SELECT last_event_id FROM inventory_snapshot WHERE dc_id = 1 AND product_id = 1"
            ),
            lambda value: value == newer.event_id,
        )
        current = await db.fetchrow(
            "SELECT * FROM inventory_snapshot WHERE dc_id = 1 AND product_id = 1"
        )
        producer.produce(TOPIC_INVENTORY, key=key, value=older.model_dump_json())
        assert await asyncio.to_thread(producer.flush, 10) == 0
        await wait_for(
            lambda: db.fetchval(
                "SELECT 1 FROM processed_event WHERE event_id = $1", older.event_id
            ),
            bool,
        )
        assert (
            await db.fetchrow("SELECT * FROM inventory_snapshot WHERE dc_id = 1 AND product_id = 1")
            == current
        )
    finally:
        await asyncio.to_thread(probe.close)
        await db.close()
