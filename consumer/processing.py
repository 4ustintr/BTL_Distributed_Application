"""Durable, idempotent inventory projections; Kafka offsets follow database commits."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any

import asyncpg
from confluent_kafka import Consumer, Message, Producer

from common.constants import DC_IDS, TOPIC_DLQ, PendingTxnStatus
from common.ids import inventory_aggregate_id
from common.models import InventoryEventPayload

log = logging.getLogger("hq-consumer")


def parse_event(message: Message) -> InventoryEventPayload:
    """Validate F8 before touching the database; malformed events go to DLQ."""
    raw = message.value()
    if raw is None:
        raise ValueError("Inventory event cannot be a tombstone")
    event = InventoryEventPayload.model_validate_json(raw, strict=True)
    if (
        event.source_dc not in DC_IDS
        or not 0 < event.product_id <= 2**63 - 1
        or not 0 < event.version <= 2**63 - 1
    ):
        raise ValueError("Invalid inventory identity or version")
    if not 0 <= event.qty_reserved <= event.qty_on_hand <= 2**31 - 1:
        raise ValueError("Invalid inventory quantities")
    if event.occurred_at.utcoffset() is None:
        raise ValueError("occurred_at must include a timezone")
    expected_key = inventory_aggregate_id(event.source_dc, event.product_id).encode()
    if message.key() != expected_key:
        raise ValueError("Kafka key does not match inventory identity")
    return event


class InventoryStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def apply(self, event: InventoryEventPayload, key: bytes) -> datetime | None:
        """db.md 3.4 steps 2–4, including APPLIED for older replay events."""
        async with self.pool.acquire() as connection, connection.transaction():
            inserted = await connection.fetchval(
                """INSERT INTO processed_event
                       (event_id, source_dc, event_type, aggregate_id)
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT (event_id) DO NOTHING RETURNING event_id""",
                event.event_id,
                event.source_dc,
                event.event_type,
                key.decode(),
            )
            if inserted is None:
                return None
            synced_at = await connection.fetchval(
                """INSERT INTO inventory_snapshot AS s
                       (dc_id, product_id, qty_on_hand, qty_reserved,
                        last_version, last_event_id, synced_at)
                   VALUES ($1, $2, $3, $4, $5, $6, now())
                   ON CONFLICT (dc_id, product_id) DO UPDATE
                   SET qty_on_hand = EXCLUDED.qty_on_hand,
                       qty_reserved = EXCLUDED.qty_reserved,
                       last_version = EXCLUDED.last_version,
                       last_event_id = EXCLUDED.last_event_id, synced_at = now()
                   WHERE s.last_version < EXCLUDED.last_version
                   RETURNING synced_at""",
                event.source_dc,
                event.product_id,
                event.qty_on_hand,
                event.qty_reserved,
                event.version,
                event.event_id,
            )
            if event.request_id is not None:
                await connection.execute(
                    """UPDATE pending_txn SET status = $2, applied_at = now(), updated_at = now()
                       WHERE txn_id = $1 AND status = $3""",
                    event.request_id,
                    PendingTxnStatus.APPLIED,
                    PendingTxnStatus.PENDING,
                )
        return synced_at


def deliver_to_dlq(producer: Producer, message: Message) -> None:
    """Wait for broker delivery; a failed DLQ write must not advance the source offset."""
    delivered = False
    delivery_error: Any = None

    def on_delivery(error: Any, _: Message) -> None:
        nonlocal delivered, delivery_error
        delivered = True
        delivery_error = error

    producer.produce(
        TOPIC_DLQ,
        key=message.key(),
        value=message.value() or b"null",
        headers=message.headers(),
        on_delivery=on_delivery,
    )
    outstanding = producer.flush(10)
    if outstanding or not delivered or delivery_error is not None:
        raise RuntimeError(f"DLQ delivery failed: {delivery_error}; pending={outstanding}")


async def handle_message(
    message: Message, store: InventoryStore, consumer: Consumer, producer: Producer
) -> None:
    try:
        event = parse_event(message)
    except ValueError as error:
        log.warning("Invalid inventory event: %s", error)
        await asyncio.to_thread(deliver_to_dlq, producer, message)
    else:
        synced_at = await store.apply(event, message.key())
        if synced_at is not None:
            # M1 is an event-time measurement (F8/tests.md), not an ordering mechanism.
            log.info(
                json.dumps(
                    {
                        "event_id": str(event.event_id),
                        "request_id": str(event.request_id) if event.request_id else None,
                        "source_dc": event.source_dc,
                        "product_id": event.product_id,
                        "version": event.version,
                        "occurred_at": event.occurred_at.isoformat(),
                        "synced_at": synced_at.isoformat(),
                        "latency_ms": (synced_at - event.occurred_at).total_seconds() * 1000,
                    }
                )
            )
    # Synchronous acknowledgement reports broker errors instead of silently losing them.
    await asyncio.to_thread(consumer.commit, message=message, asynchronous=False)
