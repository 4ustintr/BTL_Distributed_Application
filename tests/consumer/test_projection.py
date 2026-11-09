"""Projection transaction boundaries, including stale replays and database failures."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from common.models import InventoryEventPayload
from consumer.processing import InventoryStore


def setup_store(event):
    connection = Mock()
    connection.fetchval = AsyncMock(side_effect=[event.event_id, datetime.now(UTC)])
    connection.execute = AsyncMock()
    transaction = AsyncMock()
    transaction.__aexit__.return_value = False
    connection.transaction.return_value = transaction
    acquired = AsyncMock()
    acquired.__aenter__.return_value = connection
    acquired.__aexit__.return_value = False
    pool = Mock()
    pool.acquire.return_value = acquired
    return InventoryStore(pool), connection, transaction


def event_for(demo_ids):
    return InventoryEventPayload.model_validate(
        {
            "event_id": demo_ids["REQ_KB3_IN"],
            "event_type": "InventoryChanged",
            "source_dc": 2,
            "product_id": 1,
            "qty_on_hand": 65,
            "qty_reserved": 0,
            "version": 8,
            "occurred_at": datetime.now(UTC),
            "request_id": demo_ids["REQ_KB3_IN"],
        }
    )


async def test_duplicate_does_not_write_snapshot_or_pending(demo_ids):
    event = event_for(demo_ids)
    store, connection, transaction = setup_store(event)
    connection.fetchval.side_effect = [None]
    assert await store.apply(event, b"2:1") is None
    assert connection.fetchval.await_count == 1
    connection.execute.assert_not_awaited()
    transaction.__aexit__.assert_awaited_once_with(None, None, None)


async def test_stale_replay_still_applies_pending_in_same_transaction(demo_ids):
    event = event_for(demo_ids)
    store, connection, transaction = setup_store(event)
    connection.fetchval.side_effect = [event.event_id, None]
    assert await store.apply(event, b"2:1") is None
    assert connection.execute.call_args.args[1:] == (event.request_id, "APPLIED", "PENDING")
    transaction.__aexit__.assert_awaited_once_with(None, None, None)


async def test_projection_failure_exits_transaction_with_error(demo_ids):
    event = event_for(demo_ids)
    store, connection, transaction = setup_store(event)
    connection.fetchval.side_effect = [event.event_id, ConnectionError("db failed")]
    with pytest.raises(ConnectionError):
        await store.apply(event, b"2:1")
    assert transaction.__aexit__.call_args.args[0] is ConnectionError
    connection.execute.assert_not_awaited()
