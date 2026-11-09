"""Consumer (db.md 3.4): chỉ commit offset sau khi CSDL commit; sự kiện sai dạng đi DLQ."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest
from confluent_kafka import KafkaError

from common.constants import TOPIC_INVENTORY, EventType, RefType
from common.models import InventoryEventPayload
from consumer.main import is_transient_poll_error
from consumer.processing import handle_message, parse_event


def event_data(demo_ids):
    return {
        "event_id": demo_ids["REQ_SEED_2"],
        "event_type": EventType.INVENTORY_CHANGED,
        "source_dc": 2,
        "occurred_at": datetime.now(UTC).isoformat(),
        "product_id": 1,
        "qty_on_hand": 40,
        "qty_reserved": 0,
        "version": 1,
        "request_id": demo_ids["REQ_SEED_2"],
        "ref_type": RefType.RECEIPT,
    }


def message_for(data):
    event = InventoryEventPayload.model_validate(data)
    return Mock(
        value=Mock(return_value=event.model_dump_json().encode()),
        key=Mock(return_value=b"2:1"),
        topic=Mock(return_value=TOPIC_INVENTORY),
        headers=Mock(return_value=None),
    )


async def test_commit_only_after_database(demo_ids):
    order = []
    store = Mock()

    async def apply(*args):
        order.append("database_committed")

    store.apply = AsyncMock(side_effect=apply)
    kafka = Mock()
    kafka.commit.side_effect = lambda **kwargs: order.append("offset_committed")
    await handle_message(message_for(event_data(demo_ids)), store, kafka, Mock())
    assert order == ["database_committed", "offset_committed"]
    assert kafka.commit.call_args.kwargs["asynchronous"] is False


async def test_database_failure_never_acknowledged(demo_ids):
    store = Mock(apply=AsyncMock(side_effect=ConnectionError("database unavailable")))
    kafka = Mock()
    with pytest.raises(ConnectionError):
        await handle_message(message_for(event_data(demo_ids)), store, kafka, Mock())
    kafka.commit.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", 0),
        ("version", 2**63),
        ("product_id", 2**63),
        ("qty_on_hand", 2**31),
        ("source_dc", 4),
        ("qty_on_hand", -1),
        ("qty_reserved", 41),
        ("occurred_at", "2026-11-08T10:00:00"),
    ],
)
def test_invalid_payload_is_permanent_error(demo_ids, field, value):
    data = event_data(demo_ids)
    data[field] = value
    with pytest.raises(ValueError):
        parse_event(message_for(data))


def test_wrong_partition_key_rejected(demo_ids):
    message = message_for(event_data(demo_ids))
    message.key.return_value = b"1:1"
    with pytest.raises(ValueError):
        parse_event(message)


async def test_dlq_failure_does_not_acknowledge():
    message = Mock(value=Mock(return_value=b"bad-json"), key=Mock(return_value=b"2:1"))
    producer = Mock()
    producer.flush.return_value = 1
    kafka = Mock()
    with pytest.raises(RuntimeError):
        await handle_message(message, Mock(), kafka, producer)
    kafka.commit.assert_not_called()


async def test_dlq_delivery_before_acknowledgement():
    message = Mock(value=Mock(return_value=b"bad-json"), key=Mock(return_value=b"2:1"))
    order = []
    producer = Mock()

    def produce(*args, **kwargs):
        order.append("dlq_sent")
        kwargs["on_delivery"](None, Mock())

    producer.produce.side_effect = produce
    producer.flush.return_value = 0
    kafka = Mock()
    kafka.commit.side_effect = lambda **kwargs: order.append("offset_committed")
    await handle_message(message, Mock(), kafka, producer)
    assert order == ["dlq_sent", "offset_committed"]


@pytest.mark.parametrize(
    ("code", "waits"),
    [
        (KafkaError.UNKNOWN_TOPIC_OR_PART, True),  # topic chưa có: Debezium chưa phát sự kiện nào
        (KafkaError._UNKNOWN_TOPIC, True),
        (KafkaError._PARTITION_EOF, True),
        (KafkaError._ALL_BROKERS_DOWN, True),
    ],
)
def test_poll_error_topic_not_created_yet_is_not_fatal(code, waits):
    # Sau make reset topic chỉ được tạo khi có sự kiện đầu tiên; Consumer phải chờ, không thoát
    assert is_transient_poll_error(KafkaError(code)) is waits
