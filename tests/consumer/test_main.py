"""Startup/poll errors must not restart a healthy Consumer or acknowledge unread data."""

from unittest.mock import AsyncMock, Mock

import pytest
from confluent_kafka import KafkaError, KafkaException

from consumer.main import poll_once


@pytest.mark.parametrize(
    "code",
    [
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError._UNKNOWN_TOPIC,
        KafkaError._TRANSPORT,
        KafkaError._ALL_BROKERS_DOWN,
    ],
)
@pytest.mark.parametrize("raised", [False, True])
async def test_temporary_poll_errors_are_not_fatal(code, raised):
    error = KafkaError(code)
    kafka = Mock()
    if raised:
        kafka.poll.side_effect = KafkaException(error)
    else:
        kafka.poll.return_value = Mock(error=Mock(return_value=error))
    store = Mock(apply=AsyncMock())
    producer = Mock()
    assert await poll_once(kafka, producer, store) is False
    kafka.commit.assert_not_called()
    store.apply.assert_not_called()
    producer.produce.assert_not_called()


async def test_permanent_poll_failure_is_not_hidden():
    kafka = Mock()
    kafka.poll.return_value = Mock(
        error=Mock(
            return_value=KafkaError(
                KafkaError.TOPIC_AUTHORIZATION_FAILED,
            )
        )
    )
    with pytest.raises(KafkaException):
        await poll_once(kafka, Mock(), Mock())


async def test_valid_record_after_missing_topic_is_processed(monkeypatch):
    missing = Mock(error=Mock(return_value=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)))
    valid = Mock(error=Mock(return_value=None))
    kafka = Mock(poll=Mock(side_effect=[missing, valid]))
    handler = AsyncMock()
    monkeypatch.setattr("consumer.main.handle_message", handler)
    store, producer = Mock(), Mock()
    assert await poll_once(kafka, producer, store) is False
    assert await poll_once(kafka, producer, store) is True
    handler.assert_awaited_once_with(valid, store, kafka, producer)
