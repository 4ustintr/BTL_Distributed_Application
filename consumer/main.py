"""Run with python -m consumer.main, or the consumer/Dockerfile image."""

from __future__ import annotations

import asyncio
import logging
import os
import signal

import asyncpg
from confluent_kafka import Consumer, KafkaException, Producer

from common.constants import (
    CONSUMER_GROUP,
    ENV_HQ_DB_URL,
    ENV_KAFKA_BOOTSTRAP,
    TOPIC_INVENTORY,
)
from consumer.processing import InventoryStore, handle_message

log = logging.getLogger("hq-consumer")


async def run() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        # signal.signal also works on Windows, where add_signal_handler is unavailable.
        signal.signal(signum, lambda *_: loop.call_soon_threadsafe(stop.set))
    pool = await asyncpg.create_pool(os.environ[ENV_HQ_DB_URL], min_size=1, max_size=2)
    bootstrap = os.environ[ENV_KAFKA_BOOTSTRAP]
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": CONSUMER_GROUP,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    store = InventoryStore(pool)
    try:
        consumer.subscribe([TOPIC_INVENTORY])
        log.info("Subscribed to %s as %s", TOPIC_INVENTORY, CONSUMER_GROUP)
        while not stop.is_set():
            message = await asyncio.to_thread(consumer.poll, 0.5)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            # A database/broker failure exits without acknowledging this message.
            # The restart policy replays it; no later offset can skip it.
            await handle_message(message, store, consumer, producer)
    finally:
        log.info("Stopping HQ Consumer")
        await asyncio.to_thread(consumer.close)
        await asyncio.to_thread(producer.flush, 10)
        await pool.close()
        log.info("HQ Consumer stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(run())
