"""TC10 with real Kafka offset reset; real Debezium/WAL delivery into production Consumer."""

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from time import monotonic
from uuid import uuid5

import asyncpg
import httpx
import pytest
from chung_support import cdc_db as isolated_database  # noqa: F401
from chung_support import make_event
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from common.constants import CONSUMER_GROUP, TOPIC_DLQ, TOPIC_INVENTORY
from consumer.benchmark import summarize

pytestmark = pytest.mark.integration
CONNECTOR_PATH = Path(__file__).resolve().parents[2] / "infra/connectors/dc2-outbox.json"


async def eventually(check):
    deadline = monotonic() + 30
    while monotonic() < deadline:
        value = await check()
        if value:
            return value
        await asyncio.sleep(0.1)
    raise AssertionError("Condition not met before integration timeout")


@asynccontextmanager
async def running_consumer():
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "consumer.main",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    log_lines = []
    reader = asyncio.create_task(process.stdout.read())
    try:
        yield log_lines
    finally:
        if process.returncode is None:
            process.terminate()
        await asyncio.wait_for(process.wait(), 20)
        output = await reader
        log_lines.extend(output.decode().splitlines())
        assert process.returncode == 0, output.decode()


async def prepare_topics():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "")
    assert bootstrap == "chung-kafka:9092", "Never reset topics on the shared demo cluster"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    # Production topic names, isolated broker; repeated runs start from clean offsets.
    topics = [TOPIC_INVENTORY, TOPIC_DLQ]
    existing = await asyncio.to_thread(admin.list_topics, timeout=10)
    to_delete = [topic for topic in topics if topic in existing.topics]
    if to_delete:
        for future in admin.delete_topics(to_delete).values():
            await asyncio.to_thread(future.result, 15)
        for _ in range(100):
            metadata = await asyncio.to_thread(admin.list_topics, timeout=10)
            if not any(topic in metadata.topics for topic in to_delete):
                break
            await asyncio.sleep(0.1)
    for future in admin.create_topics([NewTopic(topic, 3, 1) for topic in topics]).values():
        await asyncio.to_thread(future.result, 15)
    resetter = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": CONSUMER_GROUP,
            "enable.auto.commit": False,
        }
    )
    try:
        await asyncio.to_thread(
            resetter.commit,
            offsets=[TopicPartition(TOPIC_INVENTORY, index, 0) for index in range(3)],
            asynchronous=False,
        )
    finally:
        await asyncio.to_thread(resetter.close)


def publish(event):
    producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]})
    producer.produce(TOPIC_INVENTORY, key="2:1", value=event.model_dump_json())
    assert producer.flush(10) == 0


async def test_tc10_offset_reset_and_dlq_with_real_kafka(cdc_db, demo_ids):
    _, admin = cdc_db
    await prepare_topics()
    event = make_event(demo_ids, version=8, qty=35)
    await asyncio.to_thread(publish, event)
    probe = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
            "group.id": CONSUMER_GROUP,
            "enable.auto.commit": False,
        }
    )
    partitions = [TopicPartition(TOPIC_INVENTORY, index) for index in range(3)]

    async def caught_up():
        committed = await asyncio.to_thread(probe.committed, partitions, timeout=5)
        ends = await asyncio.gather(
            *(
                asyncio.to_thread(probe.get_watermark_offsets, part, timeout=5)
                for part in partitions
            )
        )
        return all(max(0, part.offset) == end[1] for part, end in zip(committed, ends, strict=True))

    try:
        async with running_consumer():
            await eventually(lambda: admin.fetchval("SELECT COUNT(*) = 1 FROM processed_event"))
            await eventually(caught_up)
        before = await admin.fetch("SELECT * FROM inventory_snapshot ORDER BY dc_id")
        await asyncio.to_thread(
            probe.commit,
            offsets=[TopicPartition(TOPIC_INVENTORY, index, 0) for index in range(3)],
            asynchronous=False,
        )
        async with running_consumer():
            await eventually(caught_up)
            assert await admin.fetchval("SELECT COUNT(*) FROM processed_event") == 1
            assert await admin.fetch("SELECT * FROM inventory_snapshot ORDER BY dc_id") == before
            producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]})
            producer.produce(TOPIC_INVENTORY, key="2:1", value="invalid-json")
            producer.produce(
                TOPIC_INVENTORY,
                key="2:1",
                value=make_event(
                    demo_ids, version=9, qty=30, request_key="REQ_KB3_OUT"
                ).model_dump_json(),
            )
            await asyncio.to_thread(producer.flush, 10)
            await eventually(
                lambda: admin.fetchval(
                    "SELECT qty_on_hand = 30 FROM inventory_snapshot WHERE dc_id = 2"
                )
            )
            await eventually(caught_up)
        dlq = Consumer(
            {
                "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
                "group.id": CONSUMER_GROUP,
                "enable.auto.commit": False,
            }
        )
        try:
            dlq.assign([TopicPartition(TOPIC_DLQ, index, 0) for index in range(3)])
            message = await asyncio.to_thread(dlq.poll, 10)
            assert message is not None and not message.error()
            assert message.key() == b"2:1" and message.value() == b"invalid-json"
        finally:
            await asyncio.to_thread(dlq.close)
    finally:
        await asyncio.to_thread(probe.close)


async def test_tc10_debezium_outbox_wal_to_snapshot(cdc_db, demo_ids, tmp_path):
    _, admin = cdc_db
    await prepare_topics()
    connector = json.loads(CONNECTOR_PATH.read_text())
    connector["config"].update(
        {
            "database.hostname": "chung-dc2",
            "database.port": "5432",
            "database.password": "chungtest",
        }
    )
    async with httpx.AsyncClient(base_url="http://chung-connect:8083", timeout=10) as client:
        await client.delete("/connectors/dc2-outbox")
        response = await client.post("/connectors", json=connector)
        assert response.status_code == 201, response.text
        try:

            async def connector_running():
                state = (await client.get("/connectors/dc2-outbox/status")).json()
                return state.get("tasks") and all(
                    task["state"] == "RUNNING" for task in state["tasks"]
                )

            await eventually(connector_running)
            event = make_event(demo_ids, version=2, qty=35)
            node = await asyncpg.connect("postgresql://postgres:chungtest@chung-dc2:5432/dc2")
            try:
                sample_ids = []
                async with running_consumer() as log_lines:
                    async with node.transaction():
                        await node.execute("SET LOCAL app.source = 'api'")
                        await node.execute(
                            "UPDATE inventory SET qty_on_hand = 35 WHERE product_id = 1"
                        )
                        await node.execute(
                            """INSERT INTO outbox_event
                               (event_id, aggregate_type, aggregate_id, event_type, payload)
                               VALUES ($1, 'INVENTORY', '2:1', $2, $3::jsonb)""",
                            event.event_id,
                            event.event_type,
                            event.model_dump_json(),
                        )
                    await eventually(
                        lambda: admin.fetchval(
                            "SELECT qty_on_hand = 35 AND last_version = 2 "
                            "FROM inventory_snapshot WHERE dc_id = 2"
                        )
                    )
                    # M1 local workload: 200 source transactions, 100 ms apart.
                    # Node API is outside this owner's scope; SQL is a test-only emitter.
                    await node.execute(
                        "UPDATE inventory SET qty_on_hand = 235 WHERE product_id = 1"
                    )
                    for index in range(200):
                        request_id = uuid5(event.event_id, f"M1:{index}")
                        sample_ids.append(str(request_id))
                        async with node.transaction():
                            await node.execute("SET LOCAL app.source = 'api'")
                            stock = await node.fetchrow(
                                """UPDATE inventory SET qty_on_hand = qty_on_hand - 1
                                   WHERE product_id = 1 AND qty_on_hand >= 1
                                   RETURNING qty_on_hand, version,
                                             clock_timestamp() AS occurred_at"""
                            )
                            sample = event.model_copy(
                                update={
                                    "event_id": request_id,
                                    "request_id": request_id,
                                    "qty_on_hand": stock["qty_on_hand"],
                                    "version": stock["version"],
                                    "occurred_at": stock["occurred_at"],
                                }
                            )
                            await node.execute(
                                """INSERT INTO outbox_event
                                   (event_id, aggregate_type, aggregate_id, event_type, payload)
                                   VALUES ($1, 'INVENTORY', '2:1', $2, $3::jsonb)""",
                                sample.event_id,
                                sample.event_type,
                                sample.model_dump_json(),
                            )
                        await asyncio.sleep(0.1)
                    await eventually(
                        lambda: admin.fetchval("SELECT COUNT(*) = 201 FROM processed_event")
                    )
                result = summarize(sample_ids, log_lines)
                result["environment"] = (
                    "Single Docker host; PostgreSQL 16 / Debezium 2.7 / Kafka 3.8"
                )
                result["source"] = "Test-only SQL outbox emitter; excludes Node API HTTP path"
                (tmp_path / "M1-local.json").write_text(json.dumps(result, indent=2))
                print("M1 local:", json.dumps(result))
            finally:
                await node.close()
        finally:
            await client.delete("/connectors/dc2-outbox")
