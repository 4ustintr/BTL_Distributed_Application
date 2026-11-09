"""CDC tests use the group's actual Compose stack and tests/fixtures.py configuration."""

from __future__ import annotations

import asyncio
import os
import subprocess
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import TypeVar

import asyncpg
from confluent_kafka import Consumer, TopicPartition
from fixtures import REPO_ROOT, Target, host_ips, integration_target, make_reset, read_env_file

from common.constants import (
    CONSUMER_GROUP,
    ENV_HQ_DB_URL,
    ENV_HQ_URL,
    ENV_KAFKA_BOOTSTRAP,
    ENV_POSTGRES_PASSWORD,
    TOPIC_INVENTORY,
    TOPIC_PARTITIONS,
    DcStatus,
)

T = TypeVar("T")


def target() -> Target:
    configured = integration_target()
    return replace(
        configured,
        hq=os.environ.get(ENV_HQ_URL, configured.hq),
        hq_db=os.environ.get(ENV_HQ_DB_URL, configured.hq_db),
    )


def command(*args: str) -> str:
    result = subprocess.run(
        list(args),
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout


def make(*goals: str) -> None:
    command("make", *goals)


async def reset_stack() -> Target:
    # Mandatory TCs fail if reset/setup fails; there is no private-hostname skip gate.
    await asyncio.to_thread(make_reset)
    configured = target()
    db = await asyncpg.connect(configured.hq_db)
    try:
        # make seed waits for HTTP commits; CDC delivery may still be in flight.
        await wait_for(
            lambda: db.fetchval(
                "SELECT COUNT(*) FROM inventory_snapshot "
                "WHERE product_id = 1 AND dc_id IN (1, 2, 3) AND last_version >= 1"
            ),
            lambda count: count == 3,
        )
    finally:
        await db.close()
    return configured


async def wait_for(
    fetch: Callable[[], Awaitable[T]], done: Callable[[T], bool], seconds: float = 30
) -> T:
    deadline = asyncio.get_running_loop().time() + seconds
    while True:
        value = await fetch()
        if done(value):
            return value
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"Condition not met after {seconds}s; last value: {value!r}")
        await asyncio.sleep(0.2)


async def wait_status(db: asyncpg.Connection, dc_id: int, status: DcStatus) -> None:
    await wait_for(
        lambda: db.fetchval("SELECT status FROM dc_status WHERE dc_id = $1", dc_id),
        lambda value: value == status,
        seconds=60,
    )


def kafka_bootstrap() -> str:
    host_a, _ = host_ips()
    return os.environ.get(ENV_KAFKA_BOOTSTRAP, f"{host_a}:29092")


def kafka_probe() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": kafka_bootstrap(),
            "group.id": CONSUMER_GROUP,
            "enable.auto.commit": False,
        }
    )


async def wait_caught_up(probe: Consumer) -> None:
    metadata = await asyncio.to_thread(probe.list_topics, TOPIC_INVENTORY, timeout=5)
    topic = metadata.topics[TOPIC_INVENTORY]
    assert topic.error is None, topic.error
    assert set(topic.partitions) == set(range(TOPIC_PARTITIONS)), "F8 requires three partitions"
    partitions = [TopicPartition(TOPIC_INVENTORY, index) for index in range(TOPIC_PARTITIONS)]

    async def fetch() -> bool:
        committed = await asyncio.to_thread(probe.committed, partitions, timeout=5)
        ends = await asyncio.gather(
            *(
                asyncio.to_thread(probe.get_watermark_offsets, partition, timeout=5)
                for partition in partitions
            )
        )
        return all(max(0, part.offset) == end[1] for part, end in zip(committed, ends, strict=True))

    await wait_for(fetch, bool)


def node_db_url(dc_id: int) -> str:
    _, host_b = host_ips()
    values = {**read_env_file(REPO_ROOT / os.environ.get("ENV_FILE", ".env")), **os.environ}
    password = values[ENV_POSTGRES_PASSWORD]
    return f"postgresql://postgres:{password}@{host_b}:{5432 + dc_id}/dc{dc_id}"


def reset_offsets() -> None:
    command(
        "docker",
        "exec",
        "kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--group",
        CONSUMER_GROUP,
        "--topic",
        TOPIC_INVENTORY,
        "--reset-offsets",
        "--to-earliest",
        "--execute",
    )
