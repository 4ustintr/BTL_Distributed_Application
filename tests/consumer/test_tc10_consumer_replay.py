"""TC10 – Consumer nhận lại sự kiện (docs/tests.md mục 4).

Cách chạy: reset offset của hq-consumer về earliest, chạy lại Consumer.
Assert: inventory_snapshot không đổi; số dòng processed_event không đổi.
Spec: F8, db.md 3.4 (bước 2: processed_event chặn trùng; WHERE last_version < version).
"""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
import pytest
from fixtures import docker, integration_target, make_reset

from common.constants import CONSUMER_GROUP, TOPIC_INVENTORY

pytestmark = pytest.mark.integration

CONSUMER_CONTAINER = "hq-consumer"  # F2
KAFKA_CONTAINER = "kafka"  # F2
KAFKA_GROUPS = "/opt/kafka/bin/kafka-consumer-groups.sh"
SEED_STOCK = {1: 100, 2: 40, 3: 10}  # tests.md mục 2: tồn ban đầu Bắc, Trung, Nam
WAIT_S = 60.0

SQL_SNAPSHOT = """SELECT dc_id, product_id, qty_on_hand, qty_reserved, last_version, last_event_id
                  FROM inventory_snapshot ORDER BY dc_id, product_id"""


async def wait_for(fetch: Any, done: Any, what: str) -> Any:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WAIT_S
    while True:
        value = await fetch()
        if done(value):
            return value
        if loop.time() >= deadline:
            raise AssertionError(f"hết {WAIT_S:.0f} giây chờ {what}; giá trị cuối: {value!r}")
        await asyncio.sleep(1)


def kafka_groups(*args: str) -> str:
    return docker(
        "exec", KAFKA_CONTAINER, KAFKA_GROUPS, "--bootstrap-server", "kafka:9092",
        "--group", CONSUMER_GROUP, *args,
    )  # fmt: skip


def lag_of_inventory_topic() -> list[str]:
    """Cột LAG của từng partition outbox.event.INVENTORY trong group hq-consumer."""
    lags = []
    for line in kafka_groups("--describe").splitlines():
        cols = line.split()
        if len(cols) >= 6 and cols[1] == TOPIC_INVENTORY:
            lags.append(cols[5])  # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
    return lags


async def test_tc10_consumer_replays_events_without_effect() -> None:
    make_reset()
    t = integration_target()
    async with asyncpg.create_pool(t.hq_db, min_size=1, max_size=1) as db:
        # Seed đã nhập tồn qua API; chờ CDC đưa đủ 3 dòng về snapshot
        before = await wait_for(
            lambda: db.fetch(SQL_SNAPSHOT),
            lambda rows: {r["dc_id"]: r["qty_on_hand"] for r in rows} == SEED_STOCK,
            "snapshot Bắc 100, Trung 40, Nam 10",
        )
        processed_before = await db.fetchval("SELECT count(*) FROM processed_event")
        assert processed_before >= len(SEED_STOCK)

        # Group phải không còn thành viên thì mới đặt lại offset được
        docker("stop", CONSUMER_CONTAINER)
        try:
            out = kafka_groups(
                "--reset-offsets", "--to-earliest", "--topic", TOPIC_INVENTORY, "--execute"
            )
            assert TOPIC_INVENTORY in out, out
        finally:
            docker("start", CONSUMER_CONTAINER)

        # Consumer đọc lại toàn bộ topic từ đầu: chờ hết lag
        lags = await wait_for(
            lambda: asyncio.to_thread(lag_of_inventory_topic),
            lambda values: bool(values) and all(v == "0" for v in values),
            "hq-consumer đọc lại hết topic (LAG = 0)",
        )
        assert lags

        assert await db.fetch(SQL_SNAPSHOT) == before
        assert await db.fetchval("SELECT count(*) FROM processed_event") == processed_before
