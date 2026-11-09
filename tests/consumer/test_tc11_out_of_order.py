"""TC11 (tùy chọn) – Sự kiện đến sai thứ tự (docs/tests.md mục 4).

Cách chạy: gửi vào topic một sự kiện có version nhỏ hơn last_version.
Assert: inventory_snapshot không đổi (sự kiện vẫn được ghi nhận trong processed_event).
Kho Bắc vừa seed có version 1 (dòng inventory mới); xuất 1 thùng để lên version 2, rồi gửi sự kiện
version 1 với số tồn khác hẳn.
Spec: F8, db.md 3.4 (WHERE s.last_version < EXCLUDED.last_version; không so theo thời gian).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import asyncpg
import httpx
import pytest
from confluent_kafka import Producer
from fixtures import SKU_A_PRODUCT_ID, host_ips, integration_target, make_reset

from common.constants import DC_IDS, TOPIC_INVENTORY, EventType, RefType
from common.ids import inventory_aggregate_id, new_request_id
from common.models import InventoryEventPayload, OutboundRequest

pytestmark = [pytest.mark.integration, pytest.mark.optional]

DC_BAC = DC_IDS[0]  # db.md 2.1
KAFKA_EXTERNAL_PORT = 29092  # F2: listener cho công cụ chạy trên máy host
WAIT_S = 30.0

SQL_ROW = """SELECT qty_on_hand, qty_reserved, last_version, last_event_id
             FROM inventory_snapshot WHERE dc_id = $1 AND product_id = $2"""


async def wait_for(fetch: Any, done: Any, what: str) -> Any:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WAIT_S
    while True:
        value = await fetch()
        if done(value):
            return value
        if loop.time() >= deadline:
            raise AssertionError(f"hết {WAIT_S:.0f} giây chờ {what}; giá trị cuối: {value!r}")
        await asyncio.sleep(0.5)


async def test_tc11_older_event_does_not_overwrite_snapshot() -> None:
    make_reset()
    t = integration_target()
    host_a, _ = host_ips()
    async with (
        asyncpg.create_pool(t.hq_db, min_size=1, max_size=1) as db,
        httpx.AsyncClient(timeout=10) as http,
    ):
        out = OutboundRequest(
            request_id=new_request_id(),
            product_id=SKU_A_PRODUCT_ID,
            qty=1,
            ref_type=RefType.ISSUE,
            created_by="tc11",
        )
        r = await http.post(f"{t.nodes[DC_BAC]}/outbound", json=out.model_dump(mode="json"))
        assert r.status_code == 200, r.text
        current = await wait_for(
            lambda: db.fetchrow(SQL_ROW, DC_BAC, SKU_A_PRODUCT_ID),
            lambda row: row is not None and row["last_version"] >= 2,
            "snapshot Bắc lên version 2",
        )

        stale = InventoryEventPayload(
            event_id=new_request_id(),
            event_type=EventType.INVENTORY_CHANGED,
            source_dc=DC_BAC,
            occurred_at=datetime.now(UTC),
            product_id=SKU_A_PRODUCT_ID,
            qty_on_hand=999,
            qty_reserved=0,
            version=current["last_version"] - 1,
        )
        producer = Producer({"bootstrap.servers": f"{host_a}:{KAFKA_EXTERNAL_PORT}"})
        producer.produce(
            TOPIC_INVENTORY,
            key=inventory_aggregate_id(DC_BAC, SKU_A_PRODUCT_ID),
            value=stale.model_dump_json(),
        )
        assert producer.flush(10) == 0

        # Consumer đã xử lý sự kiện (ghi processed_event) nhưng không ghi đè số mới bằng số cũ
        await wait_for(
            lambda: db.fetchval(
                "SELECT 1 FROM processed_event WHERE event_id = $1", stale.event_id
            ),
            lambda found: found == 1,
            "Consumer xử lý sự kiện version cũ",
        )
        assert await db.fetchrow(SQL_ROW, DC_BAC, SKU_A_PRODUCT_ID) == current
