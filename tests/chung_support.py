"""Fixtures only for the explicitly isolated btl-chung-tests stack."""

import os
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest

from common.constants import EventType, RefType
from common.models import InventoryEventPayload

TEST_DSN = "postgresql://postgres:chungtest@chung-hq:5432/hq_db"


async def seed_catalog(connection):
    await connection.execute("""
        INSERT INTO dc (dc_id, dc_code, dc_name, region, fdw_server) VALUES
          (1, 'DC-BAC', 'Bắc', 'BAC', 'srv_dc1'),
          (2, 'DC-TRUNG', 'Trung', 'TRUNG', 'srv_dc2'),
          (3, 'DC-NAM', 'Nam', 'NAM', 'srv_dc3');
        INSERT INTO product_category (category_id, category_code, category_name)
          VALUES (1, 'CAT-BEV', 'Đồ uống');
        INSERT INTO product (product_id, sku, product_name, category_id, unit)
          VALUES (1, 'SKU-A', 'Nước suối', 1, 'thùng');
    """)


@pytest.fixture(name="cdc_db")
async def cdc_db():
    if "chung-hq" not in os.environ.get("HQ_DB_URL", ""):
        pytest.skip("Requires tests/consumer/compose.integration.yml (isolated databases)")
    admin = await asyncpg.connect(TEST_DSN)
    try:
        await admin.execute("TRUNCATE dc, product_category CASCADE")
        await admin.execute("TRUNCATE processed_event")
        await seed_catalog(admin)
        await admin.execute("""
            INSERT INTO dc_status (dc_id, status, node_mode)
            SELECT dc_id, 'ONLINE', 'ONLINE' FROM dc;
            INSERT INTO inventory_snapshot (dc_id, product_id, qty_on_hand)
            VALUES (1, 1, 100), (2, 1, 40), (3, 1, 10);
        """)
        await admin.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        for dc_id, qty in ((1, 100), (2, 40), (3, 10)):
            node = await asyncpg.connect(
                f"postgresql://postgres:chungtest@chung-dc{dc_id}:5432/dc{dc_id}"
            )
            try:
                await node.execute("TRUNCATE dc, product_category CASCADE")
                await node.execute("TRUNCATE outbox_event")
                await seed_catalog(node)
                await node.execute(
                    "INSERT INTO inventory (dc_id, product_id, qty_on_hand) VALUES ($1, 1, $2)",
                    dc_id,
                    qty,
                )
                await node.execute(
                    """INSERT INTO stock_movement
                       (request_id, dc_id, product_id, movement_type, qty_change, ref_type)
                       VALUES ($1, $2, 1, 'INBOUND', $3, 'RECEIPT')""",
                    UUID(os.environ[f"REQ_SEED_{dc_id}"]),
                    dc_id,
                    qty,
                )
            finally:
                await node.close()
            await admin.execute(f"DROP SERVER IF EXISTS srv_dc{dc_id} CASCADE")
            await admin.execute(f"""
                DROP SCHEMA IF EXISTS dc{dc_id} CASCADE;
                CREATE SERVER srv_dc{dc_id} FOREIGN DATA WRAPPER postgres_fdw
                  OPTIONS (host 'chung-dc{dc_id}', port '5432', dbname 'dc{dc_id}',
                           connect_timeout '3', keep_connections 'off');
                CREATE USER MAPPING FOR hq_app SERVER srv_dc{dc_id}
                  OPTIONS (user 'fdw_reader', password 'chungtest');
                CREATE SCHEMA dc{dc_id};
                CREATE USER MAPPING FOR postgres SERVER srv_dc{dc_id}
                  OPTIONS (user 'fdw_reader', password 'chungtest');
                IMPORT FOREIGN SCHEMA public LIMIT TO (inventory, stock_movement)
                  FROM SERVER srv_dc{dc_id} INTO dc{dc_id};
                GRANT USAGE ON SCHEMA dc{dc_id} TO hq_app;
                GRANT SELECT ON ALL TABLES IN SCHEMA dc{dc_id} TO hq_app;
                GRANT USAGE ON FOREIGN SERVER srv_dc{dc_id} TO hq_app;
            """)
        pool = await asyncpg.create_pool(os.environ["HQ_DB_URL"], min_size=0, max_size=10)
        try:
            yield pool, admin
        finally:
            await pool.close()
    finally:
        await admin.close()


def make_event(demo_ids, *, version=1, qty=35, request_key="REQ_KB2_OUT"):
    return InventoryEventPayload(
        event_id=UUID(demo_ids[request_key]),
        event_type=EventType.INVENTORY_CHANGED,
        source_dc=2,
        product_id=1,
        qty_on_hand=qty,
        qty_reserved=0,
        version=version,
        occurred_at=datetime.now(UTC),
        request_id=UUID(demo_ids[request_key]),
        ref_type=RefType.ISSUE,
    )
