"""Tiện ích chung cho test Saga điều chuyển TC05, TC07 (docs/tests.md mục 4).

Địa chỉ theo spec F2: HQ API và hq_db ở HOST_A_IP, Node API ở HOST_B_IP, lấy từ .env ở gốc repo.
Tồn kho và transfer_order ở kho đọc qua postgres_fdw (schema dc1..dc3, spec F3), nên test chỉ
cần một kết nối tới hq_db bằng role hq_app.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
from fixtures import REPO_ROOT, load_demo_ids

from common.constants import DC_IDS, ENV_HOST_A_IP, ENV_HOST_B_IP, TransferStatus

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS
HQ_PORT = 8000
NODE_PORTS = {DC_BAC: 8001, DC_TRUNG: 8002, DC_NAM: 8003}
HQ_DB_PORT = 5432
ENV_POSTGRES_PASSWORD = "POSTGRES_PASSWORD"
WAIT_S = 10.0


@dataclass(frozen=True)
class Target:
    hq: str
    nodes: dict[int, str]
    hq_db: str


def target() -> Target:
    env = {**load_demo_ids(REPO_ROOT / ".env"), **os.environ}
    host_a, host_b = env[ENV_HOST_A_IP], env[ENV_HOST_B_IP]
    password = env[ENV_POSTGRES_PASSWORD]
    return Target(
        hq=f"http://{host_a}:{HQ_PORT}",
        nodes={dc: f"http://{host_b}:{port}" for dc, port in NODE_PORTS.items()},
        hq_db=f"postgresql://hq_app:{password}@{host_a}:{HQ_DB_PORT}/hq_db",
    )


def make_reset() -> None:
    subprocess.run(["make", "reset"], cwd=REPO_ROOT, check=True)


async def stock(db: asyncpg.Pool, dc_id: int, product_id: int) -> tuple[int, int]:
    row = await db.fetchrow(
        f"SELECT qty_on_hand, qty_reserved FROM dc{dc_id}.inventory "
        "WHERE dc_id = $1 AND product_id = $2",
        dc_id,
        product_id,
    )
    assert row is not None, f"kho {dc_id} chưa có dòng inventory cho SKU {product_id}"
    return row["qty_on_hand"], row["qty_on_hand"] - row["qty_reserved"]


async def wait_order(
    db: asyncpg.Pool, dc_id: int, transfer_id: UUID, status: TransferStatus
) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WAIT_S
    while True:
        row = await db.fetchrow(
            f"SELECT status, cancel_reason FROM dc{dc_id}.transfer_order WHERE transfer_id = $1",
            transfer_id,
        )
        found = dict(row) if row else {}
        if found.get("status") == status or loop.time() >= deadline:
            return found
        await asyncio.sleep(0.5)
