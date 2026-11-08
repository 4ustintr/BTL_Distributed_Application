"""Tiện ích cho test tích hợp TC14, TC15, TC16 (Nhật Khánh; docs/tests.md mục 4).

Địa chỉ theo spec F2, lấy HOST_A_IP, HOST_B_IP, POSTGRES_PASSWORD từ .env ở gốc repo.

GIẢ ĐỊNH: lệnh make của máy B (kill-dc2, start-dc2) chạy bằng docker trên chính máy chạy pytest,
tức HQ và kho đặt chung một máy, hoặc docker CLI đã trỏ sẵn sang máy B. Cách điều khiển máy B từ
máy A khi chạy hai máy thật cần trưởng nhóm chốt (tests/conftest.py thuộc Minh Danh).
"""

from __future__ import annotations

import asyncio
import os
import subprocess
from dataclasses import dataclass
from typing import Any

import asyncpg
import httpx
from fixtures import REPO_ROOT, SKU_A_PRODUCT_ID, load_demo_ids

from common.constants import ENV_HOST_A_IP, ENV_HOST_B_IP, DcStatus, PendingTxnStatus

HQ_PORT = 8000  # F2
NODE_PORTS = {1: 8001, 2: 8002, 3: 8003}  # F2
HQ_DB_PORT = 5432  # F2
ENV_POSTGRES_PASSWORD = "POSTGRES_PASSWORD"  # .env.example
HQ_CONSUMER_CONTAINER = "hq-consumer"  # F2
POLL_S = 0.5


@dataclass(frozen=True)
class Target:
    hq: str
    nodes: dict[int, str]
    hq_db: str


def target() -> Target:
    env = {**load_demo_ids(REPO_ROOT / ".env"), **os.environ}
    host_a, host_b = env[ENV_HOST_A_IP], env[ENV_HOST_B_IP]
    return Target(
        hq=f"http://{host_a}:{HQ_PORT}",
        nodes={dc: f"http://{host_b}:{port}" for dc, port in NODE_PORTS.items()},
        hq_db=f"postgresql://hq_app:{env[ENV_POSTGRES_PASSWORD]}@{host_a}:{HQ_DB_PORT}/hq_db",
    )


def make(*targets: str) -> None:
    subprocess.run(["make", *targets], cwd=REPO_ROOT, check=True)


def make_reset() -> None:
    make("reset")


def docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True)


async def _wait(fetch: Any, done: Any, timeout_s: float, what: str) -> Any:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while True:
        value = await fetch()
        if done(value):
            return value
        if loop.time() >= deadline:
            raise AssertionError(f"Hết {timeout_s:.0f} giây chờ {what}; giá trị cuối: {value!r}")
        await asyncio.sleep(POLL_S)


async def dc_status(t: Target, dc_id: int) -> str | None:
    conn = await asyncpg.connect(t.hq_db)
    try:
        return await conn.fetchval("SELECT status FROM dc_status WHERE dc_id = $1", dc_id)
    finally:
        await conn.close()


async def wait_dc_status(t: Target, dc_id: int, status: DcStatus, timeout_s: float = 60) -> None:
    await _wait(
        lambda: dc_status(t, dc_id),
        lambda s: s == status,
        timeout_s,
        f"dc_status kho {dc_id} = {status}",
    )


async def pending_rows(t: Target, dc_id: int) -> list[dict[str, Any]]:
    conn = await asyncpg.connect(t.hq_db)
    try:
        rows = await conn.fetch(
            "SELECT txn_id, txn_type, qty, status, error_msg FROM pending_txn WHERE dc_id = $1",
            dc_id,
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def count_pending(t: Target) -> int:
    conn = await asyncpg.connect(t.hq_db)
    try:
        return await conn.fetchval("SELECT count(*) FROM pending_txn")
    finally:
        await conn.close()


async def wait_txn_status(
    t: Target, dc_id: int, txn_id: str, status: PendingTxnStatus, timeout_s: float = 90
) -> dict[str, Any]:
    async def fetch() -> dict[str, Any] | None:
        return next((r for r in await pending_rows(t, dc_id) if str(r["txn_id"]) == txn_id), None)

    return await _wait(
        fetch,
        lambda r: r is not None and r["status"] == status,
        timeout_s,
        f"pending_txn {txn_id} = {status}",
    )


async def node_stock(t: Target, dc_id: int) -> tuple[int, int]:
    """(tồn, khả dụng) đọc từ GET /inventory của kho (F6)."""
    async with httpx.AsyncClient(timeout=5) as http:
        r = await http.get(f"{t.nodes[dc_id]}/inventory", params={"product_id": SKU_A_PRODUCT_ID})
    r.raise_for_status()
    item = r.json()["items"][0]
    return item["qty_on_hand"], item["available"]


async def snapshot_stock(t: Target, dc_id: int) -> tuple[int, int] | None:
    conn = await asyncpg.connect(t.hq_db)
    try:
        row = await conn.fetchrow(
            """SELECT qty_on_hand, qty_reserved FROM inventory_snapshot
               WHERE dc_id = $1 AND product_id = $2""",
            dc_id,
            SKU_A_PRODUCT_ID,
        )
        return (
            None if row is None else (row["qty_on_hand"], row["qty_on_hand"] - row["qty_reserved"])
        )
    finally:
        await conn.close()


async def wait_snapshot(
    t: Target, dc_id: int, expected: tuple[int, int], timeout_s: float = 30
) -> None:
    await _wait(
        lambda: snapshot_stock(t, dc_id),
        lambda s: s == expected,
        timeout_s,
        f"snapshot kho {dc_id} = {expected}",
    )
