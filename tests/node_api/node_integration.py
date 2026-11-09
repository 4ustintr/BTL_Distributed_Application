"""Helper cho test integration của node-api: gọi Node API bằng HTTP, đọc CSDL kho bằng asyncpg.

Cần stack đang chạy trên cùng một máy (HOST_A_IP = HOST_B_IP); conftest gốc tự skip khi khác.
Kho dc N: Node API cổng 8000+N, PostgreSQL cổng 5432+N, role node_app (F2, F3).
Chạy `make reset` bằng tay trước khi chạy test; test chỉ dùng số liệu tương đối nên chạy lại được.
"""

from __future__ import annotations

import json
import os
from typing import Any
from uuid import UUID

import asyncpg
import httpx
import pytest
from fixtures import REPO_ROOT, host_ips, read_env_file

from common.constants import ENV_HOST_B_IP, NodeMode

ENV_POSTGRES_PASSWORD = "POSTGRES_PASSWORD"  # biến của compose (.env.example), không phải spec F2


def _host_b() -> str:
    host = host_ips()[1]
    assert host, f"thiếu {ENV_HOST_B_IP} (đặt trong .env hoặc biến môi trường)"
    return host


def _password() -> str:
    pw = os.environ.get(ENV_POSTGRES_PASSWORD) or read_env_file(REPO_ROOT / ".env").get(
        ENV_POSTGRES_PASSWORD
    )
    assert pw, f"thiếu {ENV_POSTGRES_PASSWORD} (đặt trong .env hoặc biến môi trường)"
    return pw


class NodeHarness:
    """Kết nối tới Node API và CSDL của một kho; dùng bằng async with."""

    def __init__(self, dc_id: int) -> None:
        self.dc_id = dc_id
        self.http: httpx.AsyncClient
        self.db: asyncpg.Connection

    async def __aenter__(self) -> NodeHarness:
        host = _host_b()
        self.http = httpx.AsyncClient(base_url=f"http://{host}:{8000 + self.dc_id}", timeout=10)
        dsn = f"postgresql://node_app:{_password()}@{host}:{5432 + self.dc_id}/dc{self.dc_id}"
        self.db = await asyncpg.connect(dsn)
        health = (await self.http.get("/health")).json()
        if health["mode"] != NodeMode.ONLINE:
            pytest.fail(
                f"kho {self.dc_id} đang {health['mode']}, chưa ONLINE: chờ Replay Worker hoặc gọi "
                "tay POST /recovery/complete (kèm X-Internal-Key) rồi chạy lại"
            )
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.http.aclose()
        await self.db.close()

    async def post(self, path: str, body: dict[str, Any]) -> httpx.Response:
        return await self.http.post(path, json={**body, "request_id": str(body["request_id"])})

    async def inventory(self, product_id: int) -> asyncpg.Record:
        row = await self.db.fetchrow(
            "SELECT qty_on_hand, qty_reserved, version FROM inventory "
            "WHERE dc_id = $1 AND product_id = $2",
            self.dc_id,
            product_id,
        )
        assert row is not None, f"kho {self.dc_id} chưa có dòng tồn cho SKU {product_id}"
        return row

    async def movements(self, request_ids: list[UUID]) -> list[asyncpg.Record]:
        return await self.db.fetch(
            "SELECT request_id, movement_type, qty_change, ref_type FROM stock_movement "
            "WHERE request_id = ANY($1::uuid[]) ORDER BY movement_id",
            request_ids,
        )

    async def outbox_events(self, request_ids: list[UUID]) -> list[dict[str, Any]]:
        """Payload các sự kiện outbox ứng với request_id, theo thứ tự phát sinh."""
        rows = await self.db.fetch(
            "SELECT payload FROM outbox_event WHERE payload->>'request_id' = ANY($1::text[]) "
            "ORDER BY created_at, event_id",
            [str(r) for r in request_ids],
        )
        return [json.loads(r["payload"]) for r in rows]
