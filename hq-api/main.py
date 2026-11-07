"""HQ API (spec F2, F5, F7), kèm Health Monitor và Replay Worker chạy nền.

Chạy: uvicorn main:app.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import dashboard, failover, invariant, transfers
from workers import health, replay

from common.constants import (
    CORS_ORIGIN_REGEX,
    ENV_HQ_DB_URL,
    ENV_INTERNAL_KEY,
    ENV_NODE_URLS,
    HQ_HEALTH_OK,
)
from common.errors import install_error_handlers
from common.http import InternalClient, internal_key_guard
from common.models import HqHealthResponse

log = logging.getLogger("hq-api")

# Endpoint do Node API gọi (cột "Gọi bởi" ở F7) -> bắt buộc X-Internal-Key.
# /transfers/{id}/decline không nằm đây vì Dashboard (trình duyệt) cũng gọi (F7).
INTERNAL_ROUTES: tuple[tuple[str, str], ...] = (
    ("POST", "/transfers/{id}/shipped"),
    ("POST", "/transfers/{id}/received"),
)

# Worker nền khởi động cùng app (F2: hq-api kèm Health Monitor và Replay Worker)
WORKERS = (health.run, replay.run)


def parse_node_urls(raw: str) -> dict[int, str]:
    """NODE_URLS = {"1": "http://HOST_B_IP:8001", ...} (F2) -> {1: url, ...}."""
    return {int(dc_id): url for dc_id, url in json.loads(raw).items()}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    state = app.state
    state.internal_key = os.environ[ENV_INTERNAL_KEY]
    state.node_urls = parse_node_urls(os.environ[ENV_NODE_URLS])
    # min_size=0: app vẫn lên khi CSDL chưa sẵn sàng. Pool cần >= 3 kết nối cho 3 truy vấn FDW
    # song song (F3); mặc định max_size=10.
    state.pool = await asyncpg.create_pool(os.environ[ENV_HQ_DB_URL], min_size=0)
    state.nodes = {
        dc_id: InternalClient(url, state.internal_key) for dc_id, url in state.node_urls.items()
    }
    tasks = [asyncio.create_task(run(app), name=run.__module__) for run in WORKERS]
    state.worker_tasks = tasks
    log.info("HQ API khởi động, kho: %s", state.node_urls)
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for client in state.nodes.values():
            await client.aclose()
        await state.pool.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="HQ API",
        lifespan=lifespan,
        dependencies=[Depends(internal_key_guard(INTERNAL_ROUTES))],
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=CORS_ORIGIN_REGEX,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    install_error_handlers(app)

    @app.get("/health")
    async def health_check() -> HqHealthResponse:
        """F7: 200 {"status": "ok"}."""
        return HqHealthResponse(status=HQ_HEALTH_OK)

    # Mỗi chủ sở hữu đăng ký router của mình bằng một dòng
    app.include_router(transfers.router)  # Minh Hiếu – F7, F10.f, F10.g
    app.include_router(dashboard.router)  # Ngọc Chung – F7, db.md 2.4
    app.include_router(invariant.router)  # Ngọc Chung – F7
    app.include_router(failover.router)  # Nhật Khánh – F7, F10.h
    return app


app = create_app()
