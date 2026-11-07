"""Node API của một kho (spec F2, F5, F6, F9). Chạy: uvicorn main:app."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
import recovery
from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from routers import inventory, transfers

from common.constants import (
    CORS_ORIGIN_REGEX,
    ENV_DC_ID,
    ENV_HQ_URL,
    ENV_INTERNAL_KEY,
    ENV_LOCK_MODE,
    ENV_NODE_DB_URL,
    HEALTH_CHECK_TIMEOUT_S,
    NODE_DB_OK,
    NODE_START_MODE,
    LockMode,
)
from common.errors import install_error_handlers
from common.http import InternalClient, internal_key_guard

log = logging.getLogger("node-api")

# Endpoint do HQ gọi (cột "Gọi bởi" = HQ ở F6) -> bắt buộc X-Internal-Key
INTERNAL_ROUTES: tuple[tuple[str, str], ...] = (
    ("POST", "/transfers/notice"),
    ("POST", "/transfers/{id}/cancel"),
    ("POST", "/transfers/{id}/sync-status"),
    ("POST", "/replay"),
    ("POST", "/recovery/complete"),
)

NODE_DB_ERROR = "error"  # GIẢ ĐỊNH: F6 chỉ ghi "CSDL lỗi thì 503", không ghi giá trị trường db


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    state = app.state
    state.dc_id = int(os.environ[ENV_DC_ID])
    state.internal_key = os.environ[ENV_INTERNAL_KEY]
    state.lock_mode = LockMode(os.environ.get(ENV_LOCK_MODE, LockMode.OPTIMISTIC))
    state.mode = NODE_START_MODE  # F9: khởi động luôn ở RECOVERING
    # min_size=0: app vẫn lên khi CSDL chưa sẵn sàng; /health khi đó trả 503
    state.pool = await asyncpg.create_pool(os.environ[ENV_NODE_DB_URL], min_size=0)
    state.hq = InternalClient(os.environ[ENV_HQ_URL], state.internal_key)
    log.info("Node API kho %s khởi động, mode=%s", state.dc_id, state.mode)
    try:
        yield
    finally:
        await state.hq.aclose()
        await state.pool.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Node API",
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
    async def health(request: Request) -> JSONResponse:
        """F6: 200 {"dc_id", "mode", "db": "ok"}; CSDL lỗi thì 503."""
        state = request.app.state
        body = {"dc_id": state.dc_id, "mode": str(state.mode), "db": NODE_DB_OK}
        try:
            await asyncio.wait_for(state.pool.fetchval("SELECT 1"), HEALTH_CHECK_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001 – mọi lỗi CSDL đều là 503
            log.warning("Health: CSDL lỗi: %r", exc)
            return JSONResponse(status_code=503, content={**body, "db": NODE_DB_ERROR})
        return JSONResponse(status_code=200, content=body)

    # Mỗi chủ sở hữu đăng ký router của mình bằng một dòng
    app.include_router(inventory.router)  # Tuấn Minh – F6, F10.a
    app.include_router(transfers.router)  # Minh Hiếu – F6, F10.b–e
    app.include_router(recovery.router)  # Tuấn Minh – F6, F9
    return app


app = create_app()
