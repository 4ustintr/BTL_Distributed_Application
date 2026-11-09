"""POST /recovery/complete – F6, F9 (Tuấn Minh). Đổi chế độ bằng request.app.state.mode.

Kèm cảnh báo F11: kho còn RECOVERING sau RECOVERING_WARN_AFTER_S giây thì ghi một dòng vào log.
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import APIRouter, FastAPI, Request

from common.constants import RECOVERING_WARN_AFTER_S, NodeMode
from common.models import RecoveryCompleteResponse

log = logging.getLogger("node-api")


async def _warn_if_still_recovering(app: FastAPI) -> None:
    await asyncio.sleep(RECOVERING_WARN_AFTER_S)
    if app.state.mode == NodeMode.RECOVERING:
        log.warning(
            "Kho %s vẫn RECOVERING sau %.0f giây: chờ HQ gọi /recovery/complete "
            "(hoặc quản trị viên gọi tay nếu HQ không chạy)",
            app.state.dc_id,
            RECOVERING_WARN_AFTER_S,
        )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    watcher = asyncio.create_task(_warn_if_still_recovering(app))
    try:
        yield
    finally:
        watcher.cancel()
        with suppress(asyncio.CancelledError):
            await watcher


router = APIRouter(lifespan=lifespan)


@router.post("/recovery/complete", response_model=RecoveryCompleteResponse)
async def recovery_complete(request: Request) -> RecoveryCompleteResponse:
    """F9: RECOVERING -> ONLINE. HQ gọi; quản trị viên gọi tay khi HQ không chạy."""
    request.app.state.mode = NodeMode.ONLINE
    return RecoveryCompleteResponse(mode=NodeMode.ONLINE)
