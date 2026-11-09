"""POST /recovery/complete – F6, F9 (Tuấn Minh). Đổi chế độ bằng request.app.state.mode."""

from fastapi import APIRouter, Request

from common.constants import NodeMode
from common.models import RecoveryCompleteResponse

router = APIRouter()


@router.post("/recovery/complete", response_model=RecoveryCompleteResponse)
async def recovery_complete(request: Request) -> RecoveryCompleteResponse:
    """F9: RECOVERING -> ONLINE. HQ gọi; quản trị viên gọi tay khi HQ không chạy."""
    request.app.state.mode = NodeMode.ONLINE
    return RecoveryCompleteResponse(mode=NodeMode.ONLINE)
