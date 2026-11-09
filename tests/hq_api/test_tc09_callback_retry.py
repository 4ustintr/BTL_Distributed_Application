"""TC09 (tùy chọn) – Callback lỗi rồi bấm lại (docs/tests.md mục 4).

Cách chạy: docker pause hq-api lúc kho Bắc bấm Xuất phiếu 1, unpause rồi bấm Xuất lại.
Assert: lần 1 trả 503 (kho đã xuất nhưng gọi HQ /shipped hết lượt thử, F5); lần 2 trả 200
duplicate và HQ chuyển SHIPPED (kho gửi lại callback, F10.c bước 1).
Spec: F5, F6 (ghi chú sau bảng), F7 /shipped, F10.c; db.md 3.2 quy tắc 3.
"""

import asyncpg
import httpx
import pytest
from fixtures import SKU_A_PRODUCT_ID, docker
from saga_helpers import DC_BAC, DC_NAM, make_reset, stock, target

from common.constants import ErrorCode, TransferStatus
from common.models import (
    CreateTransferRequest,
    CreateTransferResponse,
    TransferActionRequest,
    TransferItem,
)

pytestmark = [pytest.mark.integration, pytest.mark.optional]

HQ_API_CONTAINER = "hq-api"  # F2
QTY = 50  # tests.md mục 2: phiếu 1
# Kho thử lại callback F5 tối đa 5 × 2 + 4 × 2 = 18 giây rồi mới trả 503
SHIP_TIMEOUT_S = 30


async def test_tc09_callback_failure_then_retry_is_duplicate() -> None:
    make_reset()
    t = target()
    actor = TransferActionRequest(created_by="tc09").model_dump(mode="json")
    async with httpx.AsyncClient(timeout=SHIP_TIMEOUT_S) as http:
        req = CreateTransferRequest(
            from_dc_id=DC_BAC,
            to_dc_id=DC_NAM,
            items=[TransferItem(product_id=SKU_A_PRODUCT_ID, qty=QTY)],
        )
        r = await http.post(f"{t.hq}/transfers", json=req.model_dump(mode="json"))
        assert r.status_code == 201, r.text
        tid = CreateTransferResponse.model_validate(r.json()).transfer_id

        docker("pause", HQ_API_CONTAINER)
        try:
            first = await http.post(f"{t.nodes[DC_BAC]}/transfers/{tid}/ship", json=actor)
        finally:
            docker("unpause", HQ_API_CONTAINER)
        assert first.status_code == 503, first.text
        assert first.json()["error_code"] == ErrorCode.DC_UNREACHABLE

        second = await http.post(f"{t.nodes[DC_BAC]}/transfers/{tid}/ship", json=actor)
        assert second.status_code == 200, second.text
        assert second.json() == {"status": TransferStatus.SHIPPED, "duplicate": True}

        r = await http.get(f"{t.hq}/transfers/{tid}")
        assert r.json()["status"] == TransferStatus.SHIPPED

    # Kho chỉ xuất một lần dù bấm hai lần: Bắc 50/50 (tồn/khả dụng)
    async with asyncpg.create_pool(t.hq_db, min_size=1, max_size=1) as db:
        assert await stock(db, DC_BAC, SKU_A_PRODUCT_ID) == (50, 50)
