"""TC07 – Từ chối trước khi xuất (docs/tests.md mục 4).

Tạo phiếu Bắc → Trung 10 thùng, kho Trung từ chối khi kho Bắc chưa xuất.
Assert: decline 200; CANCELLED ở cả 3 nơi, cancel_reason = DECLINED; qty_reserved Bắc về 0.
Spec: F6 /decline ở kho, F7 /decline ở HQ, F10.b, F10.e, F10.g; KB1 bước 5.
"""

import asyncpg
import httpx
import pytest
from fixtures import SKU_A_PRODUCT_ID
from saga_helpers import DC_BAC, DC_TRUNG, make_reset, stock, target, wait_order

from common.constants import CancelReason, TransferStatus
from common.models import (
    CreateTransferRequest,
    CreateTransferResponse,
    DeclineRequest,
    TransferItem,
)

pytestmark = pytest.mark.integration

QTY = 10


async def test_tc07_decline_before_ship() -> None:
    make_reset()
    t = target()
    async with (
        httpx.AsyncClient(timeout=15) as http,
        asyncpg.create_pool(t.hq_db, min_size=1, max_size=1) as db,
    ):
        req = CreateTransferRequest(
            from_dc_id=DC_BAC,
            to_dc_id=DC_TRUNG,
            items=[TransferItem(product_id=SKU_A_PRODUCT_ID, qty=QTY)],
        )
        r = await http.post(f"{t.hq}/transfers", json=req.model_dump(mode="json"))
        assert r.status_code == 201, r.text
        tid = CreateTransferResponse.model_validate(r.json()).transfer_id
        assert await stock(db, DC_BAC, SKU_A_PRODUCT_ID) == (100, 90)

        # KB1-5b: nhân viên kho Trung bấm Từ chối; Node API Trung chuyển tiếp lên HQ /decline
        decline = DeclineRequest(reason=CancelReason.DECLINED).model_dump(mode="json")
        r = await http.post(f"{t.nodes[DC_TRUNG]}/transfers/{tid}/decline", json=decline)
        assert r.status_code == 200, r.text
        assert r.json()["status"] == TransferStatus.CANCELLED

        # CANCELLED ở transfer_summary và cả 2 transfer_order
        r = await http.get(f"{t.hq}/transfers/{tid}")
        assert r.status_code == 200, r.text
        hq = r.json()
        assert hq["status"] == TransferStatus.CANCELLED
        assert hq["cancel_reason"] == CancelReason.DECLINED
        bac = await wait_order(db, DC_BAC, tid, TransferStatus.CANCELLED)
        assert bac.get("status") == TransferStatus.CANCELLED
        assert bac.get("cancel_reason") == CancelReason.DECLINED
        trung = await wait_order(db, DC_TRUNG, tid, TransferStatus.CANCELLED)
        # /sync-status chỉ mang status (F6) nên không assert cancel_reason ở bản INBOUND
        assert trung.get("status") == TransferStatus.CANCELLED

        # Kho Bắc nhả giữ chỗ: qty_reserved về 0, khả dụng trở lại bằng tồn
        assert await stock(db, DC_BAC, SKU_A_PRODUCT_ID) == (100, 100)
