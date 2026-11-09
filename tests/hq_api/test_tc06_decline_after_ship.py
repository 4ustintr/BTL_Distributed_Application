"""TC06 (tùy chọn) – Từ chối sau khi đã xuất (docs/tests.md mục 4).

Cách chạy: tạo phiếu (Bắc → Trung 10 thùng), kho Bắc xuất, rồi kho Trung từ chối.
Assert: decline trả 409 INVALID_STATE; phiếu vẫn SHIPPED ở cả 3 nơi (transfer_summary, bản OUTBOUND
ở Bắc, bản INBOUND ở Trung).
Spec: F6, F7, F10.c, F10.g bước 3; db.md 3.2 quy tắc 4 (bước xuất là bước then chốt của Saga).
"""

import asyncpg
import httpx
import pytest
from fixtures import SKU_A_PRODUCT_ID
from saga_helpers import DC_BAC, DC_TRUNG, make_reset, target, wait_order

from common.constants import CancelReason, ErrorCode, TransferStatus
from common.models import (
    CreateTransferRequest,
    CreateTransferResponse,
    DeclineRequest,
    TransferActionRequest,
    TransferItem,
)

pytestmark = [pytest.mark.integration, pytest.mark.optional]

QTY = 10


async def test_tc06_decline_after_ship_is_rejected() -> None:
    make_reset()
    t = target()
    async with httpx.AsyncClient(timeout=30) as http:
        req = CreateTransferRequest(
            from_dc_id=DC_BAC,
            to_dc_id=DC_TRUNG,
            items=[TransferItem(product_id=SKU_A_PRODUCT_ID, qty=QTY)],
        )
        r = await http.post(f"{t.hq}/transfers", json=req.model_dump(mode="json"))
        assert r.status_code == 201, r.text
        tid = CreateTransferResponse.model_validate(r.json()).transfer_id

        actor = TransferActionRequest(created_by="tc06").model_dump(mode="json")
        r = await http.post(f"{t.nodes[DC_BAC]}/transfers/{tid}/ship", json=actor)
        assert r.status_code == 200, r.text

        decline = DeclineRequest(reason=CancelReason.DECLINED).model_dump(mode="json")
        r = await http.post(f"{t.nodes[DC_TRUNG]}/transfers/{tid}/decline", json=decline)
        assert r.status_code == 409, r.text
        assert r.json()["error_code"] == ErrorCode.INVALID_STATE

        r = await http.get(f"{t.hq}/transfers/{tid}")
        assert r.json()["status"] == TransferStatus.SHIPPED

    async with asyncpg.create_pool(t.hq_db, min_size=1, max_size=1) as db:
        bac = await wait_order(db, DC_BAC, tid, TransferStatus.SHIPPED)
        trung = await wait_order(db, DC_TRUNG, tid, TransferStatus.SHIPPED)
    assert bac.get("status") == TransferStatus.SHIPPED
    assert trung.get("status") == TransferStatus.SHIPPED
