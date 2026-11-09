"""TC05 – Điều chuyển trọn vẹn (docs/tests.md mục 4).

Tạo phiếu 1 (Bắc → Nam, 50 thùng), kho Bắc xuất, kho Nam nhận; không chạy race.
Assert: RECEIVED ở transfer_summary và cả 2 transfer_order; Bắc 50/50, Nam 60/60, DIFF = 0.
Spec: F6 ship/receive, F7 POST /transfers, F10.b–d, F10.f; KB1 bước 1, 3, 4.
"""

from datetime import date

import asyncpg
import httpx
import pytest
from fixtures import SKU_A_PRODUCT_ID
from saga_helpers import DC_BAC, DC_NAM, make_reset, stock, target, wait_order

from common.constants import TransferStatus
from common.ids import make_transfer_code
from common.models import (
    CreateTransferRequest,
    CreateTransferResponse,
    TransferActionRequest,
    TransferItem,
)

pytestmark = pytest.mark.integration

QTY = 50
ACTOR = TransferActionRequest(created_by="tc05").model_dump(mode="json")


async def test_tc05_transfer_full() -> None:
    make_reset()
    t = target()
    async with (
        httpx.AsyncClient(timeout=15) as http,
        asyncpg.create_pool(t.hq_db, min_size=1, max_size=1) as db,
    ):
        # KB1-1: HQ tạo phiếu; kho Bắc giữ chỗ 50, kho Nam có bản INBOUND
        req = CreateTransferRequest(
            from_dc_id=DC_BAC,
            to_dc_id=DC_NAM,
            items=[TransferItem(product_id=SKU_A_PRODUCT_ID, qty=QTY)],
        )
        r = await http.post(f"{t.hq}/transfers", json=req.model_dump(mode="json"))
        assert r.status_code == 201, r.text
        created = CreateTransferResponse.model_validate(r.json())
        assert created.status == TransferStatus.CREATED

        # transfer_code_seq về 1 sau make reset
        assert created.transfer_code == make_transfer_code(date.today().year, 1)
        tid = created.transfer_id

        # KB1-3: nhân viên kho Bắc bấm Xuất
        r = await http.post(f"{t.nodes[DC_BAC]}/transfers/{tid}/ship", json=ACTOR)
        assert r.status_code == 200, r.text
        assert r.json()["status"] == TransferStatus.SHIPPED

        # HQ đồng bộ SHIPPED xuống kho Nam qua /sync-status; chưa SHIPPED thì receive trả 409
        nam = await wait_order(db, DC_NAM, tid, TransferStatus.SHIPPED)
        assert nam.get("status") == TransferStatus.SHIPPED

        # KB1-4: nhân viên kho Nam bấm Nhận
        r = await http.post(f"{t.nodes[DC_NAM]}/transfers/{tid}/receive", json=ACTOR)
        assert r.status_code == 200, r.text
        assert r.json()["status"] == TransferStatus.RECEIVED

        # RECEIVED ở transfer_summary và cả 2 transfer_order
        r = await http.get(f"{t.hq}/transfers/{tid}")
        assert r.status_code == 200, r.text
        assert r.json()["status"] == TransferStatus.RECEIVED
        bac = await wait_order(db, DC_BAC, tid, TransferStatus.RECEIVED)
        assert bac.get("status") == TransferStatus.RECEIVED
        nam = await wait_order(db, DC_NAM, tid, TransferStatus.RECEIVED)
        assert nam.get("status") == TransferStatus.RECEIVED

        # Bắc 50/50, Nam 60/60 (tồn/khả dụng)
        assert await stock(db, DC_BAC, SKU_A_PRODUCT_ID) == (50, 50)
        assert await stock(db, DC_NAM, SKU_A_PRODUCT_ID) == (60, 60)

        # DIFF = 0
        r = await http.get(f"{t.hq}/invariant", params={"product_id": SKU_A_PRODUCT_ID})
        assert r.status_code == 200, r.text
        assert r.json()["diff"] == 0
