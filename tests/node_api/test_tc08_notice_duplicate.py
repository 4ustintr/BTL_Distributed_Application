"""TC08 (tùy chọn) – Gửi lại Transfer Notice (docs/tests.md mục 4).

Gọi POST /transfers/notice ở kho Bắc 2 lần cùng transfer_code (phiếu 1, tests.md mục 2).
Assert: lần 2 duplicate=true; qty_reserved không tăng gấp đôi.
Test CI của notice (không cần CSDL): test_transfer_notice.py.
Spec: F5 (gửi trùng), F6 (/transfers/notice), F10.b.
"""

from __future__ import annotations

from datetime import date
from uuid import uuid4

import asyncpg
import httpx
import pytest
from fixtures import SKU_A_PRODUCT_ID, integration_target, make_reset

from common.constants import DC_IDS, INTERNAL_KEY_HEADER, TransferDirection, TransferStatus
from common.ids import make_transfer_code
from common.models import TransferNoticeItem, TransferNoticeRequest

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS  # db.md 2.1
NOTICE_PATH = "/transfers/notice"  # F6
QTY = 50  # tests.md mục 2 – phiếu 1: Bắc → Nam, 50 thùng
STOCK_BAC = 100  # tests.md mục 2 – tồn ban đầu Bắc


@pytest.mark.integration
@pytest.mark.optional
async def test_tc08_notice_duplicate() -> None:
    make_reset()
    t = integration_target()
    headers = {INTERNAL_KEY_HEADER: t.internal_key}
    body = TransferNoticeRequest(
        transfer_id=uuid4(),  # GIẢ ĐỊNH: demo_ids.env chưa có transfer_id cố định cho phiếu
        transfer_code=make_transfer_code(date.today().year, 1),
        from_dc_id=DC_BAC,
        to_dc_id=DC_NAM,
        direction=TransferDirection.OUTBOUND,
        items=[TransferNoticeItem(line_no=1, product_id=SKU_A_PRODUCT_ID, qty=QTY)],
    ).model_dump(mode="json")
    node_bac = t.nodes[DC_BAC]

    async with (
        httpx.AsyncClient(timeout=15) as http,
        asyncpg.create_pool(t.hq_db, min_size=1, max_size=1) as db,
    ):
        r = await http.post(f"{node_bac}{NOTICE_PATH}", json=body, headers=headers)
        assert r.status_code == 200, r.text
        assert r.json() == {"status": TransferStatus.CREATED, "duplicate": False}

        # HQ gửi lại đúng notice đó (F5: gửi trùng trả 200 duplicate và trạng thái hiện tại)
        r = await http.post(f"{node_bac}{NOTICE_PATH}", json=body, headers=headers)
        assert r.status_code == 200, r.text
        assert r.json() == {"status": TransferStatus.CREATED, "duplicate": True}

        # qty_reserved không tăng gấp đôi: Bắc 100/50 như KB1-1 (đọc kho qua FDW, F3)
        row = await db.fetchrow(
            f"SELECT qty_on_hand, qty_reserved FROM dc{DC_BAC}.inventory "
            "WHERE dc_id = $1 AND product_id = $2",
            DC_BAC,
            SKU_A_PRODUCT_ID,
        )
        assert row is not None, "kho Bắc chưa có dòng inventory cho SKU A (make seed)"
        assert (row["qty_on_hand"], row["qty_reserved"]) == (STOCK_BAC, QTY)
        orders = await db.fetchval(
            f"SELECT count(*) FROM dc{DC_BAC}.transfer_order WHERE transfer_code = $1",
            body["transfer_code"],
        )
        assert orders == 1
