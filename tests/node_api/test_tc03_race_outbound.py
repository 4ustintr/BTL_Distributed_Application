"""TC03 – Xuất đồng thời có khóa (docs/tests.md mục 4; spec F10.a, F10.b, db.md 2.7).

Cách chạy: tạo phiếu 1 (giữ chỗ 50) rồi bắn cùng lúc 2 lệnh xuất 30 thùng vào kho Bắc
(REQ_RACE_1, REQ_RACE_2, như make race).
Assert: đúng 1 lệnh 200, 1 lệnh 422 INSUFFICIENT_STOCK; Bắc 70/20; DIFF = 0.

Phiếu 1 được giữ chỗ bằng Transfer Notice gửi thẳng tới kho Bắc (F10.b), là đúng phần việc ở kho
của bước "tạo phiếu"; không cần HQ POST /transfers.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import date

import httpx
import pytest
from fixtures import DEMO_IDS, SKU_A_PRODUCT_ID, integration_target, make_reset
from node_integration import NodeHarness

from common.constants import DC_IDS, INTERNAL_KEY_HEADER, ErrorCode, RefType, TransferDirection
from common.ids import make_transfer_code, new_request_id
from common.models import OutboundRequest, TransferNoticeItem, TransferNoticeRequest

pytestmark = pytest.mark.integration

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS  # db.md 2.1
RESERVED = 50  # tests.md mục 2: phiếu 1, Bắc → Nam, 50 thùng
RACE_QTY = 30  # tests.md mục 2: mỗi lệnh tranh chấp xuất 30 thùng


@pytest.fixture
async def bac() -> AsyncIterator[NodeHarness]:
    make_reset()  # tests.md mục 4: test tự gọi make reset
    async with NodeHarness(DC_BAC) as h:
        yield h


def outbound(request_id: str) -> dict[str, object]:
    return OutboundRequest(
        request_id=request_id,
        product_id=SKU_A_PRODUCT_ID,
        qty=RACE_QTY,
        ref_type=RefType.ISSUE,
        created_by="tc03",
    ).model_dump(mode="json")


async def test_tc03_two_concurrent_outbounds_one_wins(bac: NodeHarness) -> None:
    t = integration_target()
    start = await bac.inventory(SKU_A_PRODUCT_ID)
    assert (start["qty_on_hand"], start["qty_reserved"]) == (100, 0), (
        "TC03 cần kho Bắc ở trạng thái sau make reset và seed (100/0)"
    )

    # Phiếu 1: kho Bắc nhận Transfer Notice và giữ chỗ 50 (F10.b)
    notice = TransferNoticeRequest(
        transfer_id=new_request_id(),
        transfer_code=make_transfer_code(date.today().year, 1),
        from_dc_id=DC_BAC,
        to_dc_id=DC_NAM,
        direction=TransferDirection.OUTBOUND,
        items=[TransferNoticeItem(line_no=1, product_id=SKU_A_PRODUCT_ID, qty=RESERVED)],
    ).model_dump(mode="json")
    r = await bac.http.post(
        "/transfers/notice", json=notice, headers={INTERNAL_KEY_HEADER: t.internal_key}
    )
    assert r.status_code == 200, r.text

    req_1, req_2 = DEMO_IDS["REQ_RACE_1"], DEMO_IDS["REQ_RACE_2"]
    first, second = await asyncio.gather(
        bac.post("/outbound", outbound(req_1)), bac.post("/outbound", outbound(req_2))
    )

    assert sorted([first.status_code, second.status_code]) == [200, 422]
    loser = first if first.status_code == 422 else second
    assert loser.json()["error_code"] == ErrorCode.INSUFFICIENT_STOCK

    end = await bac.inventory(SKU_A_PRODUCT_ID)
    # Bắc 70/20 theo docs/tests.md mục 3: tồn 70, khả dụng 20 (giữ chỗ 50 vẫn nguyên)
    assert (end["qty_on_hand"], end["qty_reserved"]) == (70, RESERVED)
    assert end["qty_on_hand"] - end["qty_reserved"] == 20

    # sổ khớp tồn: chỉ lệnh thắng được ghi, lệnh thua không để lại dòng sổ hay sự kiện
    assert len(await bac.movements([req_1, req_2])) == 1
    assert len(await bac.outbox_events([req_1, req_2])) == 1

    # DIFF = 0: tồn 3 kho + đi đường + đã xuất = đã nhập (F7 /invariant)
    async with httpx.AsyncClient(timeout=15) as http:
        inv = await http.get(f"{t.hq}/invariant", params={"product_id": SKU_A_PRODUCT_ID})
    assert inv.status_code == 200, inv.text
    assert inv.json()["diff"] == 0
