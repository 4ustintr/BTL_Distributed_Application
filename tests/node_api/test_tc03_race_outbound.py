"""TC03 – Xuất đồng thời có khóa (docs/tests.md mục 4; spec F10.a, db.md 2.7).

Kho Bắc (dc1) có 100 thùng, một phiếu giữ chỗ 50 nên khả dụng 50; hai lệnh xuất 30 thùng bắn cùng
lúc (REQ_RACE_1, REQ_RACE_2). Đúng một lệnh 200, lệnh kia 422 INSUFFICIENT_STOCK; Bắc 70/20.

Khác docs/tests.md: giữ chỗ 50 được ghi bằng stock.reserve vào kho Bắc (đúng việc của
/transfers/notice, F10.b bước 3) thay vì tạo phiếu 1 qua HQ, vì phiếu thuộc Saga (HQ). Chưa kiểm
DIFF = 0 vì cần GET /invariant của HQ. Chạy sau `make reset` và seed (Bắc 100/0).
"""

import asyncio

import pytest
from fixtures import DEMO_IDS, SKU_A_PRODUCT_ID
from node_integration import NodeHarness
from services import stock

from common.constants import SQL_SET_APP_SOURCE, ErrorCode

pytestmark = pytest.mark.integration

BAC = 1


@pytest.fixture
async def bac():  # type: ignore[no-untyped-def]
    async with NodeHarness(BAC) as h:
        yield h


def outbound(request_id: str) -> dict[str, object]:
    return {
        "request_id": request_id,
        "product_id": SKU_A_PRODUCT_ID,
        "qty": 30,
        "ref_type": "ISSUE",
        "created_by": "tc03",
    }


async def test_tc03_two_concurrent_outbounds_one_wins(bac: NodeHarness) -> None:
    start = await bac.inventory(SKU_A_PRODUCT_ID)
    assert (start["qty_on_hand"], start["qty_reserved"]) == (100, 0), (
        "TC03 cần kho Bắc ở trạng thái sau make reset và seed (100/0)"
    )

    async with bac.db.transaction():  # giữ chỗ 50 như Transfer Notice của phiếu 1
        await bac.db.execute(SQL_SET_APP_SOURCE)
        await stock.reserve(bac.db, BAC, SKU_A_PRODUCT_ID, 50)

    req_1, req_2 = DEMO_IDS["REQ_RACE_1"], DEMO_IDS["REQ_RACE_2"]
    first, second = await asyncio.gather(
        bac.post("/outbound", outbound(req_1)), bac.post("/outbound", outbound(req_2))
    )

    assert sorted([first.status_code, second.status_code]) == [200, 422]
    loser = first if first.status_code == 422 else second
    assert loser.json()["error_code"] == ErrorCode.INSUFFICIENT_STOCK

    end = await bac.inventory(SKU_A_PRODUCT_ID)
    # Bắc 70/20 theo docs/tests.md mục 3: tồn 70, khả dụng 20 (giữ chỗ 50 vẫn nguyên)
    assert (end["qty_on_hand"], end["qty_reserved"]) == (70, 50)
    assert end["qty_on_hand"] - end["qty_reserved"] == 20

    # sổ khớp tồn: chỉ lệnh thắng được ghi, lệnh thua không để lại dòng sổ hay sự kiện
    assert len(await bac.movements([req_1, req_2])) == 1
    assert len(await bac.outbox_events([req_1, req_2])) == 1
