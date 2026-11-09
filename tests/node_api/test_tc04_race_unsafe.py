"""TC04 (tùy chọn) – Xuất đồng thời không khóa (docs/tests.md mục 4; demo.md KB1 bước 0).

Cách chạy: make race-unsafe trên dữ liệu vừa reset (node-api-dc1 chạy LOCK_MODE=none, chờ
UNSAFE_READ_WRITE_DELAY_S giữa đọc và ghi, F11).
Assert: cả 2 lệnh 200 (sổ có đủ 2 dòng REQ_RACE_1, REQ_RACE_2); tồn Bắc chỉ giảm 30; DIFF = 30
(chứng minh lost update).
Sau test kho Bắc vẫn chạy LOCK_MODE=none: test kế tiếp tự gọi make reset nên trở lại optimistic.
"""

from __future__ import annotations

import httpx
import pytest
from fixtures import DEMO_IDS, SKU_A_PRODUCT_ID, integration_target, make, make_reset
from node_integration import NodeHarness

from common.constants import DC_IDS

pytestmark = [pytest.mark.integration, pytest.mark.optional]

DC_BAC = DC_IDS[0]  # db.md 2.1
RACE_QTY = 30  # tests.md mục 2


async def test_tc04_race_without_lock_loses_an_update() -> None:
    make_reset()
    make("race-unsafe")
    t = integration_target()
    req_1, req_2 = DEMO_IDS["REQ_RACE_1"], DEMO_IDS["REQ_RACE_2"]
    async with NodeHarness(DC_BAC) as bac:
        movements = await bac.movements([req_1, req_2])
        end = await bac.inventory(SKU_A_PRODUCT_ID)
    # Cả hai lệnh đều được nhận (mỗi lệnh một dòng sổ -30) nhưng tồn chỉ giảm 30: mất một cập nhật
    assert sorted(m["qty_change"] for m in movements) == [-RACE_QTY, -RACE_QTY]
    assert end["qty_on_hand"] == 100 - RACE_QTY

    async with httpx.AsyncClient(timeout=15) as http:
        inv = await http.get(f"{t.hq}/invariant", params={"product_id": SKU_A_PRODUCT_ID})
    assert inv.status_code == 200, inv.text
    # tồn 120 + đi đường 0 + đã xuất 60 − đã nhập 150 = 30 (docs/tests.md TC04, demo.md KB1 bước 0)
    assert inv.json()["diff"] == RACE_QTY
