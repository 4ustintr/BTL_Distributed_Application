"""TC12 – FDW khi kho chết (docs/tests.md mục 4).

Cách chạy: make kill-dc2, gọi GET /dashboard/inventory?mode=realtime.
Assert: phản hồi dưới 1 giây; Bắc, Nam source=realtime; Trung source=offline.
Spec: F3, F7; db.md 2.4 (3 truy vấn FDW riêng, lỗi từng kho không làm mất số kho khác); KB2 bước 5.
"""

from __future__ import annotations

import time

import httpx
import pytest
from fixtures import SKU_A_PRODUCT_ID, integration_target, make, make_reset

from common.constants import DC_IDS, DashboardMode, InventorySource

pytestmark = pytest.mark.integration

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS  # db.md 2.1
SEED_STOCK = {DC_BAC: 100, DC_NAM: 10}  # tests.md mục 2


async def test_tc12_dead_dc_does_not_block_realtime_read() -> None:
    make_reset()
    t = integration_target()
    make("kill-dc2")
    try:
        async with httpx.AsyncClient(timeout=15) as http:
            params = {"mode": DashboardMode.REALTIME, "product_id": SKU_A_PRODUCT_ID}
            start = time.monotonic()
            r = await http.get(f"{t.hq}/dashboard/inventory", params=params)
            elapsed = time.monotonic() - start
    finally:
        make("start-dc2")
    assert r.status_code == 200, r.text
    assert elapsed < 1.0, f"phản hồi {elapsed:.2f} giây"
    items = {item["dc_id"]: item for item in r.json()["items"]}
    assert items[DC_TRUNG]["source"] == InventorySource.OFFLINE
    for dc_id, qty in SEED_STOCK.items():
        assert items[dc_id]["source"] == InventorySource.REALTIME
        assert items[dc_id]["qty_on_hand"] == qty
