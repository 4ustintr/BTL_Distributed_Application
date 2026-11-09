"""TC13 (tùy chọn) – FDW khi kho treo (docs/tests.md mục 4).

Cách chạy: make pause-dc2, gọi GET /dashboard/inventory?mode=realtime.
Assert: phản hồi 3–5 giây; Trung source=offline; Bắc, Nam có số.
Spec: F3 (connect_timeout 3 giây, keep_connections 'off'), F11 (timeout truy vấn FDW 5 giây);
demo.md KB2 bước 2.
"""

from __future__ import annotations

import time

import httpx
import pytest
from fixtures import SKU_A_PRODUCT_ID, integration_target, make, make_reset

from common.constants import DC_IDS, DashboardMode, InventorySource

pytestmark = [pytest.mark.integration, pytest.mark.optional]

DC_BAC, DC_TRUNG, DC_NAM = DC_IDS  # db.md 2.1
SEED_STOCK = {DC_BAC: 100, DC_NAM: 10}  # tests.md mục 2
MIN_S, MAX_S = 3.0, 5.5  # 3 giây connect_timeout; tối đa 5 giây timeout FDW cộng chút trễ HTTP


async def test_tc13_hung_dc_bounded_by_fdw_timeout() -> None:
    make_reset()
    t = integration_target()
    make("pause-dc2")
    try:
        async with httpx.AsyncClient(timeout=15) as http:
            params = {"mode": DashboardMode.REALTIME, "product_id": SKU_A_PRODUCT_ID}
            start = time.monotonic()
            r = await http.get(f"{t.hq}/dashboard/inventory", params=params)
            elapsed = time.monotonic() - start
    finally:
        make("unpause-dc2")
    assert r.status_code == 200, r.text
    assert MIN_S <= elapsed <= MAX_S, f"phản hồi {elapsed:.2f} giây"
    items = {item["dc_id"]: item for item in r.json()["items"]}
    assert items[DC_TRUNG]["source"] == InventorySource.OFFLINE
    for dc_id, qty in SEED_STOCK.items():
        assert items[dc_id]["qty_on_hand"] == qty
