"""TC15 – Failover khi kho còn sống (docs/tests.md mục 4).

Cách chạy: gọi /failover cho kho đang ONLINE.
Assert: 409 DC_NOT_OFFLINE; không có dòng pending_txn mới.
Spec: F7 /failover, F10.h bước 2. Logic có unit test ở test_failover.py.
"""

from __future__ import annotations

import httpx
import pytest
from failover_helpers import count_pending, make_reset, target, wait_dc_status
from fixtures import SKU_A_PRODUCT_ID

from common.constants import DcStatus, ErrorCode
from common.ids import new_request_id
from common.models import FailoverRequest

pytestmark = pytest.mark.integration

DC_TRUNG = 2


async def test_tc15_failover_online() -> None:
    make_reset()
    t = target()
    await wait_dc_status(t, DC_TRUNG, DcStatus.ONLINE)
    before = await count_pending(t)
    async with httpx.AsyncClient(timeout=10) as http:
        for kind in ("inbound", "outbound"):
            # request_id mới mỗi lần: lệnh chưa từng gửi, nên không rơi vào nhánh trùng (bước 1)
            body = FailoverRequest(
                request_id=new_request_id(), product_id=SKU_A_PRODUCT_ID, qty=5, created_by="tc15"
            )
            r = await http.post(
                f"{t.hq}/failover/dc/{DC_TRUNG}/{kind}", json=body.model_dump(mode="json")
            )
            assert r.status_code == 409, r.text
            assert r.json()["error_code"] == ErrorCode.DC_NOT_OFFLINE
    assert await count_pending(t) == before
