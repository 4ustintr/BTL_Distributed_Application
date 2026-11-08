"""TC14 – Ghi hộ và đẩy lại (docs/tests.md mục 4; Kịch bản 3 bước 3–11).

Cách chạy: kill-dc2; failover nhập 30, xuất 10; start-dc2.
Assert: Trung 60/60 (40 + 30 − 10); 2 pending_txn APPLIED; dc_status ONLINE.
Spec: F7 /failover, F9, F10.h–j. Cần cả hệ thống (hq-api, node-api, consumer, connector).
"""

from __future__ import annotations

import httpx
import pytest
from failover_helpers import (
    make,
    make_reset,
    node_stock,
    pending_rows,
    target,
    wait_dc_status,
    wait_snapshot,
)
from fixtures import DEMO_IDS, SKU_A_PRODUCT_ID

from common.constants import DcStatus, PendingTxnStatus
from common.models import FailoverRequest, FailoverResponse

pytestmark = pytest.mark.integration

DC_TRUNG = 2
ACTOR = "tc14"


def failover_body(request_id: str, qty: int) -> dict[str, object]:
    return FailoverRequest(
        request_id=request_id, product_id=SKU_A_PRODUCT_ID, qty=qty, created_by=ACTOR
    ).model_dump(mode="json")


async def test_tc14_failover_replay() -> None:
    make_reset()
    t = target()
    await wait_dc_status(t, DC_TRUNG, DcStatus.ONLINE)
    assert await node_stock(t, DC_TRUNG) == (40, 40)

    make("kill-dc2")
    await wait_dc_status(t, DC_TRUNG, DcStatus.OFFLINE, timeout_s=15)  # M2: dự kiến 4–6 giây

    async with httpx.AsyncClient(timeout=10) as http:
        r = await http.post(
            f"{t.hq}/failover/dc/{DC_TRUNG}/inbound",
            json=failover_body(DEMO_IDS["REQ_KB3_IN"], 30),
        )
        assert r.status_code == 200, r.text
        inbound = FailoverResponse.model_validate(r.json())
        assert (inbound.status, inbound.provisional_qty) == (PendingTxnStatus.PENDING, 70)

        r = await http.post(
            f"{t.hq}/failover/dc/{DC_TRUNG}/outbound",
            json=failover_body(DEMO_IDS["REQ_KB3_OUT"], 10),
        )
        assert r.status_code == 200, r.text
        assert FailoverResponse.model_validate(r.json()).provisional_qty == 60

    make("start-dc2")
    await wait_dc_status(t, DC_TRUNG, DcStatus.RECOVERING, timeout_s=60)
    make("restart-connector-dc2")  # KB3 bước 8: connector đọc tiếp từ replication slot
    await wait_dc_status(t, DC_TRUNG, DcStatus.ONLINE, timeout_s=120)

    rows = await pending_rows(t, DC_TRUNG)
    assert sorted(str(r["txn_id"]) for r in rows) == sorted(
        [DEMO_IDS["REQ_KB3_IN"], DEMO_IDS["REQ_KB3_OUT"]]
    )
    assert {r["status"] for r in rows} == {PendingTxnStatus.APPLIED}
    assert await node_stock(t, DC_TRUNG) == (60, 60)
    await wait_snapshot(t, DC_TRUNG, (60, 60))
