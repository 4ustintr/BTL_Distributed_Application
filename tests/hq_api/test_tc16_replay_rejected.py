"""TC16 – Replay bị từ chối (docs/tests.md mục 4; Kịch bản 3, "Trường hợp cần nói rõ").

Cách chạy: dừng hq-consumer; kho Trung xuất 30 bằng REQ_TC16_OUT_LOCAL (thật còn 10, snapshot
vẫn 40); kill-dc2; failover xuất 30 bằng REQ_TC16_OUT_FAILOVER; start-dc2; bật lại hq-consumer.
Assert: pending_txn REJECTED, error_msg chứa INSUFFICIENT_STOCK; Trung còn 10.
Spec: F6 /replay, F9, F10.h, F10.i.
"""

from __future__ import annotations

import httpx
import pytest
from failover_helpers import (
    HQ_CONSUMER_CONTAINER,
    docker,
    make,
    make_reset,
    node_stock,
    target,
    wait_dc_status,
    wait_snapshot,
    wait_txn_status,
)
from fixtures import DEMO_IDS, SKU_A_PRODUCT_ID

from common.constants import DcStatus, ErrorCode, PendingTxnStatus, RefType
from common.models import FailoverRequest, OutboundRequest

pytestmark = pytest.mark.integration

DC_TRUNG = 2
QTY = 30
ACTOR = "tc16"
REQ_LOCAL = DEMO_IDS["REQ_TC16_OUT_LOCAL"]  # tests.md mục 2: kho Trung tự xuất 30
REQ_FAILOVER = DEMO_IDS["REQ_TC16_OUT_FAILOVER"]  # tests.md mục 2: ghi hộ xuất 30


async def test_tc16_replay_rejected() -> None:
    make_reset()
    t = target()
    await wait_dc_status(t, DC_TRUNG, DcStatus.ONLINE)
    # Seed nhập qua API rồi CDC mới đưa về HQ: chờ snapshot Trung = 40 trước khi dừng Consumer,
    # nếu không HQ chưa có snapshot (coi là 0) và từ chối ghi hộ xuất 30 (422)
    await wait_snapshot(t, DC_TRUNG, (40, 40))  # (tồn, khả dụng)
    docker("stop", HQ_CONSUMER_CONTAINER)
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            # Kho Trung xuất 30 khi Consumer dừng: thật còn 10, snapshot ở HQ vẫn 40
            out = OutboundRequest(
                request_id=REQ_LOCAL,
                product_id=SKU_A_PRODUCT_ID,
                qty=QTY,
                ref_type=RefType.ISSUE,
                created_by=ACTOR,
            )
            r = await http.post(f"{t.nodes[DC_TRUNG]}/outbound", json=out.model_dump(mode="json"))
            assert r.status_code == 200, r.text
            assert r.json()["qty_on_hand"] == 10

            make("kill-dc2")
            await wait_dc_status(t, DC_TRUNG, DcStatus.OFFLINE, timeout_s=15)

            # HQ duyệt theo khả dụng tạm tính 40 >= 30 nên nhận ghi hộ
            body = FailoverRequest(
                request_id=REQ_FAILOVER, product_id=SKU_A_PRODUCT_ID, qty=QTY, created_by=ACTOR
            )
            r = await http.post(
                f"{t.hq}/failover/dc/{DC_TRUNG}/outbound", json=body.model_dump(mode="json")
            )
            assert r.status_code == 200, r.text
            assert r.json()["status"] == PendingTxnStatus.PENDING
            assert r.json()["duplicate"] is False

        make("start-dc2")
        make("restart-connector-dc2")
        row = await wait_txn_status(t, DC_TRUNG, REQ_FAILOVER, PendingTxnStatus.REJECTED)
    finally:
        docker("start", HQ_CONSUMER_CONTAINER)

    assert ErrorCode.INSUFFICIENT_STOCK in (row["error_msg"] or "")
    assert (await node_stock(t, DC_TRUNG))[0] == 10
    await wait_dc_status(t, DC_TRUNG, DcStatus.ONLINE, timeout_s=60)
