"""TC01 – Nhập, xuất cơ bản (docs/tests.md mục 4; spec F6, F10.a).

/inbound 10 rồi /outbound 5 ở kho Nam (dc3). Sau `make reset` tồn Nam đi 10 → 20 → 15; test dùng
số tương đối để chạy lại được.

Chưa kiểm "snapshot HQ = 15 trong 5 giây": cần CDC Consumer (Ngọc Chung), bổ sung khi Consumer xong.
"""

import pytest
from fixtures import SKU_A_PRODUCT_ID
from node_integration import NodeHarness

from common.constants import EventType
from common.ids import new_request_id

pytestmark = pytest.mark.integration

NAM = 3


@pytest.fixture
async def nam():  # type: ignore[no-untyped-def]
    async with NodeHarness(NAM) as h:
        yield h


async def test_tc01_inbound_then_outbound(nam: NodeHarness) -> None:
    before = (await nam.inventory(SKU_A_PRODUCT_ID))["qty_on_hand"]
    req_in, req_out = new_request_id(), new_request_id()

    r_in = await nam.post(
        "/inbound",
        {
            "request_id": req_in, "product_id": SKU_A_PRODUCT_ID, "qty": 10,
            "ref_type": "RECEIPT", "created_by": "tc01",
        },
    )  # fmt: skip
    assert r_in.status_code == 200
    assert r_in.json()["qty_on_hand"] == before + 10
    assert r_in.json()["duplicate"] is False

    r_out = await nam.post(
        "/outbound",
        {
            "request_id": req_out, "product_id": SKU_A_PRODUCT_ID, "qty": 5,
            "ref_type": "ISSUE", "created_by": "tc01",
        },
    )  # fmt: skip
    assert r_out.status_code == 200
    assert r_out.json()["qty_on_hand"] == before + 5
    assert r_out.json()["duplicate"] is False

    inv = await nam.inventory(SKU_A_PRODUCT_ID)
    assert inv["qty_on_hand"] == before + 5

    # 2 dòng sổ ứng với 2 request_id của test (sau seed Nam đã có thêm 1 dòng RECEIPT)
    moves = await nam.movements([req_in, req_out])
    assert [(m["movement_type"], m["qty_change"], m["ref_type"]) for m in moves] == [
        ("INBOUND", 10, "RECEIPT"),
        ("OUTBOUND", -5, "ISSUE"),
    ]

    # mỗi lệnh ghi đúng một InventoryChanged cùng transaction (db.md 2.11)
    events = await nam.outbox_events([req_in, req_out])
    assert [e["event_type"] for e in events] == [EventType.INVENTORY_CHANGED] * 2
    assert [e["qty_on_hand"] for e in events] == [before + 10, before + 5]
    assert events[0]["version"] < events[1]["version"]
    assert all(e["source_dc"] == NAM and e["product_id"] == SKU_A_PRODUCT_ID for e in events)
