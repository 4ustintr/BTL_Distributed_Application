"""TC02 – Gửi trùng request_id (docs/tests.md mục 4; spec F5, F10.a bước 1).

/outbound 5 hai lần cùng request_id ở kho Nam (dc3): lần 1 duplicate=false, lần 2 duplicate=true;
tồn chỉ giảm 5; chỉ 1 dòng sổ. Chạy `make reset` bằng tay trước khi chạy.
"""

import pytest
from fixtures import SKU_A_PRODUCT_ID
from node_integration import NodeHarness

from common.ids import new_request_id

pytestmark = pytest.mark.integration

NAM = 3


@pytest.fixture
async def nam():  # type: ignore[no-untyped-def]
    async with NodeHarness(NAM) as h:
        yield h


async def test_tc02_duplicate_request_id(nam: NodeHarness) -> None:
    before = (await nam.inventory(SKU_A_PRODUCT_ID))["qty_on_hand"]
    assert before >= 5, "kho Nam cần ít nhất 5 thùng (make reset rồi seed)"
    req = new_request_id()
    body = {
        "request_id": req, "product_id": SKU_A_PRODUCT_ID, "qty": 5,
        "ref_type": "ISSUE", "created_by": "tc02",
    }  # fmt: skip

    first = await nam.post("/outbound", body)
    assert first.status_code == 200
    assert first.json()["duplicate"] is False
    assert first.json()["qty_on_hand"] == before - 5

    second = await nam.post("/outbound", body)
    assert second.status_code == 200
    assert second.json()["duplicate"] is True
    assert second.json()["qty_on_hand"] == before - 5  # số hiện tại, không trừ lần hai

    assert (await nam.inventory(SKU_A_PRODUCT_ID))["qty_on_hand"] == before - 5
    assert len(await nam.movements([req])) == 1
    assert len(await nam.outbox_events([req])) == 1  # lệnh trùng không phát thêm sự kiện
