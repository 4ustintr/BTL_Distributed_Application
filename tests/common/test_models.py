"""common/models.py: model F6, F7 và payload F8."""

import uuid

import pytest
from pydantic import ValidationError

from common.constants import EventType, NodeMode, RefType, TransferStatus
from common.models import (
    CreateTransferRequest,
    InboundRequest,
    InventoryEventPayload,
    NodeHealthResponse,
    OutboundRequest,
    StockChangeResponse,
    TransferEventPayload,
    TransferNoticeRequest,
)

# Mẫu payload ở spec F8
INVENTORY_SAMPLE = {
    "event_id": "8f0c2a4e-1d3b-4c5a-9e7f-0a1b2c3d4e5f",
    "event_type": "InventoryChanged",
    "source_dc": 2,
    "occurred_at": "2026-11-20T09:15:02.481+07:00",
    "product_id": 1,
    "qty_on_hand": 65,
    "qty_reserved": 0,
    "version": 8,
    "request_id": "3b1e7c90-5a2d-4f61-8b3c-9d0e1f2a3b4c",
    "ref_type": "REPLAY",
}
TRANSFER_SAMPLE = {
    "event_id": "c4d5e6f7-0a1b-4c2d-8e3f-5a6b7c8d9e0f",
    "event_type": "TransferShipped",
    "source_dc": 1,
    "occurred_at": "2026-11-20T09:05:40.117+07:00",
    "transfer_id": "5e6f7a8b-9c0d-4e1f-a2b3-c4d5e6f7a8b9",
    "transfer_code": "TR-2026-000001",
    "direction": "OUTBOUND",
    "status": "SHIPPED",
    "shipped_at": "2026-11-20T09:05:40.117+07:00",
    "received_at": None,
    "cancel_reason": None,
}


def test_inventory_payload_f8_sample() -> None:
    p = InventoryEventPayload.model_validate(INVENTORY_SAMPLE)
    assert p.event_type is EventType.INVENTORY_CHANGED
    assert p.version == 8
    assert p.ref_type is RefType.REPLAY
    assert p.request_id == uuid.UUID(INVENTORY_SAMPLE["request_id"])
    dumped = p.model_dump(mode="json", exclude_none=True)
    assert dumped["event_id"] == INVENTORY_SAMPLE["event_id"]  # UUID dạng chuỗi (F5)


def test_inventory_payload_rejects_transfer_event_type() -> None:
    with pytest.raises(ValidationError):
        InventoryEventPayload.model_validate({**INVENTORY_SAMPLE, "event_type": "TransferShipped"})


def test_transfer_payload_f8_sample() -> None:
    p = TransferEventPayload.model_validate(TRANSFER_SAMPLE)
    assert p.status is TransferStatus.SHIPPED
    assert p.received_at is None


def test_inbound_request_ok(demo_ids: dict[str, str]) -> None:
    r = InboundRequest.model_validate(
        {
            "request_id": demo_ids["REQ_SEED_1"],
            "product_id": 1,
            "qty": 100,
            "ref_type": "RECEIPT",
            "created_by": "seed",
        }
    )
    assert r.ref_type is RefType.RECEIPT


@pytest.mark.parametrize("qty", [0, -5])
def test_qty_must_be_positive(qty: int, demo_ids: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        OutboundRequest.model_validate(
            {
                "request_id": demo_ids["REQ_KB2_OUT"],
                "product_id": 1,
                "qty": qty,
                "ref_type": "ISSUE",
                "created_by": "nv",
            }
        )


def test_outbound_requires_issue_ref_type(demo_ids: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        OutboundRequest.model_validate(
            {
                "request_id": demo_ids["REQ_KB2_OUT"],
                "product_id": 1,
                "qty": 5,
                "ref_type": "RECEIPT",
                "created_by": "nv",
            }
        )


def test_create_transfer_rejects_same_dc() -> None:
    with pytest.raises(ValidationError):
        CreateTransferRequest.model_validate(
            {"from_dc_id": 1, "to_dc_id": 1, "items": [{"product_id": 1, "qty": 50}]}
        )
    ok = CreateTransferRequest.model_validate(
        {"from_dc_id": 1, "to_dc_id": 3, "items": [{"product_id": 1, "qty": 50}]}
    )
    assert ok.items[0].qty == 50


def test_transfer_notice_needs_items() -> None:
    base = {
        "transfer_id": str(uuid.uuid4()),
        "transfer_code": "TR-2026-000001",
        "from_dc_id": 1,
        "to_dc_id": 3,
        "direction": "OUTBOUND",
    }
    with pytest.raises(ValidationError):
        TransferNoticeRequest.model_validate({**base, "items": []})
    n = TransferNoticeRequest.model_validate(
        {**base, "items": [{"line_no": 1, "product_id": 1, "qty": 50}]}
    )
    assert n.items[0].line_no == 1


def test_json_dump_shapes() -> None:
    assert NodeHealthResponse(dc_id=2, mode=NodeMode.ONLINE, db="ok").model_dump(mode="json") == {
        "dc_id": 2,
        "mode": "ONLINE",
        "db": "ok",
    }
    assert StockChangeResponse(qty_on_hand=20, version=2).model_dump() == {
        "qty_on_hand": 20,
        "version": 2,
        "duplicate": False,
    }
