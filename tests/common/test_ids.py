"""common/ids.py: request_id, uuid5 tất định (db.md 2.8), mã phiếu (F10.f)."""

import uuid

from common.constants import MovementType
from common.ids import (
    inventory_aggregate_id,
    make_transfer_code,
    new_request_id,
    transfer_movement_request_id,
)

TRANSFER_ID = "5e6f7a8b-9c0d-4e1f-a2b3-c4d5e6f7a8b9"


def test_new_request_id_is_random_uuid4() -> None:
    a, b = new_request_id(), new_request_id()
    assert a.version == 4
    assert a != b


def test_transfer_movement_request_id_is_deterministic() -> None:
    expected = uuid.uuid5(uuid.NAMESPACE_URL, f"{TRANSFER_ID}:TRANSFER_OUT:1")
    got = transfer_movement_request_id(TRANSFER_ID, MovementType.TRANSFER_OUT, 1)
    assert got == expected
    assert transfer_movement_request_id(uuid.UUID(TRANSFER_ID), "TRANSFER_OUT", 1) == expected
    assert transfer_movement_request_id(TRANSFER_ID, MovementType.TRANSFER_IN, 1) != expected
    assert transfer_movement_request_id(TRANSFER_ID, MovementType.TRANSFER_OUT, 2) != expected


def test_inventory_aggregate_id() -> None:
    assert inventory_aggregate_id(2, 1) == "2:1"


def test_make_transfer_code() -> None:
    assert make_transfer_code(2026, 1) == "TR-2026-000001"
    assert make_transfer_code(2026, 123) == "TR-2026-000123"
