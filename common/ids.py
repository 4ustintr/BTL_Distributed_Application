"""Sinh mã định danh: request_id, uuid5 tất định cho dòng sổ điều chuyển, khóa sự kiện."""

from __future__ import annotations

import uuid

from common.constants import TRANSFER_CODE_PREFIX, TRANSFER_CODE_SEQ_DIGITS, MovementType


def new_request_id() -> uuid.UUID:
    """request_id mới cho một thao tác (F9: WMS sinh một lần và giữ nguyên khi gửi lại)."""
    return uuid.uuid4()


def transfer_movement_request_id(
    transfer_id: uuid.UUID | str, movement_type: MovementType | str, line_no: int
) -> uuid.UUID:
    """request_id tất định của dòng stock_movement điều chuyển (db.md 2.8, F10.c, F10.d).

    uuid5(NAMESPACE_URL, "{transfer_id}:{movement_type}:{line_no}").
    """
    return uuid.uuid5(
        uuid.NAMESPACE_URL, f"{uuid.UUID(str(transfer_id))}:{MovementType(movement_type)}:{line_no}"
    )


def inventory_aggregate_id(dc_id: int, product_id: int) -> str:
    """aggregate_id / key Kafka của sự kiện tồn kho: "dc_id:product_id" (F8, db.md 2.11)."""
    return f"{dc_id}:{product_id}"


def make_transfer_code(year: int, seq: int) -> str:
    """'TR-' + năm + '-' + số thứ tự 6 chữ số (F10.f), ví dụ TR-2026-000001."""
    return f"{TRANSFER_CODE_PREFIX}-{year}-{seq:0{TRANSFER_CODE_SEQ_DIGITS}d}"
