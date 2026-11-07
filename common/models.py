"""Model Pydantic v2 cho request/response F6, F7 và payload sự kiện F8.

Tên trường đúng như spec. UUID được Pydantic tuần tự hóa thành chuỗi (F5).
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from common.constants import (
    CancelReason,
    DcStatus,
    EventType,
    InventorySource,
    NodeMode,
    PendingTxnStatus,
    RefType,
    TransferDirection,
    TransferStatus,
    TxnType,
)

Qty = Annotated[int, Field(gt=0)]  # F5 – qty <= 0 là VALIDATION_ERROR
CreatedBy = Annotated[str, Field(max_length=50)]  # db.md 2.8, 2.12 – created_by VARCHAR(50)


class _Model(BaseModel):
    """Gốc chung cho mọi model."""


# ---------------------------------------------------------------------------
# Chung (F5)
# ---------------------------------------------------------------------------
class ErrorResponse(_Model):
    """Dạng lỗi chung (F5)."""

    error_code: str
    message: str
    details: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Node API (F6)
# ---------------------------------------------------------------------------
class NodeHealthResponse(_Model):
    """GET /health của kho (F6)."""

    dc_id: int
    mode: NodeMode
    db: str


class InventoryItem(_Model):
    """Một dòng của GET /inventory (F6)."""

    product_id: int
    qty_on_hand: int
    qty_reserved: int
    available: int
    version: int


class InventoryResponse(_Model):
    items: list[InventoryItem]


class InboundRequest(_Model):
    """POST /inbound (F6)."""

    request_id: UUID
    product_id: int
    qty: Qty
    ref_type: Literal[RefType.RECEIPT]
    created_by: CreatedBy


class OutboundRequest(_Model):
    """POST /outbound (F6)."""

    request_id: UUID
    product_id: int
    qty: Qty
    ref_type: Literal[RefType.ISSUE]
    created_by: CreatedBy


class StockChangeResponse(_Model):
    """200 của /inbound, /outbound: {"qty_on_hand", "version", "duplicate"} (F6)."""

    qty_on_hand: int
    version: int
    duplicate: bool = False


class ReplayRequest(_Model):
    """POST /replay, HQ gọi kho (F6)."""

    request_id: UUID
    txn_type: TxnType
    product_id: int
    qty: Qty
    created_by: CreatedBy


class ReplayResponse(_Model):
    """200 của /replay: {"qty_on_hand", "duplicate"} (F6)."""

    qty_on_hand: int
    duplicate: bool = False


class TransferNoticeItem(_Model):
    line_no: int = Field(gt=0)
    product_id: int
    qty: Qty


class TransferNoticeRequest(_Model):
    """POST /transfers/notice, HQ gọi kho (F6)."""

    transfer_id: UUID
    transfer_code: str = Field(max_length=20)
    from_dc_id: int
    to_dc_id: int
    direction: TransferDirection
    items: list[TransferNoticeItem] = Field(min_length=1)

    @model_validator(mode="after")
    def _check_dcs(self) -> TransferNoticeRequest:
        if self.from_dc_id == self.to_dc_id:
            raise ValueError("to_dc_id phải khác from_dc_id")
        return self


class TransferActionRequest(_Model):
    """Body của POST /transfers/{id}/ship và /receive: {"created_by"} (F6)."""

    created_by: CreatedBy


class DeclineRequest(_Model):
    """POST /transfers/{id}/decline ở kho và ở HQ: {"reason"} (F6, F7, F10.g)."""

    reason: CancelReason


class CancelRequest(_Model):
    """POST /transfers/{id}/cancel, HQ gọi kho nguồn (F6)."""

    reason: CancelReason


class SyncStatusRequest(_Model):
    """POST /transfers/{id}/sync-status, HQ gọi kho (F6)."""

    status: TransferStatus


class TransferStatusResponse(_Model):
    """{"status", "duplicate"} của notice, ship, receive, cancel, decline (F6, F7)."""

    status: TransferStatus
    duplicate: bool = False


class RecoveryCompleteResponse(_Model):
    """POST /recovery/complete trả {"mode": "ONLINE"} (F6)."""

    mode: NodeMode


# ---------------------------------------------------------------------------
# HQ API (F7)
# ---------------------------------------------------------------------------
class HqHealthResponse(_Model):
    """GET /health của HQ: {"status": "ok"} (F7)."""

    status: str


class TransferItem(_Model):
    """Một dòng hàng của phiếu ở HQ: {"product_id", "qty"} (F7, db.md 2.5 items)."""

    product_id: int
    qty: Qty


class CreateTransferRequest(_Model):
    """POST /transfers (F7)."""

    from_dc_id: int
    to_dc_id: int
    items: list[TransferItem] = Field(min_length=1)

    @model_validator(mode="after")
    def _check_dcs(self) -> CreateTransferRequest:
        if self.from_dc_id == self.to_dc_id:
            raise ValueError("to_dc_id phải khác from_dc_id")
        return self


class CreateTransferResponse(_Model):
    """201 của POST /transfers (F7)."""

    transfer_id: UUID
    transfer_code: str
    status: TransferStatus


class TransferSummary(_Model):
    """Bản ghi transfer_summary, trả ở GET /transfers/{id} và GET /transfers (F7, db.md 2.5)."""

    transfer_id: UUID
    transfer_code: str
    from_dc_id: int
    to_dc_id: int
    status: TransferStatus
    total_lines: int | None = None
    total_qty: int | None = None
    items: list[TransferItem]
    cancel_reason: CancelReason | None = None
    cancelled_at: datetime | None = None
    last_sync_at: datetime | None = None
    created_at: datetime
    shipped_at: datetime | None = None
    received_at: datetime | None = None
    updated_at: datetime


class ShippedCallback(_Model):
    """POST /transfers/{id}/shipped, kho nguồn gọi HQ (F7)."""

    shipped_at: datetime


class ReceivedCallback(_Model):
    """POST /transfers/{id}/received, kho đích gọi HQ (F7)."""

    received_at: datetime


class FailoverRequest(_Model):
    """POST /failover/dc/{dc_id}/inbound và /outbound (F7)."""

    request_id: UUID
    product_id: int
    qty: Qty
    created_by: CreatedBy


class FailoverResponse(_Model):
    """200 của /failover: {"txn_id", "status": "PENDING", "provisional_qty"} (F7)."""

    txn_id: UUID
    status: PendingTxnStatus
    provisional_qty: int


class DashboardInventoryItem(_Model):
    """Một dòng của GET /dashboard/inventory (F7)."""

    dc_id: int
    qty_on_hand: int
    qty_reserved: int
    qty_in_transit: int
    source: InventorySource
    as_of: datetime | None = None


class DashboardInventoryResponse(_Model):
    items: list[DashboardInventoryItem]


class DcStatusItem(_Model):
    """Một phần tử của GET /dc-status (F7)."""

    dc_id: int
    status: DcStatus
    updated_at: datetime


class PendingTxn(_Model):
    """Bản ghi pending_txn, trả ở GET /pending-txn (F7, db.md 2.12)."""

    txn_id: UUID
    dc_id: int
    txn_type: TxnType
    product_id: int
    qty: int
    status: PendingTxnStatus
    created_by: str | None = None
    created_at: datetime
    updated_at: datetime | None = None
    attempts: int = 0
    applied_at: datetime | None = None
    error_msg: str | None = None


class InvariantResponse(_Model):
    """GET /invariant (F7)."""

    stock: int
    in_transit: int
    shipped_out: int
    received_in: int
    diff: int


# ---------------------------------------------------------------------------
# Payload sự kiện (F8, db.md 2.11)
# ---------------------------------------------------------------------------
class _EventBase(_Model):
    event_id: UUID
    event_type: EventType
    source_dc: int
    occurred_at: datetime  # F8 – chỉ để hiển thị và đo độ trễ, không dùng sắp thứ tự


class InventoryEventPayload(_EventBase):
    """InventoryChanged / InventoryChangedDirect (F8)."""

    event_type: Literal[EventType.INVENTORY_CHANGED, EventType.INVENTORY_CHANGED_DIRECT]
    product_id: int
    qty_on_hand: int
    qty_reserved: int
    version: int
    request_id: UUID | None = None  # db.md 3.4 bước 3 – "nếu payload có request_id"
    ref_type: RefType | None = None
    changed_by: str | None = None  # F8 – chỉ có ở InventoryChangedDirect
    client_addr: str | None = None  # F8 – chỉ có ở InventoryChangedDirect


class TransferEventPayload(_EventBase):
    """TransferCreated / Shipped / Received / Cancelled (F8). Consumer bỏ qua loại này."""

    model_config = ConfigDict(extra="allow")  # F8 – mỗi loại có trường thời gian riêng

    event_type: Literal[
        EventType.TRANSFER_CREATED,
        EventType.TRANSFER_SHIPPED,
        EventType.TRANSFER_RECEIVED,
        EventType.TRANSFER_CANCELLED,
    ]
    transfer_id: UUID
    transfer_code: str
    direction: TransferDirection
    status: TransferStatus
    shipped_at: datetime | None = None
    received_at: datetime | None = None
    cancel_reason: CancelReason | None = None
