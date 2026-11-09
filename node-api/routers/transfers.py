"""notice, ship, receive, decline, cancel, sync-status – F6, F10.b–e (Minh Hiếu)."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg
import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from services import stock

from common.constants import (
    HTTP_MAX_RETRIES,
    HTTP_RETRY_INTERVAL_S,
    HTTP_TIMEOUT_S,
    SQL_SET_APP_SOURCE,
    TRANSFER_PREV_STATUS,
    AggregateType,
    CancelReason,
    ErrorCode,
    EventType,
    MovementType,
    RefType,
    TransferDirection,
    TransferStatus,
)
from common.errors import AppError
from common.ids import inventory_aggregate_id, transfer_movement_request_id
from common.models import (
    CancelRequest,
    DeclineRequest,
    ReceivedCallback,
    ShippedCallback,
    SyncStatusRequest,
    TransferActionRequest,
    TransferNoticeRequest,
    TransferStatusResponse,
)

router = APIRouter()

# GIẢ ĐỊNH chữ ký services/stock.py (Tuấn Minh chưa viết, chờ chốt):
#   reserve(conn, dc_id, product_id, qty) -> dòng RETURNING version, qty_on_hand, qty_reserved
#     (db.md 2.7); thiếu hàng: AppError(INSUFFICIENT_STOCK);
#     hết 3 lần thử: AppError(VERSION_CONFLICT) – notice trả 409, F6 và F10.f chưa nói tới
#   release(conn, dc_id, product_id, qty) -> như reserve, giảm qty_reserved (F10.e bước 2)
#   write_outbox(conn, dc_id, aggregate_type, aggregate_id, event_type, payload): tự sinh event_id,
#     điền event_id, event_type, source_dc, occurred_at = clock_timestamp() vào payload (F8)
#   Cả ba chạy trên conn được truyền, trong transaction của router, không tự COMMIT.

SQL_INSERT_ORDER = """
INSERT INTO transfer_order (transfer_id, transfer_code, from_dc_id, to_dc_id, direction)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (transfer_code) DO NOTHING
RETURNING transfer_id
"""

SQL_INSERT_ITEM = """
INSERT INTO transfer_item (transfer_id, line_no, product_id, qty_requested)
VALUES ($1, $2, $3, $4)
"""

SQL_ORDER_STATUS = "SELECT status FROM transfer_order WHERE transfer_code = $1"

# F10.e bước 1 – chỉ bản OUTBOUND còn CREATED mới hủy được (db.md 3.2 quy tắc 2, 4)
SQL_CANCEL_ORDER = """
UPDATE transfer_order SET status = $1, cancel_reason = $2
WHERE transfer_id = $3 AND direction = $4 AND status = $5
RETURNING transfer_code
"""

SQL_OUTBOUND_STATUS = "SELECT status FROM transfer_order WHERE transfer_id = $1 AND direction = $2"

SQL_ORDER_ITEMS = "SELECT product_id, qty_requested FROM transfer_item WHERE transfer_id = $1"

# /sync-status – chỉ đi từ trạng thái liền trước, đúng bản phiếu ở kho này (db.md 3.2 quy tắc 2)
SQL_SYNC_ORDER = """
UPDATE transfer_order SET status = $1
WHERE transfer_id = $2 AND direction = $3 AND status = $4
RETURNING transfer_code, cancel_reason, shipped_at, received_at
"""

# GIẢ ĐỊNH: /sync-status chỉ nhận 3 bước không kèm thay đổi tồn kho; cặp khác bỏ qua, vẫn 200 (F6).
# Đổi bản OUTBOUND sang SHIPPED/CANCELLED hay bản INBOUND sang RECEIVED phải qua ship, cancel,
# receive để trừ kho, nhả giữ chỗ, tăng tồn (F10.c–e).
SYNC_DIRECTION = {
    TransferStatus.SHIPPED: TransferDirection.INBOUND,  # kho nguồn đã xuất (F10.d bước 1)
    TransferStatus.RECEIVED: TransferDirection.OUTBOUND,  # kho đích đã nhận (hld.md, TC05)
    TransferStatus.CANCELLED: TransferDirection.INBOUND,  # kho nguồn đã hủy (F10.g bước 2)
}

TRANSFER_EVENT = {
    TransferStatus.CREATED: EventType.TRANSFER_CREATED,
    TransferStatus.SHIPPED: EventType.TRANSFER_SHIPPED,
    TransferStatus.RECEIVED: EventType.TRANSFER_RECEIVED,
    TransferStatus.CANCELLED: EventType.TRANSFER_CANCELLED,
}


class DuplicateNotice(Exception):
    """transfer_code đã có ở kho: thoát khối transaction để ROLLBACK (F10.b bước 1)."""


def iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


async def write_inventory_event(
    conn: asyncpg.Connection,
    dc_id: int,
    product_id: int,
    row: Mapping[str, Any],
    *,
    request_id: UUID | None = None,
    ref_type: RefType | None = None,
) -> None:
    """InventoryChanged của một SKU; số liệu lấy từ RETURNING của stock (F8, db.md 2.11).

    Xuất/nhận điều chuyển có dòng sổ nên kèm request_id tất định và ref_type TRANSFER (mẫu F8).
    Giữ chỗ và nhả giữ chỗ không ghi sổ nên không có request_id (db.md 3.4).
    """
    payload: dict[str, Any] = {
        "product_id": product_id,
        "qty_on_hand": row["qty_on_hand"],
        "qty_reserved": row["qty_reserved"],
        "version": row["version"],
    }
    if request_id is not None:
        payload["request_id"] = str(request_id)
        payload["ref_type"] = ref_type
    await stock.write_outbox(
        conn,
        dc_id,
        AggregateType.INVENTORY,
        inventory_aggregate_id(dc_id, product_id),
        EventType.INVENTORY_CHANGED,
        payload,
    )


async def write_transfer_event(
    conn: asyncpg.Connection,
    dc_id: int,
    transfer_id: UUID,
    transfer_code: str,
    direction: TransferDirection,
    status: TransferStatus,
    *,
    cancel_reason: CancelReason | str | None = None,
    shipped_at: datetime | None = None,
    received_at: datetime | None = None,
) -> None:
    """Sự kiện phiếu Transfer* ứng với trạng thái mới của phiếu (F8, db.md 2.11)."""
    await stock.write_outbox(
        conn,
        dc_id,
        AggregateType.TRANSFER,
        str(transfer_id),
        TRANSFER_EVENT[status],
        {
            "transfer_id": str(transfer_id),
            "transfer_code": transfer_code,
            "direction": direction,
            "status": status,
            "shipped_at": iso(shipped_at),
            "received_at": iso(received_at),
            "cancel_reason": cancel_reason,
        },
    )


@router.post("/transfers/notice", response_model=TransferStatusResponse)
async def notice(body: TransferNoticeRequest, request: Request) -> TransferStatusResponse:
    """F6, F10.b: kho nguồn ghi phiếu OUTBOUND và giữ chỗ; kho đích ghi phiếu INBOUND."""
    dc_id: int = request.app.state.dc_id
    async with request.app.state.pool.acquire() as conn:
        try:
            async with conn.transaction():
                await conn.execute(SQL_SET_APP_SOURCE)

                inserted = await conn.fetchval(
                    SQL_INSERT_ORDER,
                    body.transfer_id,
                    body.transfer_code,
                    body.from_dc_id,
                    body.to_dc_id,
                    body.direction,
                )
                if inserted is None:
                    raise DuplicateNotice

                await conn.executemany(
                    SQL_INSERT_ITEM,
                    [(body.transfer_id, i.line_no, i.product_id, i.qty) for i in body.items],
                )

                if body.direction == TransferDirection.OUTBOUND:
                    for item in sorted(body.items, key=lambda i: i.product_id):
                        row = await stock.reserve(conn, dc_id, item.product_id, item.qty)
                        await write_inventory_event(conn, dc_id, item.product_id, row)

                await write_transfer_event(
                    conn,
                    dc_id,
                    body.transfer_id,
                    body.transfer_code,
                    body.direction,
                    TransferStatus.CREATED,
                )
        except DuplicateNotice:
            status = await conn.fetchval(SQL_ORDER_STATUS, body.transfer_code)
            return TransferStatusResponse(status=status, duplicate=True)
        except asyncpg.ForeignKeyViolationError as exc:
            # F5: lệnh ghi có SKU trong body không có trong danh mục (product nhân bản từ HQ)
            # -> 400; transaction đã ROLLBACK khi lỗi thoát khỏi khối.
            raise AppError(
                ErrorCode.VALIDATION_ERROR,
                "SKU hoặc kho trong phiếu không có trong danh mục",
                {"detail": getattr(exc, "detail", None)},
            ) from exc
        return TransferStatusResponse(status=TransferStatus.CREATED)


@router.post("/transfers/{transfer_id}/cancel", response_model=TransferStatusResponse)
async def cancel(
    transfer_id: UUID, body: CancelRequest, request: Request
) -> TransferStatusResponse:
    """F6, F10.e: kho nguồn hủy phiếu còn CREATED và nhả giữ chỗ (giao dịch bù của Saga)."""
    dc_id: int = request.app.state.dc_id
    cancel_args = (
        TransferStatus.CANCELLED,
        body.reason,
        transfer_id,
        TransferDirection.OUTBOUND,
        TransferStatus.CREATED,
    )
    async with request.app.state.pool.acquire() as conn, conn.transaction():
        await conn.execute(SQL_SET_APP_SOURCE)

        # Bước 1: không cập nhật được thì xem bản OUTBOUND đang ở trạng thái nào
        code = await conn.fetchval(SQL_CANCEL_ORDER, *cancel_args)
        while code is None:
            status = await conn.fetchval(
                SQL_OUTBOUND_STATUS, transfer_id, TransferDirection.OUTBOUND
            )
            if status in (TransferStatus.SHIPPED, TransferStatus.RECEIVED):
                raise AppError(
                    ErrorCode.INVALID_STATE,
                    "Phiếu đã xuất, kho nguồn không hủy được",
                    {"status": status},
                )
            if status != TransferStatus.CREATED:
                # Đã hủy là gửi trùng (F5). GIẢ ĐỊNH: chưa có phiếu thì duplicate = false
                duplicate = status == TransferStatus.CANCELLED
                return TransferStatusResponse(status=TransferStatus.CANCELLED, duplicate=duplicate)
            # Notice vừa commit sau lệnh UPDATE: hủy lại, không trả CANCELLED khi phiếu còn
            # CREATED. Vòng lặp dừng vì trạng thái chỉ đi tới (db.md 3.2 quy tắc 2).
            code = await conn.fetchval(SQL_CANCEL_ORDER, *cancel_args)

        # Bước 2: nhả giữ chỗ từng dòng, theo product_id tăng dần như lúc giữ (notice)
        items = sorted(
            await conn.fetch(SQL_ORDER_ITEMS, transfer_id), key=lambda i: i["product_id"]
        )
        rows = [
            await stock.release(conn, dc_id, i["product_id"], i["qty_requested"]) for i in items
        ]

        # Bước 3
        for item, row in zip(items, rows, strict=True):
            await write_inventory_event(conn, dc_id, item["product_id"], row)
        await write_transfer_event(
            conn,
            dc_id,
            transfer_id,
            code,
            TransferDirection.OUTBOUND,
            TransferStatus.CANCELLED,
            cancel_reason=body.reason,
        )
    return TransferStatusResponse(status=TransferStatus.CANCELLED)


@router.post("/transfers/{transfer_id}/sync-status", response_class=Response)
async def sync_status(transfer_id: UUID, body: SyncStatusRequest, request: Request) -> Response:
    """F6: HQ đồng bộ trạng thái phiếu xuống kho; 200 không có thân, kể cả khi bỏ qua."""
    await apply_sync_status(transfer_id, body, request)
    return Response(status_code=200)


async def apply_sync_status(transfer_id: UUID, body: SyncStatusRequest, request: Request) -> None:
    """Đổi bản phiếu ở kho này nếu đang ở trạng thái liền trước; không tiến được thì bỏ qua."""
    direction = SYNC_DIRECTION.get(body.status)
    if direction is None:  # CREATED: không có trạng thái liền trước để đồng bộ
        return
    dc_id: int = request.app.state.dc_id
    async with request.app.state.pool.acquire() as conn, conn.transaction():
        await conn.execute(SQL_SET_APP_SOURCE)
        row = await conn.fetchrow(
            SQL_SYNC_ORDER,
            body.status,
            transfer_id,
            direction,
            TRANSFER_PREV_STATUS[body.status],
        )
        if row is None:  # đã đồng bộ, đến sai thứ tự, hoặc kho chưa có phiếu
            return
        await write_transfer_event(
            conn,
            dc_id,
            transfer_id,
            row["transfer_code"],
            direction,
            body.status,
            cancel_reason=row["cancel_reason"],
            shipped_at=row["shipped_at"],
            received_at=row["received_at"],
        )


# ---------------------------------------------------------------------------
# ship, receive (F10.c, F10.d), decline (F6)
# ---------------------------------------------------------------------------
# F10.c bước 1 – chỉ bản OUTBOUND còn CREATED; F10.d bước 2 – chỉ bản INBOUND đang SHIPPED
SQL_SHIP_ORDER = """
UPDATE transfer_order SET status = $1, shipped_at = now()
WHERE transfer_id = $2 AND direction = $3 AND status = $4
RETURNING transfer_code, shipped_at
"""

SQL_RECEIVE_ORDER = """
UPDATE transfer_order SET status = $1, received_at = now()
WHERE transfer_id = $2 AND direction = $3 AND status = $4
RETURNING transfer_code, shipped_at, received_at
"""

SQL_ORDER_STATE = """
SELECT status, shipped_at, received_at FROM transfer_order
WHERE transfer_id = $1 AND direction = $2
"""

# Theo product_id tăng dần, cùng thứ tự khóa dòng inventory với notice và cancel
SQL_ORDER_LINES = """
SELECT line_no, product_id, qty_requested FROM transfer_item
WHERE transfer_id = $1 ORDER BY product_id
"""

SQL_SET_QTY_SHIPPED = """
UPDATE transfer_item SET qty_shipped = qty_requested WHERE transfer_id = $1 AND line_no = $2
"""

SQL_SET_QTY_RECEIVED = """
UPDATE transfer_item SET qty_received = qty_requested WHERE transfer_id = $1 AND line_no = $2
"""

# /decline: HQ gọi /cancel kho nguồn với thử lại F5 (5 × 2 + 4 × 2 = 18 giây) rồi mới trả lời,
# nên kho đích chờ đủ chừng ấy và không tự gửi lại (WMS chờ 20 giây, F11).
DECLINE_FORWARD_TIMEOUT_S = (
    HTTP_MAX_RETRIES + 1
) * HTTP_TIMEOUT_S + HTTP_MAX_RETRIES * HTTP_RETRY_INTERVAL_S


class AlreadyDone(Exception):
    """Bước đã chạy trước đó (gửi trùng): thoát khối transaction, trả duplicate (F5)."""

    def __init__(self, status: TransferStatus, at: datetime) -> None:
        super().__init__(status)
        self.status = status
        self.at = at


def passthrough(resp: httpx.Response) -> JSONResponse:
    """Trả nguyên phản hồi lỗi của bên được gọi (F5)."""
    try:
        content = resp.json()
    except ValueError:
        content = {"error_code": ErrorCode.DC_UNREACHABLE, "message": resp.text, "details": {}}
    return JSONResponse(status_code=resp.status_code, content=content)


async def forward_to_hq(request: Request, path: str, body: Any) -> JSONResponse | None:
    """Callback lên HQ sau COMMIT (F6, F5). Lỗi mạng sau khi thử lại: 503 DC_UNREACHABLE.

    HQ trả 4xx/5xx thì trả nguyên cho WMS (F5); trả None khi HQ nhận (2xx).
    """
    try:
        resp = await request.app.state.hq.post(path, json=body)
    except httpx.HTTPError as exc:
        raise AppError(
            ErrorCode.DC_UNREACHABLE,
            "Không gọi được HQ; bấm lại thao tác, kho sẽ nhận ra lệnh trùng",
            {"path": path, "error": type(exc).__name__},
        ) from exc
    if resp.status_code >= 400:
        return passthrough(resp)
    return None


async def order_lines(conn: asyncpg.Connection, transfer_id: UUID) -> list[asyncpg.Record]:
    """Dòng hàng theo product_id tăng dần: cùng thứ tự khóa inventory với notice, cancel."""
    return sorted(await conn.fetch(SQL_ORDER_LINES, transfer_id), key=lambda r: r["product_id"])


async def order_state(
    conn: asyncpg.Connection, transfer_id: UUID, direction: TransferDirection
) -> asyncpg.Record:
    row = await conn.fetchrow(SQL_ORDER_STATE, transfer_id, direction)
    if row is None:
        raise AppError(
            ErrorCode.NOT_FOUND, "Không có phiếu ở kho này", {"transfer_id": str(transfer_id)}
        )
    return row


@router.post("/transfers/{transfer_id}/ship", response_model=TransferStatusResponse)
async def ship(
    transfer_id: UUID, body: TransferActionRequest, request: Request
) -> TransferStatusResponse | JSONResponse:
    """F6, F10.c: kho nguồn xuất hàng; COMMIT rồi mới gọi HQ /shipped."""
    dc_id: int = request.app.state.dc_id
    async with request.app.state.pool.acquire() as conn:
        try:
            async with conn.transaction():
                await conn.execute(SQL_SET_APP_SOURCE)
                # Bước 1
                order = await conn.fetchrow(
                    SQL_SHIP_ORDER,
                    TransferStatus.SHIPPED,
                    transfer_id,
                    TransferDirection.OUTBOUND,
                    TransferStatus.CREATED,
                )
                if order is None:
                    current = await order_state(conn, transfer_id, TransferDirection.OUTBOUND)
                    if current["status"] in (TransferStatus.SHIPPED, TransferStatus.RECEIVED):
                        raise AlreadyDone(TransferStatus(current["status"]), current["shipped_at"])
                    raise AppError(
                        ErrorCode.INVALID_STATE,
                        "Phiếu không còn ở trạng thái chờ xuất",
                        {"status": current["status"]},
                    )
                # Bước 2: trừ tồn và giữ chỗ từng dòng, ghi sổ TRANSFER_OUT, qty_shipped
                moved = []
                for line in await order_lines(conn, transfer_id):
                    request_id = transfer_movement_request_id(
                        transfer_id, MovementType.TRANSFER_OUT, line["line_no"]
                    )
                    row = await stock.consume(
                        conn,
                        dc_id=dc_id,
                        product_id=line["product_id"],
                        qty=line["qty_requested"],
                        request_id=request_id,
                        created_by=body.created_by,
                        movement_type=MovementType.TRANSFER_OUT,
                        ref_id=str(transfer_id),
                    )
                    await conn.execute(SQL_SET_QTY_SHIPPED, transfer_id, line["line_no"])
                    moved.append((line["product_id"], row, request_id))
                # Bước 3
                for product_id, row, request_id in moved:
                    await write_inventory_event(
                        conn, dc_id, product_id, row,
                        request_id=request_id, ref_type=RefType.TRANSFER,
                    )  # fmt: skip
                await write_transfer_event(
                    conn,
                    dc_id,
                    transfer_id,
                    order["transfer_code"],
                    TransferDirection.OUTBOUND,
                    TransferStatus.SHIPPED,
                    shipped_at=order["shipped_at"],
                )
        except AlreadyDone as done:
            # Gửi trùng: không trừ kho lần nữa, gửi lại callback (F10.c bước 1)
            error = await forward_to_hq(
                request,
                f"/transfers/{transfer_id}/shipped",
                ShippedCallback(shipped_at=done.at).model_dump(mode="json"),
            )
            return error or TransferStatusResponse(status=done.status, duplicate=True)
    # Sau COMMIT mới gọi HQ (không gọi HTTP trong transaction)
    error = await forward_to_hq(
        request,
        f"/transfers/{transfer_id}/shipped",
        ShippedCallback(shipped_at=order["shipped_at"]).model_dump(mode="json"),
    )
    return error or TransferStatusResponse(status=TransferStatus.SHIPPED)


@router.post("/transfers/{transfer_id}/receive", response_model=TransferStatusResponse)
async def receive(
    transfer_id: UUID, body: TransferActionRequest, request: Request
) -> TransferStatusResponse | JSONResponse:
    """F6, F10.d: kho đích nhận hàng khi bản INBOUND đã SHIPPED; COMMIT rồi gọi HQ /received."""
    dc_id: int = request.app.state.dc_id
    async with request.app.state.pool.acquire() as conn:
        try:
            async with conn.transaction():
                await conn.execute(SQL_SET_APP_SOURCE)
                # Bước 1, 2: chỉ bản INBOUND đang SHIPPED (HQ đã đồng bộ qua /sync-status)
                order = await conn.fetchrow(
                    SQL_RECEIVE_ORDER,
                    TransferStatus.RECEIVED,
                    transfer_id,
                    TransferDirection.INBOUND,
                    TransferStatus.SHIPPED,
                )
                if order is None:
                    current = await order_state(conn, transfer_id, TransferDirection.INBOUND)
                    if current["status"] == TransferStatus.RECEIVED:
                        raise AlreadyDone(TransferStatus.RECEIVED, current["received_at"])
                    raise AppError(
                        ErrorCode.INVALID_STATE,
                        "Phiếu chưa được kho nguồn xuất (chưa SHIPPED)",
                        {"status": current["status"]},
                    )
                # Bước 3: tăng tồn từng dòng, ghi sổ TRANSFER_IN, qty_received
                moved = []
                for line in await order_lines(conn, transfer_id):
                    request_id = transfer_movement_request_id(
                        transfer_id, MovementType.TRANSFER_IN, line["line_no"]
                    )
                    row = await stock.add_stock(
                        conn,
                        dc_id=dc_id,
                        product_id=line["product_id"],
                        qty=line["qty_requested"],
                        request_id=request_id,
                        created_by=body.created_by,
                        movement_type=MovementType.TRANSFER_IN,
                        ref_id=str(transfer_id),
                    )
                    await conn.execute(SQL_SET_QTY_RECEIVED, transfer_id, line["line_no"])
                    moved.append((line["product_id"], row, request_id))
                # Bước 4
                for product_id, row, request_id in moved:
                    await write_inventory_event(
                        conn, dc_id, product_id, row,
                        request_id=request_id, ref_type=RefType.TRANSFER,
                    )  # fmt: skip
                await write_transfer_event(
                    conn,
                    dc_id,
                    transfer_id,
                    order["transfer_code"],
                    TransferDirection.INBOUND,
                    TransferStatus.RECEIVED,
                    shipped_at=order["shipped_at"],
                    received_at=order["received_at"],
                )
        except AlreadyDone as done:
            error = await forward_to_hq(
                request,
                f"/transfers/{transfer_id}/received",
                ReceivedCallback(received_at=done.at).model_dump(mode="json"),
            )
            return error or TransferStatusResponse(status=done.status, duplicate=True)
    error = await forward_to_hq(
        request,
        f"/transfers/{transfer_id}/received",
        ReceivedCallback(received_at=order["received_at"]).model_dump(mode="json"),
    )
    return error or TransferStatusResponse(status=TransferStatus.RECEIVED)


@router.post("/transfers/{transfer_id}/decline")
async def decline(transfer_id: UUID, body: DeclineRequest, request: Request) -> JSONResponse:
    """F6: chuyển tiếp đồng bộ tới HQ /decline, trả nguyên kết quả của HQ (F10.g)."""
    try:
        resp = await request.app.state.hq.post(
            f"/transfers/{transfer_id}/decline",
            json=body.model_dump(mode="json"),
            request_timeout=DECLINE_FORWARD_TIMEOUT_S,
            max_retries=0,
        )
    except httpx.HTTPError as exc:
        raise AppError(
            ErrorCode.DC_UNREACHABLE, "Không gọi được HQ", {"error": type(exc).__name__}
        ) from exc
    return passthrough(resp)
