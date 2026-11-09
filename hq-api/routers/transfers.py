"""POST /transfers, GET /transfers, callback shipped/received, decline, nút Hủy phiếu.

F7, F9, F10.f, F10.g; db.md 2.5, 2.13, 3.1, 3.2 (Minh Hiếu). HQ điều phối Saga: chỉ HQ API ghi
transfer_summary; mọi lần đổi trạng thái có điều kiện trạng thái liền trước và ghi transfer_event
trong cùng transaction, chỉ khi đổi được đúng một dòng. Không gọi HTTP trong transaction.

GIẢ ĐỊNH: /sync-status xuống kho chạy nền sau khi trả lời (FastAPI BackgroundTasks). Kho gọi
/shipped, /received với timeout 2 giây (F5); nếu HQ chờ đồng bộ một kho đang chết (tới 18 giây)
thì kho gửi lại liên tục. Đồng bộ lỗi thì callback gửi lại (trùng) đồng bộ lại (limitations.md).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID, uuid4

import asyncpg
import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi.responses import JSONResponse

from common.constants import (
    CancelReason,
    DcStatus,
    ErrorCode,
    TransferDirection,
    TransferEventSource,
    TransferStatus,
)
from common.errors import AppError
from common.http import InternalClient
from common.ids import make_transfer_code
from common.models import (
    CancelRequest,
    CreateTransferRequest,
    CreateTransferResponse,
    DeclineRequest,
    ReceivedCallback,
    ShippedCallback,
    SyncStatusRequest,
    TransferNoticeItem,
    TransferNoticeRequest,
    TransferStatusResponse,
    TransferSummary,
)

router = APIRouter()
log = logging.getLogger("hq-api.transfers")

# F7, F10.g: kho đích từ chối gửi DECLINED, nút Hủy phiếu trên Dashboard gửi TIMEOUT
DECLINE_REASONS = (CancelReason.DECLINED, CancelReason.TIMEOUT)


# ---------------------------------------------------------------------------
# Lưu trữ transfer_summary, transfer_event (db.md 2.5, 2.13)
# ---------------------------------------------------------------------------
class TransferStore(Protocol):
    async def dc_statuses(self, dc_ids: list[int]) -> dict[int, str]: ...
    async def missing_products(self, product_ids: list[int]) -> list[int]: ...
    async def create(
        self, transfer_id: UUID, from_dc_id: int, to_dc_id: int, items: list[dict[str, int]]
    ) -> str: ...
    async def get(self, transfer_id: UUID) -> dict[str, Any] | None: ...
    async def list(self, status: TransferStatus | None) -> list[dict[str, Any]]: ...
    async def transition(
        self,
        transfer_id: UUID,
        from_status: TransferStatus,
        to_status: TransferStatus,
        source: TransferEventSource,
        *,
        shipped_at: datetime | None = None,
        received_at: datetime | None = None,
        cancel_reason: CancelReason | None = None,
    ) -> bool: ...
    async def mark_synced(self, transfer_id: UUID) -> None: ...


SUMMARY_COLUMNS = """transfer_id, transfer_code, from_dc_id, to_dc_id, status, total_lines,
    total_qty, items, cancel_reason, cancelled_at, last_sync_at, created_at, shipped_at,
    received_at, updated_at"""

SQL_INSERT_EVENT = """
INSERT INTO transfer_event (event_id, transfer_id, from_status, to_status, source)
VALUES (gen_random_uuid(), $1, $2, $3, $4)
"""

# db.md 3.2: trạng thái chỉ đi tới, WHERE status = trạng thái liền trước
SQL_TRANSITION = """
UPDATE transfer_summary
SET status        = $3,
    shipped_at    = COALESCE(shipped_at, $4),
    received_at   = COALESCE(received_at, $5),
    cancel_reason = COALESCE($6, cancel_reason),
    cancelled_at  = CASE WHEN $7 THEN now() ELSE cancelled_at END,
    updated_at    = now()
WHERE transfer_id = $1 AND status = $2
"""


def summary_row(row: asyncpg.Record) -> dict[str, Any]:
    data = dict(row)
    if isinstance(data["items"], str):  # asyncpg trả JSONB dạng chuỗi
        data["items"] = json.loads(data["items"])
    return data


class PgTransferStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def dc_statuses(self, dc_ids: list[int]) -> dict[int, str]:
        rows = await self.pool.fetch(
            "SELECT dc_id, status FROM dc_status WHERE dc_id = ANY($1::smallint[])", dc_ids
        )
        return {r["dc_id"]: r["status"] for r in rows}

    async def missing_products(self, product_ids: list[int]) -> list[int]:
        rows = await self.pool.fetch(
            """SELECT p FROM unnest($1::bigint[]) AS p
               WHERE NOT EXISTS (SELECT 1 FROM product WHERE product_id = p)""",
            product_ids,
        )
        return [r["p"] for r in rows]

    async def create(
        self, transfer_id: UUID, from_dc_id: int, to_dc_id: int, items: list[dict[str, int]]
    ) -> str:
        """F10.f bước 2: một transaction sinh mã, ghi phiếu CREATED và lịch sử NULL -> CREATED."""
        async with self.pool.acquire() as conn, conn.transaction():
            seq = await conn.fetchrow(
                "SELECT nextval('transfer_code_seq') AS seq, EXTRACT(YEAR FROM now())::int AS year"
            )
            code = make_transfer_code(seq["year"], seq["seq"])
            await conn.execute(
                """INSERT INTO transfer_summary
                   (transfer_id, transfer_code, from_dc_id, to_dc_id, status, total_lines,
                    total_qty, items, created_at, updated_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, now(), now())""",
                transfer_id,
                code,
                from_dc_id,
                to_dc_id,
                TransferStatus.CREATED,
                len(items),
                sum(i["qty"] for i in items),
                json.dumps(items),
            )
            await conn.execute(
                SQL_INSERT_EVENT,
                transfer_id,
                None,
                TransferStatus.CREATED,
                TransferEventSource.HQ,
            )
        return code

    async def get(self, transfer_id: UUID) -> dict[str, Any] | None:
        row = await self.pool.fetchrow(
            f"SELECT {SUMMARY_COLUMNS} FROM transfer_summary WHERE transfer_id = $1", transfer_id
        )
        return None if row is None else summary_row(row)

    async def list(self, status: TransferStatus | None) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            f"""SELECT {SUMMARY_COLUMNS} FROM transfer_summary
                WHERE ($1::text IS NULL OR status = $1)
                ORDER BY created_at DESC, transfer_code DESC""",
            None if status is None else str(status),
        )
        return [summary_row(r) for r in rows]

    async def transition(
        self,
        transfer_id: UUID,
        from_status: TransferStatus,
        to_status: TransferStatus,
        source: TransferEventSource,
        *,
        shipped_at: datetime | None = None,
        received_at: datetime | None = None,
        cancel_reason: CancelReason | None = None,
    ) -> bool:
        """Đổi trạng thái có điều kiện; ghi transfer_event chỉ khi đổi đúng 1 dòng (db.md 2.13)."""
        async with self.pool.acquire() as conn, conn.transaction():
            result = await conn.execute(
                SQL_TRANSITION,
                transfer_id,
                from_status,
                to_status,
                shipped_at,
                received_at,
                None if cancel_reason is None else str(cancel_reason),
                to_status == TransferStatus.CANCELLED,
            )
            if result != "UPDATE 1":
                return False
            await conn.execute(SQL_INSERT_EVENT, transfer_id, from_status, to_status, source)
        return True

    async def mark_synced(self, transfer_id: UUID) -> None:
        await self.pool.execute(
            "UPDATE transfer_summary SET last_sync_at = now() WHERE transfer_id = $1",
            transfer_id,
        )


def get_store(request: Request) -> TransferStore:
    return PgTransferStore(request.app.state.pool)


StoreDep = Depends(get_store)


# ---------------------------------------------------------------------------
# Gọi kho (client có timeout và thử lại F5 ở common/http.py)
# ---------------------------------------------------------------------------
def node(request: Request, dc_id: int) -> InternalClient:
    return request.app.state.nodes[dc_id]


async def call(client: InternalClient, path: str, body: Any) -> httpx.Response | None:
    """Gọi kho; None khi vẫn lỗi mạng sau khi thử lại (F5)."""
    try:
        return await client.post(path, json=body)
    except httpx.HTTPError as exc:
        log.warning("%s%s lỗi mạng sau khi thử lại: %r", client.base_url, path, exc)
        return None


def passthrough(resp: httpx.Response) -> JSONResponse:
    """F5: phản hồi 4xx/5xx của kho trả nguyên cho bên gọi."""
    try:
        content = resp.json()
    except ValueError:
        content = {"error_code": ErrorCode.DC_UNREACHABLE, "message": resp.text, "details": {}}
    return JSONResponse(status_code=resp.status_code, content=content)


def error_code_of(resp: httpx.Response) -> str | None:
    try:
        code = resp.json().get("error_code")
    except (ValueError, AttributeError):
        return None
    return code if isinstance(code, str) else None


def unreachable(dc_id: int, message: str) -> AppError:
    return AppError(ErrorCode.DC_UNREACHABLE, message, {"dc_id": dc_id})


async def sync_status(
    store: TransferStore, client: InternalClient, transfer_id: UUID, status: TransferStatus
) -> None:
    """HQ đồng bộ trạng thái phiếu xuống một kho (F6 /sync-status); chạy nền, lỗi chỉ ghi log."""
    resp = await call(
        client,
        f"/transfers/{transfer_id}/sync-status",
        SyncStatusRequest(status=status).model_dump(mode="json"),
    )
    if resp is not None and resp.status_code == 200:
        await store.mark_synced(transfer_id)
    else:
        log.warning("Đồng bộ %s phiếu %s xuống %s chưa được", status, transfer_id, client.base_url)


async def load(store: TransferStore, transfer_id: UUID) -> dict[str, Any]:
    row = await store.get(transfer_id)
    if row is None:
        raise AppError(ErrorCode.NOT_FOUND, "Không có phiếu", {"transfer_id": str(transfer_id)})
    return row


# ---------------------------------------------------------------------------
# POST /transfers (F7, F10.f)
# ---------------------------------------------------------------------------
@router.post("/transfers", status_code=201, response_model=CreateTransferResponse)
async def create_transfer(
    body: CreateTransferRequest,
    request: Request,
    background: BackgroundTasks,
    store: TransferStore = StoreDep,
) -> CreateTransferResponse | JSONResponse:
    product_ids = [i.product_id for i in body.items]
    if len(set(product_ids)) != len(product_ids):
        raise AppError(  # transfer_item UNIQUE (transfer_id, product_id), db.md 2.10
            ErrorCode.VALIDATION_ERROR, "Mỗi SKU chỉ một dòng", {"product_ids": product_ids}
        )
    missing = await store.missing_products(product_ids)
    if missing:  # F5: lệnh ghi có SKU không tồn tại trong danh mục
        raise AppError(
            ErrorCode.VALIDATION_ERROR, "SKU không có trong danh mục", {"missing": missing}
        )

    # Bước 1: hai kho phải cùng ONLINE, không thì không tạo phiếu
    dcs = [body.from_dc_id, body.to_dc_id]
    statuses = await store.dc_statuses(dcs)
    if any(statuses.get(dc) != DcStatus.ONLINE for dc in dcs):
        raise AppError(
            ErrorCode.DC_UNREACHABLE,
            "Kho nguồn hoặc kho đích không ONLINE, không tạo phiếu",
            {"dc_status": {str(dc): statuses.get(dc) for dc in dcs}},
        )

    # Bước 2: ghi trước, gọi sau (db.md 3.2 quy tắc 1)
    transfer_id = uuid4()
    items = [{"product_id": i.product_id, "qty": i.qty} for i in body.items]
    code = await store.create(transfer_id, body.from_dc_id, body.to_dc_id, items)

    def notice(direction: TransferDirection) -> dict[str, Any]:
        return TransferNoticeRequest(
            transfer_id=transfer_id,
            transfer_code=code,
            from_dc_id=body.from_dc_id,
            to_dc_id=body.to_dc_id,
            direction=direction,
            items=[
                TransferNoticeItem(line_no=n, product_id=i.product_id, qty=i.qty)
                for n, i in enumerate(body.items, start=1)
            ],
        ).model_dump(mode="json")

    # Bước 3: kho nguồn trước (giữ chỗ)
    source = await call(
        node(request, body.from_dc_id), "/transfers/notice", notice(TransferDirection.OUTBOUND)
    )
    if source is not None and source.status_code == 422:
        await store.transition(
            transfer_id,
            TransferStatus.CREATED,
            TransferStatus.CANCELLED,
            TransferEventSource.HQ,
            cancel_reason=CancelReason.INSUFFICIENT_STOCK,
        )
        return passthrough(source)
    if source is None or source.status_code != 200:
        return await compensate(request, store, background, transfer_id, body, source, False)

    # Bước 4: kho đích
    dest = await call(
        node(request, body.to_dc_id), "/transfers/notice", notice(TransferDirection.INBOUND)
    )
    if dest is None or dest.status_code != 200:
        return await compensate(request, store, background, transfer_id, body, dest, True)

    return CreateTransferResponse(
        transfer_id=transfer_id, transfer_code=code, status=TransferStatus.CREATED
    )


async def compensate(
    request: Request,
    store: TransferStore,
    background: BackgroundTasks,
    transfer_id: UUID,
    body: CreateTransferRequest,
    failed: httpx.Response | None,
    destination_contacted: bool,
) -> JSONResponse:
    """F10.f bước 3, 4: gọi /cancel kho nguồn (an toàn kể cả khi kho chưa nhận notice).

    /cancel trả 200 thì phiếu CANCELLED (TIMEOUT) và trả lỗi của kho (hoặc 503 nếu lỗi mạng);
    /cancel cũng lỗi thì phiếu giữ CREATED, trả 503 để nhân viên bấm Hủy phiếu sau (bước g).
    """
    cancel = await call(
        node(request, body.from_dc_id),
        f"/transfers/{transfer_id}/cancel",
        CancelRequest(reason=CancelReason.TIMEOUT).model_dump(mode="json"),
    )
    if cancel is None or cancel.status_code != 200:
        raise unreachable(
            body.from_dc_id,
            "Không hủy được phiếu ở kho nguồn; phiếu giữ CREATED, bấm Hủy phiếu khi kho "
            "liên lạc lại",
        )
    await store.transition(
        transfer_id,
        TransferStatus.CREATED,
        TransferStatus.CANCELLED,
        TransferEventSource.HQ,
        cancel_reason=CancelReason.TIMEOUT,
    )
    if destination_contacted:
        # Kho đích có thể đã ghi bản INBOUND trước khi lỗi: đồng bộ CANCELLED (bỏ qua nếu chưa có)
        background.add_task(
            sync_status,
            store,
            node(request, body.to_dc_id),
            transfer_id,
            TransferStatus.CANCELLED,
        )
    if failed is not None:
        return passthrough(failed)
    failed_dc = body.to_dc_id if destination_contacted else body.from_dc_id
    raise unreachable(failed_dc, "Không gọi được kho; phiếu đã hủy")


# ---------------------------------------------------------------------------
# GET /transfers, GET /transfers/{id} (F7)
# ---------------------------------------------------------------------------
@router.get("/transfers", response_model=list[TransferSummary])
async def list_transfers(
    status: TransferStatus | None = None, store: TransferStore = StoreDep
) -> list[dict[str, Any]]:
    """Danh sách phiếu (mảng JSON), lọc theo status nếu có."""
    return await store.list(status)


@router.get("/transfers/{transfer_id}", response_model=TransferSummary)
async def get_transfer(transfer_id: UUID, store: TransferStore = StoreDep) -> dict[str, Any]:
    return await load(store, transfer_id)


# ---------------------------------------------------------------------------
# Callback từ kho (F7, F9) và đồng bộ xuống kho còn lại
# ---------------------------------------------------------------------------
@router.post("/transfers/{transfer_id}/shipped", response_model=TransferStatusResponse)
async def shipped(
    transfer_id: UUID,
    body: ShippedCallback,
    request: Request,
    background: BackgroundTasks,
    store: TransferStore = StoreDep,
) -> TransferStatusResponse:
    """Kho nguồn báo đã xuất: CREATED -> SHIPPED, rồi đồng bộ SHIPPED xuống kho đích."""
    row = await load(store, transfer_id)
    moved = await store.transition(
        transfer_id,
        TransferStatus.CREATED,
        TransferStatus.SHIPPED,
        TransferEventSource.for_dc(row["from_dc_id"]),
        shipped_at=body.shipped_at,
    )
    status = (
        TransferStatus.SHIPPED
        if moved
        else TransferStatus((await load(store, transfer_id))["status"])
    )
    if status == TransferStatus.CANCELLED:
        raise AppError(ErrorCode.INVALID_STATE, "Phiếu đã hủy", {"status": status})
    if status == TransferStatus.SHIPPED:
        # Gửi lại cả khi callback trùng: sửa được lần đồng bộ trước bị lỗi
        background.add_task(sync_status, store, node(request, row["to_dc_id"]), transfer_id, status)
    return TransferStatusResponse(status=status, duplicate=not moved)


@router.post("/transfers/{transfer_id}/received", response_model=TransferStatusResponse)
async def received(
    transfer_id: UUID,
    body: ReceivedCallback,
    request: Request,
    background: BackgroundTasks,
    store: TransferStore = StoreDep,
) -> TransferStatusResponse:
    """Kho đích báo đã nhận: SHIPPED -> RECEIVED, rồi đồng bộ RECEIVED xuống kho nguồn."""
    row = await load(store, transfer_id)
    moved = await store.transition(
        transfer_id,
        TransferStatus.SHIPPED,
        TransferStatus.RECEIVED,
        TransferEventSource.for_dc(row["to_dc_id"]),
        received_at=body.received_at,
    )
    status = (
        TransferStatus.RECEIVED
        if moved
        else TransferStatus((await load(store, transfer_id))["status"])
    )
    if status != TransferStatus.RECEIVED:
        raise AppError(ErrorCode.INVALID_STATE, "Phiếu chưa SHIPPED", {"status": status})
    background.add_task(sync_status, store, node(request, row["from_dc_id"]), transfer_id, status)
    return TransferStatusResponse(status=status, duplicate=not moved)


# ---------------------------------------------------------------------------
# POST /transfers/{id}/decline (F7, F10.g): kho đích từ chối, hoặc nút Hủy phiếu
# ---------------------------------------------------------------------------
@router.post("/transfers/{transfer_id}/decline", response_model=TransferStatusResponse)
async def decline(
    transfer_id: UUID,
    body: DeclineRequest,
    request: Request,
    background: BackgroundTasks,
    store: TransferStore = StoreDep,
) -> TransferStatusResponse | JSONResponse:
    if body.reason not in DECLINE_REASONS:
        raise AppError(
            ErrorCode.VALIDATION_ERROR,
            "Lý do từ chối là DECLINED (kho đích) hoặc TIMEOUT (nút Hủy phiếu)",
            {"reason": body.reason},
        )
    row = await load(store, transfer_id)
    destination = node(request, row["to_dc_id"])
    if row["status"] == TransferStatus.CANCELLED:  # gửi trùng (F5)
        background.add_task(sync_status, store, destination, transfer_id, TransferStatus.CANCELLED)
        return TransferStatusResponse(status=TransferStatus.CANCELLED, duplicate=True)

    # Bước 1: kho nguồn quyết định có hủy được không (db.md 3.2 quy tắc 4)
    resp = await call(
        node(request, row["from_dc_id"]),
        f"/transfers/{transfer_id}/cancel",
        CancelRequest(reason=body.reason).model_dump(mode="json"),
    )
    if resp is None:  # Bước 3: không gọi được kho nguồn, phiếu giữ nguyên
        raise unreachable(row["from_dc_id"], "Không gọi được kho nguồn; phiếu giữ nguyên")
    if resp.status_code != 200:
        # Bước 3: 409 INVALID_STATE (đã xuất) hoặc lỗi khác (vd VERSION_CONFLICT) trả nguyên
        return passthrough(resp)

    # Bước 2: ghi CANCELLED (điều kiện CREATED) cùng lịch sử, rồi đồng bộ xuống kho đích
    moved = await store.transition(
        transfer_id,
        TransferStatus.CREATED,
        TransferStatus.CANCELLED,
        TransferEventSource.HQ,
        cancel_reason=body.reason,
    )
    current = TransferStatus((await load(store, transfer_id))["status"])
    if current != TransferStatus.CANCELLED:
        raise AppError(ErrorCode.INVALID_STATE, "Phiếu không còn CREATED", {"status": current})
    background.add_task(sync_status, store, destination, transfer_id, TransferStatus.CANCELLED)
    return TransferStatusResponse(status=TransferStatus.CANCELLED, duplicate=not moved)
