"""Nơi DUY NHẤT chứa SQL thay đổi inventory, stock_movement, outbox_event – F10.a–e (Tuấn Minh).

Các hàm nhận kết nối asyncpg đang nằm trong transaction do router mở; không hàm nào tự mở
transaction, tự COMMIT hay gọi HTTP. reserve, release, write_outbox có chữ ký đúng như
node-api/routers/transfers.py (Minh Hiếu) gọi.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from common.constants import (
    UNSAFE_READ_WRITE_DELAY_S,
    VERSION_CONFLICT_MAX_RETRIES,
    AggregateType,
    ErrorCode,
    EventType,
    LockMode,
    MovementType,
    RefType,
)
from common.errors import AppError

log = logging.getLogger("node-api")


@dataclass(frozen=True)
class StockResult:
    """Dòng inventory sau khi đổi (RETURNING ở F10.a bước 2)."""

    qty_on_hand: int
    qty_reserved: int
    version: int
    duplicate: bool = False

    def __getitem__(self, key: str) -> Any:
        """Đọc kiểu row["qty_on_hand"], như dòng RETURNING mà reserve và release trả về."""
        return getattr(self, key)


class DuplicateRequest(Exception):
    """F10.a bước 3: stock_movement đã có request_id này; router ROLLBACK và trả duplicate."""


def _result(row: asyncpg.Record | dict[str, Any]) -> StockResult:
    return StockResult(row["qty_on_hand"], row["qty_reserved"], row["version"])


# ---------------------------------------------------------------------------
# Hàm đọc
# ---------------------------------------------------------------------------
async def movement_exists(conn: asyncpg.Connection, request_id: UUID) -> bool:
    """F10.a bước 1: lệnh này đã được áp dụng chưa."""
    found = await conn.fetchval("SELECT 1 FROM stock_movement WHERE request_id = $1", request_id)
    return found is not None


async def product_exists(conn: asyncpg.Connection, product_id: int) -> bool:
    """SKU có trong bản sao danh mục (F5: SKU không tồn tại là VALIDATION_ERROR / NOT_FOUND)."""
    found = await conn.fetchval("SELECT 1 FROM product WHERE product_id = $1", product_id)
    return found is not None


async def current_stock(
    conn: asyncpg.Connection, dc_id: int, product_id: int
) -> StockResult | None:
    row = await conn.fetchrow(
        "SELECT qty_on_hand, qty_reserved, version FROM inventory "
        "WHERE dc_id = $1 AND product_id = $2",
        dc_id,
        product_id,
    )
    return None if row is None else _result(row)


async def list_inventory(
    conn: asyncpg.Connection, dc_id: int, product_id: int | None = None
) -> list[asyncpg.Record]:
    """Các dòng inventory của kho; product_id None thì lấy hết (GET /inventory, F6)."""
    return await conn.fetch(
        "SELECT product_id, qty_on_hand, qty_reserved, version FROM inventory "
        "WHERE dc_id = $1 AND ($2::bigint IS NULL OR product_id = $2) ORDER BY product_id",
        dc_id,
        product_id,
    )


async def _read_version(
    conn: asyncpg.Connection, dc_id: int, product_id: int
) -> asyncpg.Record | None:
    """Đọc không khóa (khóa lạc quan, db.md 2.7)."""
    return await conn.fetchrow(
        "SELECT version, qty_on_hand, qty_reserved FROM inventory "
        "WHERE dc_id = $1 AND product_id = $2",
        dc_id,
        product_id,
    )


# ---------------------------------------------------------------------------
# Hàm ghi
# ---------------------------------------------------------------------------
async def _insert_movement(
    conn: asyncpg.Connection,
    *,
    request_id: UUID,
    dc_id: int,
    product_id: int,
    movement_type: MovementType,
    qty_change: int,
    ref_type: RefType,
    created_by: str,
    ref_id: str | None = None,
) -> None:
    """F10.a bước 3. Không ghi được dòng nào là trùng request_id (chốt chặn cuối)."""
    row = await conn.fetchrow(
        "INSERT INTO stock_movement "
        "(request_id, dc_id, product_id, movement_type, qty_change, ref_type, ref_id, created_by) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
        "ON CONFLICT (request_id) DO NOTHING RETURNING movement_id",
        request_id,
        dc_id,
        product_id,
        movement_type,
        qty_change,
        ref_type,
        ref_id,
        created_by,
    )
    if row is None:
        raise DuplicateRequest(str(request_id))


async def add_stock(
    conn: asyncpg.Connection,
    *,
    dc_id: int,
    product_id: int,
    qty: int,
    request_id: UUID,
    created_by: str,
    ref_type: RefType | None = None,
    movement_type: MovementType = MovementType.INBOUND,
    ref_id: str | None = None,
) -> StockResult:
    """Nhập kho rồi ghi sổ (F10.a bước 2 nhánh nhập, bước 3). inventory trước sổ vì khóa ngoại.

    movement_type INBOUND (mặc định): ref_type mặc định RECEIPT.
    movement_type TRANSFER_IN (F10.d bước 3, nhận điều chuyển): ref_type mặc định TRANSFER.
    ref_id là mã chứng từ ghi vào sổ (db.md 2.8), ví dụ transfer_id.
    """
    if movement_type == MovementType.INBOUND:
        ref_type = ref_type or RefType.RECEIPT
    elif movement_type == MovementType.TRANSFER_IN:
        ref_type = ref_type or RefType.TRANSFER
    else:
        raise ValueError(f"add_stock không xử lý movement_type {movement_type}")
    row = await conn.fetchrow(
        "INSERT INTO inventory (dc_id, product_id, qty_on_hand) VALUES ($1, $2, $3) "
        "ON CONFLICT (dc_id, product_id) DO UPDATE "
        "SET qty_on_hand = inventory.qty_on_hand + EXCLUDED.qty_on_hand, updated_at = now() "
        "RETURNING qty_on_hand, qty_reserved, version",
        dc_id,
        product_id,
        qty,
    )
    assert row is not None  # upsert luôn trả đúng một dòng
    await _insert_movement(
        conn,
        request_id=request_id,
        dc_id=dc_id,
        product_id=product_id,
        movement_type=movement_type,
        qty_change=qty,
        ref_type=ref_type,
        created_by=created_by,
        ref_id=ref_id,
    )
    return _result(row)


# Ba câu UPDATE có điều kiện version (db.md 2.7): $1 qty, $2 dc_id, $3 product_id, $4 version
_SQL_CONSUME = (
    "UPDATE inventory SET qty_on_hand = qty_on_hand - $1, updated_at = now() "
    "WHERE dc_id = $2 AND product_id = $3 AND version = $4 "
    "AND qty_on_hand - qty_reserved >= $1 "
    "RETURNING qty_on_hand, qty_reserved, version"
)
_SQL_RESERVE = (
    "UPDATE inventory SET qty_reserved = qty_reserved + $1, updated_at = now() "
    "WHERE dc_id = $2 AND product_id = $3 AND version = $4 "
    "AND qty_on_hand - qty_reserved >= $1 "
    "RETURNING qty_on_hand, qty_reserved, version"
)
_SQL_TRANSFER_OUT = (
    "UPDATE inventory SET qty_on_hand = qty_on_hand - $1, qty_reserved = qty_reserved - $1, "
    "updated_at = now() "
    "WHERE dc_id = $2 AND product_id = $3 AND version = $4 "
    "AND qty_reserved >= $1 "
    "RETURNING qty_on_hand, qty_reserved, version"
)
_SQL_RELEASE = (
    "UPDATE inventory SET qty_reserved = qty_reserved - $1, updated_at = now() "
    "WHERE dc_id = $2 AND product_id = $3 AND version = $4 "
    "AND qty_reserved >= $1 "
    "RETURNING qty_on_hand, qty_reserved, version"
)


def _insufficient(dc_id: int, product_id: int, qty: int, row: asyncpg.Record | None) -> AppError:
    available = 0 if row is None else row["qty_on_hand"] - row["qty_reserved"]
    log.warning(
        "INSUFFICIENT_STOCK kho %s SKU %s: cần %s, khả dụng %s", dc_id, product_id, qty, available
    )
    return AppError(
        ErrorCode.INSUFFICIENT_STOCK,
        "Không đủ hàng khả dụng",
        {"product_id": product_id, "requested": qty, "available": available},
    )


def _reserved_too_small(
    dc_id: int, product_id: int, qty: int, row: asyncpg.Record | None
) -> AppError:
    """Giữ chỗ không đủ cho thao tác: 422 INSUFFICIENT_STOCK (F10.c bước 2).

    Không dùng 409 INVALID_STATE: F10.c, F10.g dành mã đó cho phiếu đã CANCELLED/đã xuất hàng.
    """
    reserved = 0 if row is None else row["qty_reserved"]
    log.warning(
        "INSUFFICIENT_STOCK kho %s SKU %s: cần %s, đang giữ %s", dc_id, product_id, qty, reserved
    )
    return AppError(
        ErrorCode.INSUFFICIENT_STOCK,
        "Số lượng giữ chỗ không đủ",
        {"product_id": product_id, "requested": qty, "reserved": reserved},
    )


async def _update_optimistic(
    conn: asyncpg.Connection,
    sql: str,
    dc_id: int,
    product_id: int,
    qty: int,
    on_unchanged: Callable[[int, int, int, asyncpg.Record | None], AppError],
) -> asyncpg.Record:
    """Đọc version, UPDATE có điều kiện, không ra dòng thì đọc lại (db.md 2.7).

    Version đã đổi thì thử lại (tối đa VERSION_CONFLICT_MAX_RETRIES lần sau lần đầu); version vẫn
    như cũ nghĩa là điều kiện khả dụng không thỏa, lỗi do on_unchanged quyết định.
    """
    current = await _read_version(conn, dc_id, product_id)
    if current is None:  # chưa có dòng tồn
        raise on_unchanged(dc_id, product_id, qty, None)
    for _attempt in range(1 + VERSION_CONFLICT_MAX_RETRIES):
        row = await conn.fetchrow(sql, qty, dc_id, product_id, current["version"])
        if row is not None:
            return row
        fresh = await _read_version(conn, dc_id, product_id)
        if fresh is None or fresh["version"] == current["version"]:
            raise on_unchanged(dc_id, product_id, qty, fresh)
        log.warning(
            "VERSION_CONFLICT kho %s SKU %s: version %s -> %s, thử lại",
            dc_id,
            product_id,
            current["version"],
            fresh["version"],
        )
        current = fresh
    raise AppError(
        ErrorCode.VERSION_CONFLICT,
        "Xung đột version, hết số lần thử lại",
        {"product_id": product_id, "retries": VERSION_CONFLICT_MAX_RETRIES},
    )


async def _overwrite_unsafe(
    conn: asyncpg.Connection, dc_id: int, product_id: int, qty: int
) -> asyncpg.Record:
    """LOCK_MODE=none (F2), chỉ cho make race-unsafe: đọc rồi ghi đè, không kiểm tra version.

    Hai lệnh đọc cùng một tồn rồi cùng ghi đè thì một lệnh bị mất (lost update, TC04). Cố ý
    khác quy tắc "UPDATE inventory luôn có điều kiện version" để chứng minh vì sao cần khóa.
    """
    current = await _read_version(conn, dc_id, product_id)
    if current is None or current["qty_on_hand"] - current["qty_reserved"] < qty:
        raise _insufficient(dc_id, product_id, qty, current)
    # F11: chờ giữa đọc và ghi để lệnh song song kịp đọc cùng số tồn, lost update luôn tái hiện
    await asyncio.sleep(UNSAFE_READ_WRITE_DELAY_S)
    row = await conn.fetchrow(
        "UPDATE inventory SET qty_on_hand = $1, updated_at = now() "
        "WHERE dc_id = $2 AND product_id = $3 "
        "RETURNING qty_on_hand, qty_reserved, version",
        current["qty_on_hand"] - qty,
        dc_id,
        product_id,
    )
    assert row is not None  # dòng đã đọc được ở trên
    return row


async def consume(
    conn: asyncpg.Connection,
    *,
    dc_id: int,
    product_id: int,
    qty: int,
    request_id: UUID,
    created_by: str,
    ref_type: RefType | None = None,
    lock_mode: LockMode = LockMode.OPTIMISTIC,
    movement_type: MovementType = MovementType.OUTBOUND,
    ref_id: str | None = None,
) -> StockResult:
    """Xuất kho, khóa lạc quan theo version (db.md 2.7), rồi ghi sổ (F10.a bước 2, 3).

    movement_type OUTBOUND (mặc định): trừ qty_on_hand, điều kiện khả dụng; ref_type mặc định ISSUE;
    lock_mode NONE chỉ áp dụng cho nhánh này (make race-unsafe).
    movement_type TRANSFER_OUT (F10.c bước 2): trừ cả qty_on_hand và qty_reserved cùng qty, điều
    kiện qty_reserved >= qty; ref_type mặc định TRANSFER; giữ chỗ không đủ: 422 INSUFFICIENT_STOCK.
    ref_id là mã chứng từ ghi vào sổ (db.md 2.8), ví dụ transfer_id.
    """
    if movement_type == MovementType.TRANSFER_OUT:
        row = await _update_optimistic(
            conn, _SQL_TRANSFER_OUT, dc_id, product_id, qty, _reserved_too_small
        )
        ref_type = ref_type or RefType.TRANSFER
    elif movement_type == MovementType.OUTBOUND:
        if lock_mode == LockMode.NONE:
            row = await _overwrite_unsafe(conn, dc_id, product_id, qty)
        else:
            row = await _update_optimistic(
                conn, _SQL_CONSUME, dc_id, product_id, qty, _insufficient
            )
        ref_type = ref_type or RefType.ISSUE
    else:
        raise ValueError(f"consume không xử lý movement_type {movement_type}")
    await _insert_movement(
        conn,
        request_id=request_id,
        dc_id=dc_id,
        product_id=product_id,
        movement_type=movement_type,
        qty_change=-qty,
        ref_type=ref_type,
        created_by=created_by,
        ref_id=ref_id,
    )
    return _result(row)


async def reserve(
    conn: asyncpg.Connection, dc_id: int, product_id: int, qty: int
) -> asyncpg.Record:
    """F10.b bước 3: giữ chỗ qty thùng. Trả dòng RETURNING (version, qty_on_hand, qty_reserved).

    Thiếu hàng khả dụng: INSUFFICIENT_STOCK (422). Hết số lần thử lại: VERSION_CONFLICT (409).
    Không ghi sổ: giữ chỗ không đổi qty_on_hand; người gọi ghi outbox.
    """
    return await _update_optimistic(conn, _SQL_RESERVE, dc_id, product_id, qty, _insufficient)


async def release(
    conn: asyncpg.Connection, dc_id: int, product_id: int, qty: int
) -> asyncpg.Record:
    """F10.e bước 2: nhả qty thùng giữ chỗ. Trả dòng RETURNING như reserve.

    Nhả quá số đang giữ hoặc chưa có dòng tồn: INSUFFICIENT_STOCK (422).
    """
    return await _update_optimistic(conn, _SQL_RELEASE, dc_id, product_id, qty, _reserved_too_small)


async def write_outbox(
    conn: asyncpg.Connection,
    dc_id: int,
    aggregate_type: AggregateType | str,
    aggregate_id: str,
    event_type: EventType | str,
    payload: Mapping[str, Any],
) -> None:
    """F10.a bước 4: một dòng outbox_event cùng transaction (F8, db.md 2.11).

    event_id và occurred_at sinh ở kho (gen_random_uuid, clock_timestamp); event_id, event_type,
    source_dc, occurred_at được ghi vào payload và thắng khóa trùng của người gọi.
    """
    await conn.execute(
        "INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload) "
        "SELECT e.id, $1::text, $2::text, $3::text, "
        "$4::jsonb || jsonb_build_object('event_id', e.id, 'event_type', $3::text, "
        "'source_dc', $5::int, 'occurred_at', clock_timestamp()) "
        "FROM (SELECT gen_random_uuid() AS id) e",
        str(aggregate_type),
        aggregate_id,
        str(event_type),
        json.dumps(payload, default=str),
        dc_id,
    )
