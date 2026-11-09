"""Nơi DUY NHẤT chứa SQL thay đổi inventory, stock_movement, outbox_event – F10.a–e (Tuấn Minh).

Các hàm nhận kết nối asyncpg đang nằm trong transaction do router mở; không hàm nào tự mở
transaction hay gọi HTTP. Còn thiếu cho Saga (Minh Hiếu): reserve, release.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from common.constants import (
    VERSION_CONFLICT_MAX_RETRIES,
    AggregateType,
    ErrorCode,
    EventType,
    MovementType,
    RefType,
)
from common.errors import AppError
from common.ids import inventory_aggregate_id

log = logging.getLogger("node-api")


@dataclass(frozen=True)
class StockResult:
    """Dòng inventory sau khi đổi (RETURNING ở F10.a bước 2)."""

    qty_on_hand: int
    qty_reserved: int
    version: int
    duplicate: bool = False


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
) -> None:
    """F10.a bước 3. Không ghi được dòng nào là trùng request_id (chốt chặn cuối)."""
    row = await conn.fetchrow(
        "INSERT INTO stock_movement "
        "(request_id, dc_id, product_id, movement_type, qty_change, ref_type, created_by) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7) "
        "ON CONFLICT (request_id) DO NOTHING RETURNING movement_id",
        request_id,
        dc_id,
        product_id,
        movement_type,
        qty_change,
        ref_type,
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
    ref_type: RefType,
) -> StockResult:
    """F10.a bước 2 (nhập) và bước 3. inventory trước stock_movement vì khóa ngoại."""
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
        movement_type=MovementType.INBOUND,
        qty_change=qty,
        ref_type=ref_type,
        created_by=created_by,
    )
    return _result(row)


async def consume(
    conn: asyncpg.Connection,
    *,
    dc_id: int,
    product_id: int,
    qty: int,
    request_id: UUID,
    created_by: str,
    ref_type: RefType,
) -> StockResult:
    """F10.a bước 2 (xuất) và bước 3, khóa lạc quan theo version (db.md 2.7).

    UPDATE không ra dòng thì đọc lại: version đã đổi thì thử lại (tối đa
    VERSION_CONFLICT_MAX_RETRIES lần sau lần đầu), version vẫn như cũ nghĩa là không đủ hàng.
    """
    current = await _read_version(conn, dc_id, product_id)
    if current is None:  # chưa có dòng tồn: khả dụng 0
        raise _insufficient(dc_id, product_id, qty, 0)

    for _attempt in range(1 + VERSION_CONFLICT_MAX_RETRIES):
        row = await conn.fetchrow(
            "UPDATE inventory SET qty_on_hand = qty_on_hand - $1, updated_at = now() "
            "WHERE dc_id = $2 AND product_id = $3 AND version = $4 "
            "AND qty_on_hand - qty_reserved >= $1 "
            "RETURNING qty_on_hand, qty_reserved, version",
            qty,
            dc_id,
            product_id,
            current["version"],
        )
        if row is not None:
            break
        fresh = await _read_version(conn, dc_id, product_id)
        if fresh is None or fresh["version"] == current["version"]:
            available = 0 if fresh is None else fresh["qty_on_hand"] - fresh["qty_reserved"]
            raise _insufficient(dc_id, product_id, qty, available)
        log.warning(
            "VERSION_CONFLICT kho %s SKU %s: version %s -> %s, thử lại",
            dc_id,
            product_id,
            current["version"],
            fresh["version"],
        )
        current = fresh
    else:
        raise AppError(
            ErrorCode.VERSION_CONFLICT,
            "Xung đột version khi xuất hàng, hết số lần thử lại",
            {"product_id": product_id, "retries": VERSION_CONFLICT_MAX_RETRIES},
        )

    await _insert_movement(
        conn,
        request_id=request_id,
        dc_id=dc_id,
        product_id=product_id,
        movement_type=MovementType.OUTBOUND,
        qty_change=-qty,
        ref_type=ref_type,
        created_by=created_by,
    )
    return _result(row)


def _insufficient(dc_id: int, product_id: int, qty: int, available: int) -> AppError:
    log.warning(
        "INSUFFICIENT_STOCK kho %s SKU %s: cần %s, khả dụng %s", dc_id, product_id, qty, available
    )
    return AppError(
        ErrorCode.INSUFFICIENT_STOCK,
        "Không đủ hàng khả dụng",
        {"product_id": product_id, "requested": qty, "available": available},
    )


async def write_outbox(
    conn: asyncpg.Connection,
    *,
    dc_id: int,
    product_id: int,
    qty_on_hand: int,
    qty_reserved: int,
    version: int,
    request_id: UUID,
    ref_type: RefType,
) -> None:
    """F10.a bước 4: một sự kiện InventoryChanged (F8, db.md 2.11) cùng transaction.

    event_id và occurred_at sinh ở kho (gen_random_uuid, clock_timestamp), ghi cả vào payload.
    """
    await conn.execute(
        "INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload) "
        "SELECT e.id, $1::text, $2::text, $3::text, jsonb_build_object("
        "'event_id', e.id, 'event_type', $3::text, 'source_dc', $4::int, "
        "'occurred_at', clock_timestamp(), 'product_id', $5::bigint, "
        "'qty_on_hand', $6::int, 'qty_reserved', $7::int, 'version', $8::bigint, "
        "'request_id', $9::uuid, 'ref_type', $10::text) "
        "FROM (SELECT gen_random_uuid() AS id) e",
        AggregateType.INVENTORY,
        inventory_aggregate_id(dc_id, product_id),
        EventType.INVENTORY_CHANGED,
        dc_id,
        product_id,
        qty_on_hand,
        qty_reserved,
        version,
        request_id,
        ref_type,
    )
