"""node-api/services/stock.py: add_stock, consume, write_outbox (F10.a bước 1–4; db.md 2.7–2.11).

Unit test với kết nối giả: kiểm tra thứ tự câu lệnh, điều kiện trong SQL và ánh xạ lỗi. Không
chứng minh SQL chạy đúng trên PostgreSQL; phần đó do test_tc01, test_tc02 (integration) kiểm.

Hợp đồng của stock.py (do Tuấn Minh đặt, Minh Hiếu gọi các hàm này):
    class DuplicateRequest(Exception)               # bước 3: stock_movement trùng request_id
    movement_exists(conn, request_id) -> bool       # bước 1
    product_exists(conn, product_id) -> bool
    current_stock(conn, dc_id, product_id) -> StockResult | None
    add_stock(conn, *, dc_id, product_id, qty, request_id, created_by, ref_type) -> StockResult
    consume(conn, *, dc_id, product_id, qty, request_id, created_by, ref_type) -> StockResult
    write_outbox(conn, *, dc_id, product_id, qty_on_hand, qty_reserved, version,
                 request_id, ref_type) -> None
StockResult có qty_on_hand, qty_reserved, version, duplicate (mặc định False).
"""

import re
import uuid

import pytest
from node_fakes import FakeConn
from services import stock

from common.constants import VERSION_CONFLICT_MAX_RETRIES, ErrorCode
from common.errors import AppError

DC_ID = 3
PRODUCT_ID = 1
REQ = uuid.UUID("11111111-1111-4111-8111-111111111111")
WHO = "tester"


def row(qty_on_hand: int, qty_reserved: int, version: int) -> dict[str, int]:
    return {"qty_on_hand": qty_on_hand, "qty_reserved": qty_reserved, "version": version}


def inv_row(version: int, qty_on_hand: int = 20, qty_reserved: int = 0) -> dict[str, int]:
    return row(qty_on_hand, qty_reserved, version)


MOVEMENT_OK = {"movement_id": 9}


# ---------------------------------------------------------------------------
# add_stock: nhập (F10.a bước 2 nhánh nhập, bước 3)
# ---------------------------------------------------------------------------
async def test_add_stock_upserts_inventory_before_movement() -> None:
    conn = FakeConn({"upsert_inventory": [row(20, 0, 2)], "insert_movement": [MOVEMENT_OK]})
    result = await stock.add_stock(
        conn, dc_id=DC_ID, product_id=PRODUCT_ID, qty=10, request_id=REQ,
        created_by=WHO, ref_type="RECEIPT",
    )  # fmt: skip
    # inventory trước stock_movement (khóa ngoại), không có bước nào khác
    assert conn.kinds() == ["upsert_inventory", "insert_movement"]
    assert (result.qty_on_hand, result.qty_reserved, result.version) == (20, 0, 2)
    assert result.duplicate is False


async def test_add_stock_upsert_sql_adds_quantity_and_never_sets_version() -> None:
    conn = FakeConn({"upsert_inventory": [row(20, 0, 2)], "insert_movement": [MOVEMENT_OK]})
    await stock.add_stock(
        conn, dc_id=DC_ID, product_id=PRODUCT_ID, qty=10, request_id=REQ,
        created_by=WHO, ref_type="RECEIPT",
    )  # fmt: skip
    sql = conn.calls_of("upsert_inventory")[0].sql
    assert "on conflict (dc_id, product_id) do update" in sql
    assert "qty_on_hand = inventory.qty_on_hand + excluded.qty_on_hand" in sql
    assert not re.search(r"version\s*=", sql)  # trigger tự tăng version, ứng dụng không đụng


async def test_add_stock_writes_inbound_movement_with_positive_change() -> None:
    conn = FakeConn({"upsert_inventory": [row(20, 0, 2)], "insert_movement": [MOVEMENT_OK]})
    await stock.add_stock(
        conn, dc_id=DC_ID, product_id=PRODUCT_ID, qty=10, request_id=REQ,
        created_by=WHO, ref_type="RECEIPT",
    )  # fmt: skip
    call = conn.calls_of("insert_movement")[0]
    assert "on conflict (request_id) do nothing" in call.sql
    assert str(REQ) in call.str_args
    assert "INBOUND" in call.str_args and "RECEIPT" in call.str_args
    assert 10 in call.args and WHO in call.args


async def test_add_stock_raises_duplicate_when_movement_conflicts() -> None:
    conn = FakeConn({"upsert_inventory": [row(20, 0, 2)], "insert_movement": [None]})
    with pytest.raises(stock.DuplicateRequest):
        await stock.add_stock(
            conn, dc_id=DC_ID, product_id=PRODUCT_ID, qty=10, request_id=REQ,
            created_by=WHO, ref_type="RECEIPT",
        )  # fmt: skip
    assert "insert_outbox" not in conn.kinds()


# ---------------------------------------------------------------------------
# consume: xuất (F10.a bước 2 nhánh xuất, db.md 2.7)
# ---------------------------------------------------------------------------
async def consume(conn: FakeConn, qty: int = 5):  # type: ignore[no-untyped-def]
    return await stock.consume(
        conn, dc_id=DC_ID, product_id=PRODUCT_ID, qty=qty, request_id=REQ,
        created_by=WHO, ref_type="ISSUE",
    )  # fmt: skip


async def test_consume_update_is_conditional_on_version_and_availability() -> None:
    conn = FakeConn(
        {
            "select_inventory": [inv_row(3)],
            "update_inventory": [row(15, 0, 4)],
            "insert_movement": [MOVEMENT_OK],
        }
    )
    result = await consume(conn, qty=5)
    kinds = conn.kinds()
    assert kinds.index("select_inventory") < kinds.index("update_inventory")
    assert kinds.index("update_inventory") < kinds.index("insert_movement")
    update = conn.calls_of("update_inventory")[0]
    assert re.search(r"version\s*=\s*\$\d", update.sql)  # điều kiện version
    assert re.search(r"qty_on_hand\s*-\s*qty_reserved\s*>=\s*\$\d", update.sql)  # khả dụng
    assert not re.search(r"version\s*=\s*(inventory\.)?version\s*\+", update.sql)
    assert 3 in update.args and 5 in update.args  # version vừa đọc, qty
    assert (result.qty_on_hand, result.version) == (15, 4)


async def test_consume_read_is_optimistic_not_a_row_lock() -> None:
    conn = FakeConn(
        {
            "select_inventory": [inv_row(3)],
            "update_inventory": [row(15, 0, 4)],
            "insert_movement": [MOVEMENT_OK],
        }
    )
    await consume(conn)
    assert "for update" not in conn.calls_of("select_inventory")[0].sql


async def test_consume_writes_outbound_movement_with_negative_change() -> None:
    conn = FakeConn(
        {
            "select_inventory": [inv_row(3)],
            "update_inventory": [row(15, 0, 4)],
            "insert_movement": [MOVEMENT_OK],
        }
    )
    await consume(conn, qty=5)
    call = conn.calls_of("insert_movement")[0]
    assert "on conflict (request_id) do nothing" in call.sql
    assert -5 in call.args
    assert "OUTBOUND" in call.str_args and "ISSUE" in call.str_args
    assert str(REQ) in call.str_args


async def test_consume_insufficient_stock_when_version_unchanged() -> None:
    # khả dụng 0: UPDATE không ra dòng, đọc lại thấy version vẫn 3 -> 422, không ghi sổ
    conn = FakeConn({"select_inventory": [inv_row(3, 20, 20)], "update_inventory": [None]})
    with pytest.raises(AppError) as exc:
        await consume(conn, qty=30)
    assert exc.value.error_code == ErrorCode.INSUFFICIENT_STOCK
    assert exc.value.status_code == 422
    assert "insert_movement" not in conn.kinds()


async def test_consume_insufficient_stock_when_no_inventory_row() -> None:
    # G2: SKU chưa có dòng tồn -> khả dụng 0 -> 422, không UPDATE
    conn = FakeConn({"select_inventory": [None]})
    with pytest.raises(AppError) as exc:
        await consume(conn)
    assert exc.value.error_code == ErrorCode.INSUFFICIENT_STOCK
    assert "update_inventory" not in conn.kinds()


async def test_consume_retries_when_version_changed_then_succeeds() -> None:
    # lệnh khác chen vào: UPDATE không ra dòng, version đọc lại là 4 (đã đổi) -> thử lại
    conn = FakeConn(
        {
            "select_inventory": [inv_row(3), inv_row(4)],
            "update_inventory": [None, row(15, 0, 5)],
            "insert_movement": [MOVEMENT_OK],
        }
    )
    result = await consume(conn)
    updates = conn.calls_of("update_inventory")
    assert len(updates) == 2
    assert 3 in updates[0].args and 4 in updates[1].args  # lần sau dùng version mới
    assert result.version == 5
    assert conn.kinds()[-1] == "insert_movement"


async def test_consume_version_conflict_after_retries_exhausted() -> None:
    # G1: 1 lần đầu + VERSION_CONFLICT_MAX_RETRIES lần thử lại; version đổi sau mỗi lần đọc
    versions = [inv_row(v) for v in range(3, 20)]
    conn = FakeConn({"select_inventory": versions, "update_inventory": [None]})
    with pytest.raises(AppError) as exc:
        await consume(conn)
    assert exc.value.error_code == ErrorCode.VERSION_CONFLICT
    assert exc.value.status_code == 409
    assert len(conn.calls_of("update_inventory")) == 1 + VERSION_CONFLICT_MAX_RETRIES
    assert "insert_movement" not in conn.kinds()


async def test_consume_raises_duplicate_when_movement_conflicts() -> None:
    conn = FakeConn(
        {
            "select_inventory": [inv_row(3)],
            "update_inventory": [row(15, 0, 4)],
            "insert_movement": [None],
        }
    )
    with pytest.raises(stock.DuplicateRequest):
        await consume(conn)


# ---------------------------------------------------------------------------
# write_outbox (F10.a bước 4, db.md 2.11, F8)
# ---------------------------------------------------------------------------
async def test_write_outbox_inserts_inventory_changed_event() -> None:
    conn = FakeConn()
    await stock.write_outbox(
        conn, dc_id=DC_ID, product_id=PRODUCT_ID, qty_on_hand=15, qty_reserved=2, version=7,
        request_id=REQ, ref_type="ISSUE",
    )  # fmt: skip
    assert conn.kinds() == ["insert_outbox"]
    call = conn.calls[0]
    blob = call.sql + " " + " ".join(call.str_args)
    assert "InventoryChanged" in blob and "INVENTORY" in blob
    assert "3:1" in call.str_args  # aggregate_id "dc_id:product_id" (common.ids)
    for value in (15, 2, 7):  # qty_on_hand, qty_reserved, version lấy từ RETURNING bước 2
        assert value in call.args
    assert str(REQ) in call.str_args and "ISSUE" in call.str_args


async def test_write_outbox_stamps_event_id_and_time_in_sql() -> None:
    # G6: event_id và occurred_at sinh ở kho, ghi vào cả payload (F8)
    conn = FakeConn()
    await stock.write_outbox(
        conn, dc_id=DC_ID, product_id=PRODUCT_ID, qty_on_hand=15, qty_reserved=0, version=7,
        request_id=REQ, ref_type="ISSUE",
    )  # fmt: skip
    sql = conn.calls[0].sql
    assert "gen_random_uuid()" in sql and "clock_timestamp()" in sql
    for key in ("event_id", "event_type", "source_dc", "occurred_at", "request_id"):
        assert key in sql


# ---------------------------------------------------------------------------
# Hàm đọc dùng ở bước 1 và để dựng response
# ---------------------------------------------------------------------------
async def test_movement_exists_checks_request_id_in_ledger() -> None:
    yes = FakeConn({"select_movement": [1]})
    assert await stock.movement_exists(yes, REQ) is True
    assert "where request_id" in yes.calls[0].sql
    assert str(REQ) in yes.calls[0].str_args
    assert await stock.movement_exists(FakeConn({"select_movement": [None]}), REQ) is False


async def test_product_exists_checks_catalog() -> None:
    conn = FakeConn({"select_product": [1]})
    assert await stock.product_exists(conn, PRODUCT_ID) is True
    assert await stock.product_exists(FakeConn({"select_product": [None]}), 99) is False


async def test_current_stock_returns_row_or_none() -> None:
    conn = FakeConn({"select_inventory": [inv_row(7, 20, 5)]})
    cur = await stock.current_stock(conn, DC_ID, PRODUCT_ID)
    assert (cur.qty_on_hand, cur.qty_reserved, cur.version) == (20, 5, 7)
    assert await stock.current_stock(FakeConn({"select_inventory": [None]}), DC_ID, 2) is None
