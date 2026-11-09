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
    reserve(conn, dc_id, product_id, qty) -> Record         # F10.b bước 3; dòng RETURNING
    release(conn, dc_id, product_id, qty) -> Record         # F10.e bước 2; dòng RETURNING
    write_outbox(conn, dc_id, aggregate_type, aggregate_id, event_type, payload) -> None
(chữ ký reserve, release, write_outbox đúng như node-api/routers/transfers.py của Minh Hiếu gọi)
StockResult có qty_on_hand, qty_reserved, version, duplicate (mặc định False).
"""

import inspect
import json
import re
import uuid

import pytest
from node_fakes import FakeConn
from services import stock

from common.constants import VERSION_CONFLICT_MAX_RETRIES, AggregateType, ErrorCode, EventType
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
# write_outbox tổng quát (F10.a bước 4, F8, db.md 2.11)
# ---------------------------------------------------------------------------
INVENTORY_PAYLOAD = {
    "product_id": PRODUCT_ID,
    "qty_on_hand": 15,
    "qty_reserved": 2,
    "version": 7,
    "request_id": REQ,
    "ref_type": "ISSUE",
}


def payload_arg(call) -> dict:  # type: ignore[no-untyped-def]
    """Tham số payload (chuỗi JSON) của câu INSERT outbox_event."""
    found = [json.loads(a) for a in call.args if isinstance(a, str) and a.startswith("{")]
    assert len(found) == 1
    return found[0]


async def test_write_outbox_inserts_inventory_changed_event() -> None:
    conn = FakeConn()
    await stock.write_outbox(
        conn, DC_ID, AggregateType.INVENTORY, "3:1", EventType.INVENTORY_CHANGED, INVENTORY_PAYLOAD
    )
    assert conn.kinds() == ["insert_outbox"]
    call = conn.calls[0]
    for value in ("INVENTORY", "3:1", "InventoryChanged"):
        assert value in call.str_args  # aggregate_type, aggregate_id (key Kafka), event_type
    assert DC_ID in call.args
    payload = payload_arg(call)
    assert payload["qty_on_hand"] == 15 and payload["qty_reserved"] == 2
    assert payload["version"] == 7 and payload["product_id"] == PRODUCT_ID
    assert payload["request_id"] == str(REQ) and payload["ref_type"] == "ISSUE"


async def test_write_outbox_stamps_event_id_and_time_in_sql() -> None:
    # F8: event_id, event_type, source_dc, occurred_at do kho điền, ghi vào cả payload
    conn = FakeConn()
    await stock.write_outbox(
        conn, DC_ID, AggregateType.INVENTORY, "3:1", EventType.INVENTORY_CHANGED, INVENTORY_PAYLOAD
    )
    sql = conn.calls[0].sql
    assert "gen_random_uuid()" in sql and "clock_timestamp()" in sql
    for key in ("event_id", "event_type", "source_dc", "occurred_at"):
        assert key in sql
    assert "event_id" not in payload_arg(conn.calls[0])  # không do người gọi truyền


async def test_write_outbox_serves_transfer_events() -> None:
    conn = FakeConn()
    transfer_id = uuid.UUID("44444444-4444-4444-8444-444444444444")
    await stock.write_outbox(
        conn,
        1,
        AggregateType.TRANSFER,
        str(transfer_id),
        EventType.TRANSFER_SHIPPED,
        {"transfer_id": transfer_id, "status": "SHIPPED", "shipped_at": None},
    )
    call = conn.calls[0]
    assert "TRANSFER" in call.str_args and str(transfer_id) in call.str_args
    assert "TransferShipped" in call.str_args
    assert payload_arg(call)["transfer_id"] == str(transfer_id)


# ---------------------------------------------------------------------------
# reserve, release (F10.b bước 3, F10.e bước 2, db.md 2.7)
# ---------------------------------------------------------------------------
async def test_reserve_update_is_conditional_and_returns_row() -> None:
    conn = FakeConn(
        {"select_inventory": [inv_row(3, 100, 0)], "update_inventory": [row(100, 50, 4)]}
    )
    result = await stock.reserve(conn, DC_ID, PRODUCT_ID, 50)
    update = conn.calls_of("update_inventory")[0]
    assert re.search(r"qty_reserved\s*=\s*qty_reserved\s*\+\s*\$\d", update.sql)
    assert re.search(r"version\s*=\s*\$\d", update.sql)
    assert re.search(r"qty_on_hand\s*-\s*qty_reserved\s*>=\s*\$\d", update.sql)
    assert not re.search(r"version\s*=\s*(inventory\.)?version\s*\+", update.sql)
    assert 3 in update.args and 50 in update.args
    assert (result["qty_on_hand"], result["qty_reserved"], result["version"]) == (100, 50, 4)
    assert conn.kinds() == ["select_inventory", "update_inventory"]  # không ghi sổ, không outbox


async def test_reserve_insufficient_stock_is_422() -> None:
    conn = FakeConn({"select_inventory": [inv_row(3, 100, 70)], "update_inventory": [None]})
    with pytest.raises(AppError) as exc:
        await stock.reserve(conn, DC_ID, PRODUCT_ID, 50)
    assert exc.value.error_code == ErrorCode.INSUFFICIENT_STOCK and exc.value.status_code == 422


async def test_reserve_without_inventory_row_is_422() -> None:
    conn = FakeConn({"select_inventory": [None]})
    with pytest.raises(AppError) as exc:
        await stock.reserve(conn, DC_ID, PRODUCT_ID, 50)
    assert exc.value.error_code == ErrorCode.INSUFFICIENT_STOCK
    assert "update_inventory" not in conn.kinds()


async def test_reserve_retries_then_succeeds() -> None:
    conn = FakeConn(
        {
            "select_inventory": [inv_row(3, 100, 0), inv_row(4, 100, 0)],
            "update_inventory": [None, row(100, 50, 5)],
        }
    )
    result = await stock.reserve(conn, DC_ID, PRODUCT_ID, 50)
    updates = conn.calls_of("update_inventory")
    assert len(updates) == 2 and 3 in updates[0].args and 4 in updates[1].args
    assert result["version"] == 5


async def test_reserve_version_conflict_after_retries_exhausted() -> None:
    conn = FakeConn(
        {"select_inventory": [inv_row(v, 100, 0) for v in range(3, 20)], "update_inventory": [None]}
    )
    with pytest.raises(AppError) as exc:
        await stock.reserve(conn, DC_ID, PRODUCT_ID, 50)
    assert exc.value.error_code == ErrorCode.VERSION_CONFLICT and exc.value.status_code == 409
    assert len(conn.calls_of("update_inventory")) == 1 + VERSION_CONFLICT_MAX_RETRIES


async def test_release_update_lowers_reserved_conditionally() -> None:
    conn = FakeConn(
        {"select_inventory": [inv_row(3, 100, 50)], "update_inventory": [row(100, 40, 4)]}
    )
    result = await stock.release(conn, DC_ID, PRODUCT_ID, 10)
    update = conn.calls_of("update_inventory")[0]
    assert re.search(r"qty_reserved\s*=\s*qty_reserved\s*-\s*\$\d", update.sql)
    assert re.search(r"version\s*=\s*\$\d", update.sql)
    assert re.search(r"qty_reserved\s*>=\s*\$\d", update.sql)
    assert not re.search(r"version\s*=\s*(inventory\.)?version\s*\+", update.sql)
    assert 3 in update.args and 10 in update.args
    assert (result["qty_reserved"], result["version"]) == (40, 4)
    assert conn.kinds() == ["select_inventory", "update_inventory"]


async def test_release_more_than_reserved_is_invalid_state() -> None:
    # H1: không nhả được quá số đang giữ -> 409 INVALID_STATE, không để CHECK của DB báo lỗi 500
    conn = FakeConn({"select_inventory": [inv_row(3, 100, 5)], "update_inventory": [None]})
    with pytest.raises(AppError) as exc:
        await stock.release(conn, DC_ID, PRODUCT_ID, 10)
    assert exc.value.error_code == ErrorCode.INVALID_STATE and exc.value.status_code == 409


async def test_release_without_inventory_row_is_invalid_state() -> None:
    conn = FakeConn({"select_inventory": [None]})  # H2
    with pytest.raises(AppError) as exc:
        await stock.release(conn, DC_ID, PRODUCT_ID, 10)
    assert exc.value.error_code == ErrorCode.INVALID_STATE
    assert "update_inventory" not in conn.kinds()


async def test_release_retries_and_gives_up_like_reserve() -> None:
    ok = FakeConn(
        {
            "select_inventory": [inv_row(3, 100, 50), inv_row(4, 100, 50)],
            "update_inventory": [None, row(100, 40, 5)],
        }
    )
    assert (await stock.release(ok, DC_ID, PRODUCT_ID, 10))["version"] == 5
    bad = FakeConn(
        {
            "select_inventory": [inv_row(v, 100, 50) for v in range(3, 20)],
            "update_inventory": [None],
        }
    )
    with pytest.raises(AppError) as exc:
        await stock.release(bad, DC_ID, PRODUCT_ID, 10)
    assert exc.value.error_code == ErrorCode.VERSION_CONFLICT
    assert len(bad.calls_of("update_inventory")) == 1 + VERSION_CONFLICT_MAX_RETRIES


def test_signatures_match_transfers_router_usage() -> None:
    # node-api/routers/transfers.py (Minh Hiếu) gọi đúng các dạng này; lệch là TypeError khi chạy
    inspect.signature(stock.write_outbox).bind(
        None, 1, AggregateType.INVENTORY, "1:1", EventType.INVENTORY_CHANGED, {}
    )
    inspect.signature(stock.reserve).bind(None, 1, 1, 5)
    inspect.signature(stock.release).bind(None, 1, 1, 5)


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
