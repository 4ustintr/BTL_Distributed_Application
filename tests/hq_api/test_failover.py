"""Unit test endpoint ghi hộ F10.h (POST /failover/dc/{dc_id}/…), hỗ trợ TC14, TC15. Không cần CSDL.

CSDL giả trong bộ nhớ, chỉ đủ cho các câu SQL của routers/failover.py.
Số liệu theo Kịch bản 3: snapshot Trung 35, nhập 30 -> 65, xuất 10 -> 55, xuất 100 -> 422.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fixtures import DEMO_IDS, SKU_A_PRODUCT_ID
from routers import failover
from routers.failover import provisional_on_hand

from common.constants import DcStatus, PendingTxnStatus, TxnType
from common.errors import install_error_handlers

DC_TRUNG = 2
REQ_KB3_IN = DEMO_IDS["REQ_KB3_IN"]
REQ_KB3_OUT = DEMO_IDS["REQ_KB3_OUT"]
REQ_KB3_OUT_BIG = DEMO_IDS["REQ_KB3_OUT_BIG"]


# ---------------------------------------------------------------------------
# CSDL giả
# ---------------------------------------------------------------------------
class FakeDb:
    def __init__(
        self,
        status: DcStatus,
        on_hand: int | None = 35,
        reserved: int = 0,
        products: frozenset[int] = frozenset({SKU_A_PRODUCT_ID}),
    ) -> None:
        self.dc_status = {DC_TRUNG: status}
        self.snapshot = (
            None if on_hand is None else {"qty_on_hand": on_hand, "qty_reserved": reserved}
        )
        self.products = products
        self.pending: dict[UUID, dict[str, Any]] = {}
        self.sql: list[str] = []
        # Giả lập lệnh trùng chen vào giữa bước 1 và bước 5: bước 1 không thấy các txn_id này
        self.hidden_from_dup_check: set[UUID] = set()

    def rows(self) -> list[dict[str, Any]]:
        return list(self.pending.values())

    def add(self, txn_id: UUID, txn_type: TxnType, qty: int, status: PendingTxnStatus) -> None:
        self.pending[txn_id] = {
            "txn_id": txn_id,
            "dc_id": DC_TRUNG,
            "txn_type": txn_type.value,
            "product_id": SKU_A_PRODUCT_ID,
            "qty": qty,
            "created_by": "nv-trung",
            "status": status.value,
        }


class FakeConn:
    def __init__(self, db: FakeDb) -> None:
        self.db = db

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        before = dict(self.db.pending)
        try:
            yield
        except BaseException:
            self.db.pending = before  # ROLLBACK
            raise

    async def fetchval(self, sql: str, *args: Any) -> Any:
        self.db.sql.append(sql)
        if "FROM dc_status" in sql:
            return self.db.dc_status.get(args[0])
        if "FROM product" in sql:
            return 1 if args[0] in self.db.products else None
        if sql.lstrip().startswith("INSERT INTO pending_txn"):
            txn_id, dc_id, txn_type, product_id, qty, created_by = args
            if txn_id in self.db.pending:
                return None  # ON CONFLICT (txn_id) DO NOTHING
            self.db.add(txn_id, TxnType(txn_type), qty, PendingTxnStatus.PENDING)
            self.db.pending[txn_id]["created_by"] = created_by
            return PendingTxnStatus.PENDING.value
        raise AssertionError(f"SQL không mong đợi: {sql}")

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        self.db.sql.append(sql)
        if "FROM inventory_snapshot" in sql:
            return self.db.snapshot
        if "FROM pending_txn WHERE txn_id" in sql:
            txn_id = args[0]
            if txn_id in self.db.hidden_from_dup_check:
                self.db.hidden_from_dup_check.discard(txn_id)
                return None
            return self.db.pending.get(txn_id)
        raise AssertionError(f"SQL không mong đợi: {sql}")

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.db.sql.append(sql)
        assert "FROM pending_txn" in sql
        dc_id, product_id, status = args
        return [
            r
            for r in self.db.rows()
            if (r["dc_id"], r["product_id"], r["status"]) == (dc_id, product_id, status)
        ]


class FakePool:
    def __init__(self, db: FakeDb) -> None:
        self.db = db

    @contextlib.asynccontextmanager
    async def acquire(self) -> AsyncIterator[FakeConn]:
        yield FakeConn(self.db)


def client_for(db: FakeDb) -> TestClient:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(failover.router)
    app.state.pool = FakePool(db)
    return TestClient(app)


def body(request_id: str | UUID, qty: int, product_id: int = SKU_A_PRODUCT_ID) -> dict[str, Any]:
    return {
        "request_id": str(request_id),
        "product_id": product_id,
        "qty": qty,
        "created_by": "nv-trung",
    }


def reply(txn_id: str | UUID, status: str, qty: int, duplicate: bool) -> dict[str, Any]:
    return {"txn_id": str(txn_id), "status": status, "provisional_qty": qty, "duplicate": duplicate}


# ---------------------------------------------------------------------------
# Bước 2 (TC15): kho còn sống (ONLINE) hoặc đang đồng bộ (RECOVERING) -> 409, không INSERT
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("status", [DcStatus.ONLINE, DcStatus.RECOVERING])
@pytest.mark.parametrize("kind", ["inbound", "outbound"])
def test_dc_alive_is_rejected(status: DcStatus, kind: str) -> None:
    db = FakeDb(status)
    r = client_for(db).post(f"/failover/dc/{DC_TRUNG}/{kind}", json=body(uuid4(), 5))
    assert r.status_code == 409
    err = r.json()
    assert err["error_code"] == "DC_NOT_OFFLINE"
    assert set(err) == {"error_code", "message", "details"}
    assert db.pending == {}
    assert not any("INSERT" in s for s in db.sql)


# ---------------------------------------------------------------------------
# Kịch bản 3 bước 3–5: ghi hộ khi kho OFFLINE
# ---------------------------------------------------------------------------
def test_kb3_inbound_outbound_and_rejected_big_outbound() -> None:
    db = FakeDb(DcStatus.OFFLINE, on_hand=35)
    c = client_for(db)

    r = c.post(f"/failover/dc/{DC_TRUNG}/inbound", json=body(REQ_KB3_IN, 30))
    assert r.status_code == 200, r.text
    assert r.json() == reply(REQ_KB3_IN, "PENDING", 65, False)

    r = c.post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(REQ_KB3_OUT, 10))
    assert r.status_code == 200, r.text
    assert r.json() == reply(REQ_KB3_OUT, "PENDING", 55, False)

    r = c.post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(REQ_KB3_OUT_BIG, 100))
    assert r.status_code == 422
    assert r.json()["error_code"] == "INSUFFICIENT_STOCK"
    assert r.json()["details"]["available"] == 55

    assert sorted((p["txn_type"], p["qty"]) for p in db.rows()) == [
        ("INBOUND", 30),
        ("OUTBOUND", 10),
    ]


def test_outbound_respects_reserved() -> None:
    db = FakeDb(DcStatus.OFFLINE, on_hand=50, reserved=30)
    r = client_for(db).post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(uuid4(), 25))
    assert r.status_code == 422
    assert db.pending == {}


def test_missing_snapshot_counts_as_zero() -> None:
    db = FakeDb(DcStatus.OFFLINE, on_hand=None)
    c = client_for(db)
    assert c.post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(uuid4(), 1)).status_code == 422
    r = c.post(f"/failover/dc/{DC_TRUNG}/inbound", json=body(uuid4(), 5))
    assert r.json()["provisional_qty"] == 5


def test_unknown_sku_and_bad_qty_are_validation_errors() -> None:
    c = client_for(FakeDb(DcStatus.OFFLINE))
    r = c.post(f"/failover/dc/{DC_TRUNG}/inbound", json=body(uuid4(), 5, product_id=999))
    assert (r.status_code, r.json()["error_code"]) == (400, "VALIDATION_ERROR")
    r = c.post(f"/failover/dc/{DC_TRUNG}/inbound", json=body(uuid4(), 0))
    assert (r.status_code, r.json()["error_code"]) == (400, "VALIDATION_ERROR")


def test_unknown_dc_is_validation_error() -> None:
    r = client_for(FakeDb(DcStatus.OFFLINE)).post("/failover/dc/9/inbound", json=body(uuid4(), 5))
    assert (r.status_code, r.json()["error_code"]) == (400, "VALIDATION_ERROR")


# ---------------------------------------------------------------------------
# Bước 1: gửi trùng request_id -> 200, duplicate: true, status hiện tại, provisional_qty tính lại;
# không kiểm tra dc_status, không kiểm tra lại hạn mức
# ---------------------------------------------------------------------------
def test_resend_same_request_id() -> None:
    """KB3 bước 4: WMS gửi lại cùng request_id không bị ghi hai lần, cũng không bị 422 oan."""
    db = FakeDb(DcStatus.OFFLINE, on_hand=40)
    c = client_for(db)
    req = uuid4()
    first = c.post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(req, 40))
    again = c.post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(req, 40))
    assert first.json() == reply(req, "PENDING", 0, False)
    assert again.status_code == 200
    assert again.json() == reply(req, "PENDING", 0, True)
    assert len(db.pending) == 1


@pytest.mark.parametrize("status", [DcStatus.ONLINE, DcStatus.RECOVERING])
def test_duplicate_skips_dc_status_check(status: DcStatus) -> None:
    """Lệnh đã ghi hộ lúc kho OFFLINE, WMS gửi lại khi kho đã lên: vẫn 200 duplicate, không 409."""
    db = FakeDb(status, on_hand=35)
    db.add(UUID(REQ_KB3_IN), TxnType.INBOUND, 30, PendingTxnStatus.PENDING)
    r = client_for(db).post(f"/failover/dc/{DC_TRUNG}/inbound", json=body(REQ_KB3_IN, 30))
    assert r.status_code == 200, r.text
    assert r.json() == reply(REQ_KB3_IN, "PENDING", 65, True)
    assert not any("FROM dc_status" in s for s in db.sql)


def test_duplicate_skips_limit_check() -> None:
    """Gửi lại lệnh xuất cũ không bị kiểm tra hạn mức lại (lệnh mới cùng số lượng thì 422)."""
    db = FakeDb(DcStatus.OFFLINE, on_hand=12)
    db.add(UUID(REQ_KB3_OUT), TxnType.OUTBOUND, 10, PendingTxnStatus.PENDING)
    c = client_for(db)
    assert c.post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(uuid4(), 10)).status_code == 422
    r = c.post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(REQ_KB3_OUT, 10))
    assert r.status_code == 200
    assert r.json() == reply(REQ_KB3_OUT, "PENDING", 2, True)


@pytest.mark.parametrize("status", [PendingTxnStatus.APPLIED, PendingTxnStatus.REJECTED])
def test_duplicate_reports_current_status(status: PendingTxnStatus) -> None:
    db = FakeDb(DcStatus.ONLINE, on_hand=65)  # snapshot đã gồm lệnh nhập
    db.add(UUID(REQ_KB3_IN), TxnType.INBOUND, 30, status)
    r = client_for(db).post(f"/failover/dc/{DC_TRUNG}/inbound", json=body(REQ_KB3_IN, 30))
    assert r.json() == reply(REQ_KB3_IN, status.value, 65, True)


def test_conflict_at_insert_is_reported_as_duplicate() -> None:
    """Chốt chặn cuối: lệnh trùng chen vào giữa bước 1 và bước 5 -> ON CONFLICT, trả duplicate."""
    db = FakeDb(DcStatus.OFFLINE, on_hand=35)
    req = UUID(REQ_KB3_IN)
    db.add(req, TxnType.INBOUND, 30, PendingTxnStatus.PENDING)
    db.hidden_from_dup_check.add(req)
    r = client_for(db).post(f"/failover/dc/{DC_TRUNG}/inbound", json=body(req, 30))
    assert r.status_code == 200
    assert r.json() == reply(req, "PENDING", 65, True)
    assert len(db.pending) == 1


def test_provisional_on_hand_formula() -> None:
    """db.md 2.4: qty_on_hand + Σ nhập PENDING − Σ xuất PENDING."""
    pending = [
        {"txn_type": TxnType.INBOUND.value, "qty": 30},
        {"txn_type": TxnType.OUTBOUND.value, "qty": 10},
    ]
    assert provisional_on_hand(35, pending) == 55
    assert provisional_on_hand(35, []) == 35
