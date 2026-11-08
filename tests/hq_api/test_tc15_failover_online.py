"""TC15 – Failover khi kho còn sống, và quy trình ghi hộ F10.h (docs/tests.md mục 4).

Assert TC15: 409 DC_NOT_OFFLINE; không có dòng pending_txn mới.
Phần unit dùng CSDL giả trong bộ nhớ; test_tc15_integration chạy với hệ thống thật.
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
# CSDL giả: chỉ đủ cho các câu SQL của routers/failover.py
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

    def rows(self) -> list[dict[str, Any]]:
        return list(self.pending.values())


class FakeConn:
    def __init__(self, db: FakeDb) -> None:
        self.db = db
        self._staged: dict[UUID, dict[str, Any]] | None = None

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
            self.db.pending[txn_id] = {
                "txn_id": txn_id,
                "dc_id": dc_id,
                "txn_type": str(txn_type),
                "product_id": product_id,
                "qty": qty,
                "created_by": created_by,
                "status": PendingTxnStatus.PENDING.value,
            }
            return PendingTxnStatus.PENDING.value
        if "SELECT status FROM pending_txn" in sql:
            return self.db.pending[args[0]]["status"]
        raise AssertionError(f"SQL không mong đợi: {sql}")

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        self.db.sql.append(sql)
        assert "FROM inventory_snapshot" in sql
        return self.db.snapshot

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


# ---------------------------------------------------------------------------
# TC15: kho còn sống (ONLINE) hoặc đang đồng bộ (RECOVERING) -> 409, không INSERT
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("status", [DcStatus.ONLINE, DcStatus.RECOVERING])
@pytest.mark.parametrize("kind", ["inbound", "outbound"])
def test_tc15_failover_when_dc_alive(status: DcStatus, kind: str) -> None:
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
    assert r.json() == {"txn_id": REQ_KB3_IN, "status": "PENDING", "provisional_qty": 65}

    r = c.post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(REQ_KB3_OUT, 10))
    assert r.status_code == 200, r.text
    assert r.json() == {"txn_id": REQ_KB3_OUT, "status": "PENDING", "provisional_qty": 55}

    r = c.post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(REQ_KB3_OUT_BIG, 100))
    assert r.status_code == 422
    assert r.json()["error_code"] == "INSUFFICIENT_STOCK"
    assert r.json()["details"]["available"] == 55

    assert sorted((p["txn_type"], p["qty"]) for p in db.rows()) == [
        ("INBOUND", 30),
        ("OUTBOUND", 10),
    ]


def test_resend_same_request_id_is_not_double_counted() -> None:
    """KB3 bước 4: WMS gửi lại cùng request_id không bị ghi hai lần, cũng không bị 422 oan."""
    db = FakeDb(DcStatus.OFFLINE, on_hand=40)
    c = client_for(db)
    req = uuid4()
    first = c.post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(req, 40))
    again = c.post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(req, 40))
    assert first.status_code == again.status_code == 200
    assert (
        first.json()
        == again.json()
        == {
            "txn_id": str(req),
            "status": "PENDING",
            "provisional_qty": 0,
        }
    )
    assert len(db.pending) == 1


def test_resend_after_applied_reports_current_status() -> None:
    db = FakeDb(DcStatus.OFFLINE, on_hand=65)  # snapshot đã gồm lệnh nhập
    c = client_for(db)
    req = uuid4()
    c.post(f"/failover/dc/{DC_TRUNG}/inbound", json=body(req, 30))
    db.pending[req]["status"] = PendingTxnStatus.APPLIED.value
    r = c.post(f"/failover/dc/{DC_TRUNG}/inbound", json=body(req, 30))
    assert r.json() == {"txn_id": str(req), "status": "APPLIED", "provisional_qty": 65}


def test_outbound_respects_reserved() -> None:
    db = FakeDb(DcStatus.OFFLINE, on_hand=50, reserved=30)
    r = client_for(db).post(f"/failover/dc/{DC_TRUNG}/outbound", json=body(uuid4(), 25))
    assert r.status_code == 422
    assert db.pending == {}


def test_unknown_sku_and_bad_qty_are_validation_errors() -> None:
    c = client_for(FakeDb(DcStatus.OFFLINE))
    r = c.post(f"/failover/dc/{DC_TRUNG}/inbound", json=body(uuid4(), 5, product_id=999))
    assert (r.status_code, r.json()["error_code"]) == (400, "VALIDATION_ERROR")
    r = c.post(f"/failover/dc/{DC_TRUNG}/inbound", json=body(uuid4(), 0))
    assert (r.status_code, r.json()["error_code"]) == (400, "VALIDATION_ERROR")


def test_unknown_dc_is_validation_error() -> None:
    r = client_for(FakeDb(DcStatus.OFFLINE)).post("/failover/dc/9/inbound", json=body(uuid4(), 5))
    assert (r.status_code, r.json()["error_code"]) == (400, "VALIDATION_ERROR")


def test_provisional_on_hand_formula() -> None:
    """db.md 2.4: qty_on_hand + Σ nhập PENDING − Σ xuất PENDING."""
    a, b = uuid4(), uuid4()
    pending = [
        {"txn_id": a, "txn_type": TxnType.INBOUND.value, "qty": 30},
        {"txn_id": b, "txn_type": TxnType.OUTBOUND.value, "qty": 10},
    ]
    assert provisional_on_hand(35, pending) == 55
    assert provisional_on_hand(35, pending, exclude_txn_id=b) == 65


# ---------------------------------------------------------------------------
# TC15 trên hệ thống thật (máy A, sau make reset; mọi kho ONLINE)
# ---------------------------------------------------------------------------
@pytest.mark.integration
async def test_tc15_integration() -> None:
    import httpx
    from failover_helpers import count_pending, make_reset, target, wait_dc_status

    make_reset()
    t = target()
    await wait_dc_status(t, DC_TRUNG, DcStatus.ONLINE)
    before = await count_pending(t)
    async with httpx.AsyncClient(timeout=10) as http:
        for kind in ("inbound", "outbound"):
            r = await http.post(f"{t.hq}/failover/dc/{DC_TRUNG}/{kind}", json=body(uuid4(), 5))
            assert r.status_code == 409, r.text
            assert r.json()["error_code"] == "DC_NOT_OFFLINE"
    assert await count_pending(t) == before
