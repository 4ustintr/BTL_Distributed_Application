"""common/errors.py: mọi lỗi trả {error_code, message, details} theo F5."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from common.constants import ErrorCode
from common.errors import AppError, install_error_handlers
from common.models import OutboundRequest


def _client() -> TestClient:
    app = FastAPI()
    install_error_handlers(app)

    @app.get("/boom")
    async def boom() -> None:
        raise AppError(ErrorCode.INSUFFICIENT_STOCK, "Không đủ hàng", {"available": 20})

    @app.post("/outbound")
    async def outbound(body: OutboundRequest) -> dict[str, int]:
        return {"qty": body.qty}

    return TestClient(app)


def test_app_error_status_and_body() -> None:
    r = _client().get("/boom")
    assert r.status_code == 422
    assert r.json() == {
        "error_code": "INSUFFICIENT_STOCK",
        "message": "Không đủ hàng",
        "details": {"available": 20},
    }


def test_app_error_status_property() -> None:
    assert AppError(ErrorCode.DC_UNREACHABLE, "x").status_code == 503
    assert AppError(ErrorCode.UPSTREAM_TIMEOUT, "x").status_code == 504
    assert AppError(ErrorCode.NOT_FOUND, "x").to_body()["details"] == {}


def test_validation_error_is_400() -> None:
    r = _client().post("/outbound", json={"product_id": 1, "qty": 0})
    assert r.status_code == 400
    body = r.json()
    assert body["error_code"] == "VALIDATION_ERROR"
    assert set(body) == {"error_code", "message", "details"}
    fields = {tuple(e["loc"])[-1] for e in body["details"]["errors"]}
    assert {"request_id", "qty", "ref_type", "created_by"} <= fields


def test_unknown_route_is_not_found() -> None:
    r = _client().get("/khong-co")
    assert r.status_code == 404
    assert r.json()["error_code"] == "NOT_FOUND"
