"""Lỗi ứng dụng và handler trả {error_code, message, details} theo spec F5."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from common.constants import ERROR_HTTP_STATUS, ErrorCode


class AppError(Exception):
    """Lỗi nghiệp vụ; HTTP status lấy từ bảng F5 theo error_code."""

    def __init__(
        self,
        error_code: ErrorCode,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = ErrorCode(error_code)
        self.message = message
        self.details: dict[str, Any] = details or {}

    @property
    def status_code(self) -> int:
        return ERROR_HTTP_STATUS[self.error_code]

    def to_body(self) -> dict[str, Any]:
        return error_body(self.error_code, self.message, self.details)


def error_body(
    error_code: ErrorCode, message: str, details: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Thân lỗi đúng dạng F5."""
    return {"error_code": str(error_code), "message": message, "details": details or {}}


def error_response(
    error_code: ErrorCode, message: str, details: dict[str, Any] | None = None
) -> JSONResponse:
    return JSONResponse(
        status_code=ERROR_HTTP_STATUS[error_code],
        content=error_body(error_code, message, details),
    )


# HTTPException của framework (404 route không tồn tại, 401…) có mã tương ứng trong F5
_HTTP_STATUS_TO_CODE: dict[int, ErrorCode] = {
    400: ErrorCode.VALIDATION_ERROR,
    401: ErrorCode.UNAUTHORIZED,
    404: ErrorCode.NOT_FOUND,
}


async def app_error_handler(_: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, AppError)
    return JSONResponse(status_code=exc.status_code, content=exc.to_body())


async def validation_error_handler(_: Request, exc: Exception) -> JSONResponse:
    """Thiếu trường, sai kiểu, qty <= 0 … -> 400 VALIDATION_ERROR (F5)."""
    assert isinstance(exc, RequestValidationError)
    errors = [
        {"loc": list(e.get("loc", ())), "msg": str(e.get("msg", "")), "type": e.get("type")}
        for e in exc.errors()
    ]
    return error_response(ErrorCode.VALIDATION_ERROR, "Dữ liệu không hợp lệ", {"errors": errors})


async def http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, StarletteHTTPException)
    code = _HTTP_STATUS_TO_CODE.get(exc.status_code)
    if code is None:
        # F5 không có error_code cho status này (vd. 405): giữ phản hồi mặc định
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=getattr(exc, "headers", None),
        )
    return error_response(code, str(exc.detail))


def install_error_handlers(app: FastAPI) -> None:
    """Đăng ký các handler lỗi cho app FastAPI."""
    app.add_exception_handler(AppError, app_error_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
