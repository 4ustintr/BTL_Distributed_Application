"""Client HTTP nội bộ HQ <-> kho: timeout, thử lại, gắn X-Internal-Key (spec F5, F11).

Mặc định gọi tối đa 5 lần (1 lần đầu + HTTP_MAX_RETRIES = 4 lần thử lại), mỗi lần cách 2 giây.
Chỉ thử lại khi lỗi mạng hoặc quá thời gian (httpx.TransportError). Phản hồi HTTP bất kỳ
(kể cả 4xx, 5xx) được trả nguyên cho bên gọi xử lý theo F10. Hết số lần thử thì ném lại
lỗi httpx cuối cùng; bên gọi quyết định trả DC_UNREACHABLE hay UPSTREAM_TIMEOUT.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import re
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

import httpx
from fastapi import Request

from common.constants import (
    HTTP_MAX_RETRIES,
    HTTP_RETRY_INTERVAL_S,
    HTTP_TIMEOUT_S,
    INTERNAL_KEY_HEADER,
    ErrorCode,
)
from common.errors import AppError

log = logging.getLogger(__name__)


class InternalClient:
    """Client httpx async gọi một service khác (một base_url)."""

    def __init__(
        self,
        base_url: str,
        internal_key: str,
        *,
        timeout: float = HTTP_TIMEOUT_S,
        max_retries: int = HTTP_MAX_RETRIES,
        retry_interval: float = HTTP_RETRY_INTERVAL_S,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers={INTERNAL_KEY_HEADER: internal_key},
            transport=transport,
        )

    @property
    def base_url(self) -> str:
        return str(self._client.base_url)

    async def request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, Any] | None = None,
        request_timeout: float | None = None,
        max_retries: int | None = None,
    ) -> httpx.Response:
        """Gửi request; lỗi mạng thì thử lại tối đa max_retries lần, mỗi lần cách retry_interval.

        request_timeout, max_retries ghi đè cho từng lệnh (F5: "trừ khi mục riêng ghi khác",
        ví dụ Health Monitor dùng timeout 1 giây, không thử lại).
        """
        retries = self.max_retries if max_retries is None else max_retries
        kwargs: dict[str, Any] = {"json": json, "params": params}
        if request_timeout is not None:
            kwargs["timeout"] = request_timeout
        attempt = 0
        while True:
            try:
                return await self._client.request(method, path, **kwargs)
            except httpx.TransportError as exc:
                if attempt >= retries:
                    raise
                attempt += 1
                log.warning(
                    "%s %s%s lỗi (%s), thử lại lần %d/%d",
                    method,
                    self.base_url,
                    path,
                    type(exc).__name__,
                    attempt,
                    retries,
                )
                await asyncio.sleep(self.retry_interval)

    async def get(self, path: str, **kwargs: Any) -> httpx.Response:
        return await self.request("GET", path, **kwargs)

    async def post(self, path: str, **kwargs: Any) -> httpx.Response:
        return await self.request("POST", path, **kwargs)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> InternalClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()


def check_internal_key(provided: str | None, expected: str) -> None:
    """Ném AppError UNAUTHORIZED (401) nếu X-Internal-Key sai hoặc thiếu (F5)."""
    if not provided or not hmac.compare_digest(provided.encode(), expected.encode()):
        raise AppError(ErrorCode.UNAUTHORIZED, f"Sai {INTERNAL_KEY_HEADER}")


_PATH_PARAM = re.compile(r"\{[^}]+\}")


def normalize_route_path(path: str) -> str:
    """Bỏ tên tham số đường dẫn: "/transfers/{transfer_id}/cancel" -> "/transfers/{}/cancel"."""
    return _PATH_PARAM.sub("{}", path)


def internal_key_guard(
    internal_routes: Iterable[tuple[str, str]],
) -> Callable[[Request], Awaitable[None]]:
    """Dependency cấp app: kiểm tra X-Internal-Key cho các route nội bộ (F5).

    internal_routes là các cặp (method, path theo spec), ví dụ ("POST", "/replay").
    Route do trình duyệt gọi (WMS, Dashboard) không nằm trong danh sách nên không cần khóa.
    Khóa mong đợi đọc từ request.app.state.internal_key.
    """
    routes = {(m.upper(), normalize_route_path(p)) for m, p in internal_routes}

    async def guard(request: Request) -> None:
        route = request.scope.get("route")
        path = getattr(route, "path", None)
        if path is None or (request.method, normalize_route_path(path)) not in routes:
            return
        check_internal_key(request.headers.get(INTERNAL_KEY_HEADER), request.app.state.internal_key)

    return guard
