"""common/http.py: timeout, thử lại, gắn X-Internal-Key (F5), guard 401."""

import httpx
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from common.constants import HTTP_MAX_RETRIES, HTTP_TIMEOUT_S, INTERNAL_KEY_HEADER
from common.errors import install_error_handlers
from common.http import InternalClient, internal_key_guard, normalize_route_path

KEY = "khoa-test"


def _client(handler, **kw) -> InternalClient:  # type: ignore[no-untyped-def]
    return InternalClient(
        "http://node", KEY, retry_interval=0, transport=httpx.MockTransport(handler), **kw
    )


async def test_attaches_internal_key_and_default_timeout() -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(200, json={"ok": True})

    async with _client(handler) as c:
        r = await c.post("/replay", json={"a": 1})
    assert r.status_code == 200
    assert seen[0].headers[INTERNAL_KEY_HEADER] == KEY
    assert seen[0].extensions["timeout"]["read"] == HTTP_TIMEOUT_S


async def test_retries_network_errors_then_succeeds() -> None:
    calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise httpx.ConnectError("down", request=req)
        return httpx.Response(200)

    async with _client(handler) as c:
        r = await c.get("/health")
    assert r.status_code == 200
    assert calls == 3


async def test_gives_up_after_max_retries() -> None:
    calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("slow", request=req)

    async with _client(handler) as c:
        with pytest.raises(httpx.TimeoutException):
            await c.post("/transfers/notice")
    # F5: tối đa 5 lần gọi (1 lần đầu + 4 lần thử lại)
    assert calls == 5
    assert calls == 1 + HTTP_MAX_RETRIES


async def test_per_call_override_no_retry_and_timeout() -> None:
    calls = 0
    seen_timeout: list[float] = []

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        seen_timeout.append(req.extensions["timeout"]["read"])
        raise httpx.ConnectError("down", request=req)

    async with _client(handler) as c:
        with pytest.raises(httpx.ConnectError):
            await c.get("/health", request_timeout=1.0, max_retries=0)
    assert calls == 1
    assert seen_timeout == [1.0]


async def test_http_error_responses_are_not_retried() -> None:
    calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"error_code": "DC_RECOVERING"})

    async with _client(handler) as c:
        r = await c.post("/replay")
    assert r.status_code == 503
    assert calls == 1


def test_normalize_route_path() -> None:
    assert normalize_route_path("/transfers/{transfer_id}/cancel") == "/transfers/{}/cancel"
    assert normalize_route_path("/replay") == "/replay"


def _guarded_app() -> TestClient:
    app = FastAPI(dependencies=[Depends(internal_key_guard([("POST", "/transfers/{id}/cancel")]))])
    app.state.internal_key = KEY
    install_error_handlers(app)

    @app.post("/transfers/{transfer_id}/cancel")
    async def cancel(transfer_id: str) -> dict[str, str]:
        return {"id": transfer_id}

    @app.post("/transfers/{transfer_id}/ship")
    async def ship(transfer_id: str) -> dict[str, str]:
        return {"id": transfer_id}

    return TestClient(app)


def test_guard_rejects_wrong_or_missing_key() -> None:
    c = _guarded_app()
    r = c.post("/transfers/x/cancel")
    assert r.status_code == 401
    assert r.json()["error_code"] == "UNAUTHORIZED"
    assert c.post("/transfers/x/cancel", headers={INTERNAL_KEY_HEADER: "sai"}).status_code == 401
    assert c.post("/transfers/x/cancel", headers={INTERNAL_KEY_HEADER: KEY}).status_code == 200


def test_guard_ignores_browser_routes() -> None:
    assert _guarded_app().post("/transfers/x/ship").status_code == 200
