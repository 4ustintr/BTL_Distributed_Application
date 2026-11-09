"""TC17 (tùy chọn) – Kho khởi động khi HQ chết (docs/tests.md mục 4; F9 chế độ Node API).

Cách chạy: docker stop hq-api, khởi động lại node-api-dc1.
Assert: /health trả mode RECOVERING; gọi tay POST /recovery/complete thì chuyển ONLINE.
Spec: F6 GET /health, POST /recovery/complete; F9 (khởi động -> RECOVERING; quản trị viên gọi tay
khi HQ không chạy). Chạy khi HQ và kho cùng một máy (tests/conftest.py tự skip khi khác).
"""

from __future__ import annotations

import asyncio
import os
import subprocess

import httpx
import pytest
from fixtures import REPO_ROOT, host_ips, read_env_file

from common.constants import ENV_INTERNAL_KEY, INTERNAL_KEY_HEADER, NodeMode
from common.models import NodeHealthResponse, RecoveryCompleteResponse

pytestmark = [pytest.mark.integration, pytest.mark.optional]

DC_BAC = 1
NODE_PORT = 8001  # F2: node-api-dc1
HQ_API_CONTAINER = "hq-api"  # F2
NODE_CONTAINER = "node-api-dc1"  # F2
WAIT_S = 60.0


def docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True)


def make_reset() -> None:
    subprocess.run(["make", "reset"], cwd=REPO_ROOT, check=True)


async def wait_health(http: httpx.AsyncClient, url: str) -> NodeHealthResponse:
    """Chờ Node API vừa khởi động trả 200 cho GET /health."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WAIT_S
    while True:
        try:
            r = await http.get(f"{url}/health")
            if r.status_code == 200:
                return NodeHealthResponse.model_validate(r.json())
        except httpx.TransportError:
            pass
        if loop.time() >= deadline:
            raise AssertionError(f"hết {WAIT_S:.0f} giây chờ {url}/health trả 200")
        await asyncio.sleep(0.5)


async def test_tc17_start_without_hq() -> None:
    make_reset()
    _, host_b = host_ips()
    env_file = REPO_ROOT / os.environ.get("ENV_FILE", ".env")
    key = os.environ.get(ENV_INTERNAL_KEY) or read_env_file(env_file)[ENV_INTERNAL_KEY]
    node = f"http://{host_b}:{NODE_PORT}"

    docker("stop", HQ_API_CONTAINER)
    try:
        docker("restart", NODE_CONTAINER)
        async with httpx.AsyncClient(timeout=5) as http:
            health = await wait_health(http, node)
            assert (health.dc_id, health.mode) == (DC_BAC, NodeMode.RECOVERING)

            # HQ không chạy: không có Replay Worker gọi /recovery/complete, kho vẫn RECOVERING
            await asyncio.sleep(5)
            assert (await wait_health(http, node)).mode == NodeMode.RECOVERING

            # Quản trị viên gọi tay (F9), có X-Internal-Key (F5)
            r = await http.post(f"{node}/recovery/complete", headers={INTERNAL_KEY_HEADER: key})
            assert r.status_code == 200, r.text
            assert RecoveryCompleteResponse.model_validate(r.json()).mode == NodeMode.ONLINE
            assert (await wait_health(http, node)).mode == NodeMode.ONLINE
    finally:
        docker("start", HQ_API_CONTAINER)
