"""Conftest gốc: thêm gốc repo vào sys.path, nạp scripts/demo_ids.env (docs/tests.md mục 2)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = TESTS_DIR.parent

for _p in (str(REPO_ROOT), str(TESTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from fixtures import DEMO_IDS, host_ips, integration_skip_reason, print_status_line  # noqa: E402

# Cho script và test con đọc bằng os.environ; không ghi đè giá trị đã đặt sẵn
for _key, _value in DEMO_IDS.items():
    os.environ.setdefault(_key, _value)


@pytest.fixture(scope="session")
def demo_ids() -> dict[str, str]:
    """Các REQ_* cố định trong scripts/demo_ids.env."""
    return dict(DEMO_IDS)


@pytest.fixture
def status_line():  # type: ignore[no-untyped-def]
    """Hàm in dòng trạng thái theo docs/tests.md mục 3."""
    return print_status_line


@pytest.fixture(autouse=True)
def _integration_same_host(request: pytest.FixtureRequest) -> None:
    """Test @pytest.mark.integration chỉ chạy khi HOST_A_IP = HOST_B_IP (docs/tests.md mục 4)."""
    if request.node.get_closest_marker("integration") is None:
        return
    reason = integration_skip_reason(*host_ips())
    if reason is not None:
        pytest.skip(reason)
