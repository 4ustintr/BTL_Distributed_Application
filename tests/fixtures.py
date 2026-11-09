"""Dữ liệu cố định dùng chung cho test và demo (docs/tests.md mục 2, 3).

- Nạp các REQ_* từ scripts/demo_ids.env (không khai báo lại giá trị ở đây).
- format_status_line / print_status_line: dòng trạng thái theo docs/tests.md mục 3.
- host_ips / integration_skip_reason: test tích hợp chỉ chạy khi HOST_A_IP = HOST_B_IP
  (docs/tests.md mục 4).
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from common.constants import DC_REGION, ENV_HOST_A_IP, ENV_HOST_B_IP, InventorySource

REPO_ROOT = Path(__file__).resolve().parent.parent
DEMO_IDS_FILE = REPO_ROOT / "scripts" / "demo_ids.env"

SKU_A_PRODUCT_ID = 1  # tests.md mục 2 – SKU A: product_id = 1, sku = SKU-A
SKU_A = "SKU-A"  # tests.md mục 2, F12


def load_demo_ids(path: Path = DEMO_IDS_FILE) -> dict[str, str]:
    """Đọc file KEY=VALUE, bỏ dòng trống và dòng chú thích '#'."""
    ids: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            raise ValueError(f"Dòng không đúng dạng KEY=VALUE trong {path}: {raw!r}")
        ids[key.strip()] = value.strip()
    return ids


DEMO_IDS: dict[str, str] = load_demo_ids()

# docs/tests.md mục 4: test tích hợp chạy khi HQ và kho cùng một máy
INTEGRATION_SAME_HOST_MSG = "test tích hợp cần HQ và kho cùng một máy"


def read_env_file(path: Path) -> dict[str, str]:
    """Đọc .env kiểu KEY=VALUE (bỏ dòng trống, chú thích, dòng không có '='); thiếu file trả {}."""
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if sep:
            values[key.strip()] = value.strip().strip("\"'")
    return values


def host_ips(
    environ: Mapping[str, str] | None = None, env_file: Path | None = None
) -> tuple[str | None, str | None]:
    """(HOST_A_IP, HOST_B_IP): ưu tiên biến môi trường, rồi file ENV_FILE (mặc định .env)."""
    env = os.environ if environ is None else environ
    if env_file is None:
        env_file = REPO_ROOT / env.get("ENV_FILE", ".env")
    file_values = read_env_file(env_file)
    a, b = (env.get(k) or file_values.get(k) for k in (ENV_HOST_A_IP, ENV_HOST_B_IP))
    return a, b


def integration_skip_reason(host_a: str | None, host_b: str | None) -> str | None:
    """Lý do skip test tích hợp, hoặc None nếu được chạy (HOST_A_IP = HOST_B_IP)."""
    return None if host_a == host_b else INTEGRATION_SAME_HOST_MSG


# Nguồn số liệu được ghi chú sau ô kho, ví dụ "TRUNG=35/35(offline)" (tests.md mục 3)
_ANNOTATED_SOURCES = {InventorySource.OFFLINE, InventorySource.PROVISIONAL}
_NA = "NA"


def format_status_line(
    step: str,
    items: Iterable[Mapping[str, Any]],
    invariant: Mapping[str, Any] | None,
) -> str:
    """Dòng trạng thái theo docs/tests.md mục 3.

    step: mã bước, ví dụ "KB1-2".
    items: các dòng "items" của GET /dashboard/inventory (dc_id, qty_on_hand, qty_reserved,
        qty_in_transit, source).
    invariant: thân 200 của GET /invariant, hoặc None khi /invariant trả 503
        (khi đó SHIPPED_OUT, RECEIVED_IN, DIFF in NA; TRANSIT lấy tổng qty_in_transit).
    """
    by_dc = {int(it["dc_id"]): it for it in items}
    parts = [f"[{step}]"]
    for dc_id, region in DC_REGION.items():
        it = by_dc.get(dc_id)
        if it is None:
            parts.append(f"{region}={_NA}")
            continue
        on_hand = int(it["qty_on_hand"])
        available = on_hand - int(it["qty_reserved"])
        cell = f"{region}={on_hand}/{available}"
        source = it.get("source")
        if source in _ANNOTATED_SOURCES:
            cell += f"({source})"
        parts.append(cell)

    if invariant is not None:
        transit: Any = invariant["in_transit"]
        shipped_out: Any = invariant["shipped_out"]
        received_in: Any = invariant["received_in"]
        diff: Any = invariant["diff"]
    else:
        transit = sum(int(it.get("qty_in_transit") or 0) for it in by_dc.values())
        shipped_out = received_in = diff = _NA
    parts += [
        f"TRANSIT={transit}",
        f"SHIPPED_OUT={shipped_out}",
        f"RECEIVED_IN={received_in}",
        f"DIFF={diff}",
    ]
    return " ".join(parts)


def print_status_line(
    step: str,
    items: Iterable[Mapping[str, Any]],
    invariant: Mapping[str, Any] | None,
) -> str:
    """In dòng trạng thái ra stdout và trả lại chuỗi để test so khớp."""
    line = format_status_line(step, items, invariant)
    print(line)
    return line
