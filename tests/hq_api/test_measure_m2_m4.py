"""Unit test hàm thuần của scripts/measure/m2_m4.py (số đo M2, M4, docs/tests.md mục 5)."""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fixtures import REPO_ROOT

SCRIPT = REPO_ROOT / "scripts" / "measure" / "m2_m4.py"
_spec = importlib.util.spec_from_file_location("m2_m4", SCRIPT)
assert _spec is not None and _spec.loader is not None
m2_m4 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(m2_m4)


def test_m4_request_ids_are_deterministic_and_unique() -> None:
    ids = {n: m2_m4.m4_request_ids(n) for n in m2_m4.M4_NS}
    assert [len(ids[n]) for n in m2_m4.M4_NS] == [2, 20, 200]
    assert ids[20] == m2_m4.m4_request_ids(20)  # chạy lại ra đúng các mã cũ
    every = [i for n in m2_m4.M4_NS for i in ids[n]]
    assert len(set(every)) == 222  # không trùng giữa các lần đo


def test_defaults_follow_tests_md() -> None:
    assert m2_m4.M2_RUNS == 5
    assert m2_m4.M4_NS == (2, 20, 200)


@pytest.mark.parametrize(
    ("a", "b", "ok"),
    [
        ("192.168.1.10", "192.168.1.10", True),
        ("192.168.1.10", "192.168.1.20", False),
        (None, None, False),  # thiếu .env: không coi là cùng máy
        ("192.168.1.10", None, False),
        ("", "", False),
    ],
)
def test_same_host_problem(a: str | None, b: str | None, ok: bool) -> None:
    assert (m2_m4.same_host_problem(a, b) is None) is ok


def test_summarize() -> None:
    assert m2_m4.summarize([4.214, 5.6, 6.01, 4.0, 5.2]) == {
        "runs_s": [4.21, 5.6, 6.01, 4.0, 5.2],
        "mean_s": 5.0,
        "max_s": 6.01,
    }
    assert m2_m4.summarize([]) == {"runs_s": [], "mean_s": None, "max_s": None}


def test_seconds_between() -> None:
    t0 = datetime(2026, 11, 9, 10, 0, tzinfo=UTC)
    assert m2_m4.seconds_between(t0, t0 + timedelta(seconds=5, milliseconds=250)) == 5.25


def test_status_counts_fills_missing_statuses() -> None:
    rows = [{"status": "APPLIED", "count": 199}, {"status": "REJECTED", "count": 1}]
    assert m2_m4.status_counts(rows) == {"PENDING": 0, "APPLIED": 199, "REJECTED": 1}


def test_read_env_file(tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text("# chú thích\nHOST_A_IP=10.0.0.5\n\nPOSTGRES_PASSWORD='abc123'\nrac\n", "utf-8")
    assert m2_m4.read_env_file(env) == {"HOST_A_IP": "10.0.0.5", "POSTGRES_PASSWORD": "abc123"}
    assert m2_m4.read_env_file(tmp_path / "khong-co") == {}


def test_refuses_without_same_host(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ENV_FILE", str(tmp_path / "khong-co.env"))
    monkeypatch.delenv("HOST_A_IP", raising=False)
    monkeypatch.delenv("HOST_B_IP", raising=False)
    with pytest.raises(SystemExit, match="thiếu HOST_A_IP"):
        m2_m4.main(["--only", "m2"])
