"""scripts/preflight.py: đánh giá trạng thái trước giờ demo, không cần container."""

import importlib.util
import sys
from dataclasses import replace
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parents[2] / "scripts" / "preflight.py"
_spec = importlib.util.spec_from_file_location("preflight", _PATH)
assert _spec and _spec.loader
preflight = importlib.util.module_from_spec(_spec)
sys.modules["preflight"] = preflight  # dataclass cần module có trong sys.modules
_spec.loader.exec_module(preflight)

KB1_0 = preflight.EXPECTED["KB1-0"]
ONE_MACHINE = "192.168.1.10"


@pytest.fixture
def healthy() -> "preflight.Observed":
    return preflight.Observed(
        local_ips=frozenset({ONE_MACHINE, "127.0.0.1"}),
        host_a=ONE_MACHINE,
        host_b=ONE_MACHINE,
        hq_ok=True,
        dc_status={1: "ONLINE", 2: "ONLINE", 3: "ONLINE"},
        node_modes={1: "ONLINE", 2: "ONLINE", 3: "ONLINE"},
        pending={1: 0, 2: 0, 3: 0},
        connectors={1: "RUNNING RUNNING", 2: "RUNNING RUNNING", 3: "RUNNING RUNNING"},
        status_line=f"[KB1-0] {KB1_0}",
        step="KB1-0",
    )


def failed(checks: list) -> list:
    return [c for c in checks if not c.ok]


def test_all_good_passes(healthy) -> None:
    checks = preflight.assess(healthy)
    assert checks and not failed(checks)


def test_env_ip_not_on_this_machine_fails_with_env_hint(healthy) -> None:
    obs = replace(healthy, local_ips=frozenset({"10.0.0.5"}))
    bad = failed(preflight.assess(obs))
    assert len(bad) == 1
    assert ".env" in bad[0].hint


def test_machine_a_of_two_machines_is_accepted(healthy) -> None:
    obs = replace(healthy, host_b="192.168.1.20", local_ips=frozenset({ONE_MACHINE}))
    assert not failed(preflight.assess(obs))


def test_dc_stuck_recovering_without_pending_explains_what_to_do(healthy) -> None:
    obs = replace(
        healthy,
        dc_status={1: "ONLINE", 2: "ONLINE", 3: "RECOVERING"},
        node_modes={1: "ONLINE", 2: "ONLINE", 3: "RECOVERING"},
    )
    bad = failed(preflight.assess(obs))
    assert [b.title for b in bad] and "Nam" in bad[0].title
    assert "recovery/complete" in bad[0].hint


def test_node_unreachable_points_to_host_b_and_firewall(healthy) -> None:
    obs = replace(healthy, node_modes={1: "ONLINE", 2: None, 3: "ONLINE"})
    bad = failed(preflight.assess(obs))
    assert "Trung" in bad[0].title
    assert "HOST_B_IP" in bad[0].hint and "8002" in bad[0].hint


def test_pending_transactions_are_reported(healthy) -> None:
    obs = replace(healthy, pending={1: 0, 2: 2, 3: 0})
    bad = failed(preflight.assess(obs))
    assert any("ghi hộ" in b.title for b in bad)


def test_connector_task_failed_suggests_restart(healthy) -> None:
    obs = replace(
        healthy, connectors={1: "RUNNING RUNNING", 2: "RUNNING FAILED", 3: "RUNNING RUNNING"}
    )
    bad = failed(preflight.assess(obs))
    assert "dc2-outbox" in bad[0].title
    assert "restart-connector-dc2" in bad[0].hint


def test_status_line_mismatch_shows_expected_line(healthy) -> None:
    obs = replace(
        healthy,
        status_line="[KB1-0] BAC=99/99 TRUNG=40/40 NAM=10/10 TRANSIT=0 "
        "SHIPPED_OUT=1 RECEIVED_IN=150 DIFF=0",
    )
    bad = failed(preflight.assess(obs))
    assert len(bad) == 1 and KB1_0 in bad[0].hint


def test_unknown_step_only_requires_diff_zero(healthy) -> None:
    obs = replace(
        healthy,
        step="KB9-9",
        status_line="[KB9-9] BAC=1/1 TRUNG=1/1 NAM=1/1 TRANSIT=0 "
        "SHIPPED_OUT=0 RECEIVED_IN=3 DIFF=0",
    )
    assert not failed(preflight.assess(obs))
    obs = replace(obs, status_line=obs.status_line.replace("DIFF=0", "DIFF=30"))
    assert failed(preflight.assess(obs))


def test_hq_down_is_reported_first(healthy) -> None:
    obs = replace(healthy, hq_ok=False, dc_status={}, status_line=None)
    bad = failed(preflight.assess(obs))
    assert "HQ API" in bad[0].title
