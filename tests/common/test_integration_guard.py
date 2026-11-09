"""tests/conftest.py + tests/fixtures.py: test tích hợp chỉ chạy khi HOST_A_IP = HOST_B_IP."""

from pathlib import Path

from fixtures import INTEGRATION_SAME_HOST_MSG, host_ips, integration_skip_reason, read_env_file


def test_skip_reason_when_hosts_differ() -> None:
    assert integration_skip_reason("192.168.1.10", "192.168.1.20") == INTEGRATION_SAME_HOST_MSG
    assert INTEGRATION_SAME_HOST_MSG == "test tích hợp cần HQ và kho cùng một máy"


def test_no_skip_when_same_host() -> None:
    assert integration_skip_reason("192.168.1.6", "192.168.1.6") is None


def test_read_env_file(tmp_path: Path) -> None:
    f = tmp_path / ".env"
    f.write_text(
        "# chú thích\n\nHOST_A_IP=10.0.0.1\nHOST_B_IP = '10.0.0.2'\nkhong_co_dau_bang\n",
        encoding="utf-8",
    )
    assert read_env_file(f) == {"HOST_A_IP": "10.0.0.1", "HOST_B_IP": "10.0.0.2"}
    assert read_env_file(tmp_path / "khong_co.env") == {}


def test_host_ips_env_overrides_file(tmp_path: Path) -> None:
    f = tmp_path / ".env"
    f.write_text("HOST_A_IP=10.0.0.1\nHOST_B_IP=10.0.0.2\n", encoding="utf-8")
    assert host_ips({}, f) == ("10.0.0.1", "10.0.0.2")
    assert host_ips({"HOST_B_IP": "10.0.0.1"}, f) == ("10.0.0.1", "10.0.0.1")


def test_host_ips_reads_env_file_from_environ(tmp_path: Path) -> None:
    f = tmp_path / "may.env"
    f.write_text("HOST_A_IP=1.1.1.1\nHOST_B_IP=1.1.1.1\n", encoding="utf-8")
    # ENV_FILE tuyệt đối: REPO_ROOT / path tuyệt đối = path đó
    assert host_ips({"ENV_FILE": str(f)}) == ("1.1.1.1", "1.1.1.1")
