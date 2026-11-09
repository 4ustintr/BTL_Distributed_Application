"""Expose the shared CDC test helper without changing another owner's conftest."""

import sys

from fixtures import REPO_ROOT

sys.path.insert(0, str(REPO_ROOT / "tests" / "consumer"))

from stack_helpers import kafka_probe, make, reset_stack, wait_caught_up, wait_status  # noqa: E402

__all__ = ["make", "reset_stack", "wait_caught_up", "wait_status", "kafka_probe"]
