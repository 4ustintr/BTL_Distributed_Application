"""Thêm node-api vào đầu sys.path để test import được module của service này."""

import sys
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parents[2] / "node-api"
if str(SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICE_DIR))
