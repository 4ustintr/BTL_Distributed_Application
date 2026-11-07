"""Replay Worker: mỗi 2 giây đẩy pending_txn về kho đang RECOVERING – F9, F10.i (Nhật Khánh)."""

from fastapi import FastAPI


async def run(app: FastAPI) -> None:
    """Vòng lặp nền, main.py chạy bằng asyncio.create_task và hủy khi tắt app. Chưa hiện thực."""
