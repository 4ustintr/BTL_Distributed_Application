"""Health Monitor: mỗi 2 giây gọi GET /health từng kho – F9, F10.j (Nhật Khánh)."""

from fastapi import FastAPI


async def run(app: FastAPI) -> None:
    """Vòng lặp nền, main.py chạy bằng asyncio.create_task và hủy khi tắt app. Chưa hiện thực."""
