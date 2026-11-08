"""Vòng lặp nền theo chu kỳ cố định, dùng chung cho Health Monitor và Replay Worker (Nhật Khánh)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable


async def run_periodic(
    interval_s: float, tick: Callable[[], Awaitable[None]], log: logging.Logger
) -> None:
    """Gọi tick() mỗi interval_s giây, tính từ lúc bắt đầu vòng trước (F11: chu kỳ 2 giây).

    Một vòng lỗi (CSDL chưa sẵn sàng, lỗi bất ngờ) chỉ ghi log, vòng sau vẫn chạy.
    Hủy task (main.py khi tắt app) thì dừng ngay.
    """
    loop = asyncio.get_running_loop()
    next_at = loop.time()
    while True:
        try:
            await tick()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Vòng chạy lỗi, thử lại ở chu kỳ sau")
        next_at += interval_s
        delay = next_at - loop.time()
        if delay < 0:  # vòng chạy lâu hơn chu kỳ: không dồn vòng bù
            next_at = loop.time()
            delay = 0
        await asyncio.sleep(delay)
