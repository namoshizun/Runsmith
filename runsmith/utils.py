import asyncio
import time
from collections import deque
from collections.abc import Coroutine
from typing import Literal

from loguru import logger


class Timer:
    def __init__(
        self,
        unit: Literal["s", "ms"] = "ms",
        warning_thresh: float | int | None = None,
        warning_message: str | None = None,
        rounded: bool = False,
    ):
        self.unit = unit
        self.rounded = rounded
        self.warning_thresh = warning_thresh
        self.warning_message = warning_message

    def elapsed(self):
        return time.monotonic() - self.start

    def __enter__(self):
        self.start = time.monotonic()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # pyright: ignore[reportMissingParameterType]
        cost = self.elapsed()
        if self.unit == "ms":
            self.duration = cost * 1000
        else:
            self.duration = cost

        if self.rounded:
            self.duration = round(self.duration)

        if self.warning_thresh and self.warning_message and self.duration > self.warning_thresh:
            try:
                logger.warning(self.warning_message.format(duration=self.duration))
            except Exception:
                logger.warning(self.warning_message)


class CoroutineQueue:
    def __init__(self, *, max_pending: int):
        if max_pending <= 0:
            raise ValueError("max_pending must be a positive integer")

        self._max_pending = max_pending
        self._pending: deque[asyncio.Task[None]] = deque()
        self._errors: deque[BaseException] = deque()

    def _collect_completed(self):
        pending_count = len(self._pending)
        for _ in range(pending_count):
            task = self._pending.popleft()
            if not task.done():
                self._pending.append(task)
                continue

            try:
                task.result()
            except asyncio.CancelledError:
                continue
            except Exception as exc:
                self._errors.append(exc)

    def submit(self, coro: Coroutine[None, None, None]) -> bool:
        self._collect_completed()
        self._pending.append(asyncio.create_task(coro))

        is_overflowed = len(self._pending) > self._max_pending
        if not is_overflowed:
            return False

        # Drop the oldest task to make room for the new one
        dropped = self._pending.popleft()
        if not dropped.done():
            dropped.cancel()
        return True

    def flush_errors(self):
        self._collect_completed()
        while self._errors:
            exc = self._errors.popleft()
            logger.opt(exception=exc).error("Background callback task failed")

    async def drain(self):
        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)
        self._collect_completed()
