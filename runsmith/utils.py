import time
from collections.abc import Iterable
from typing import Literal, TypeVar

from loguru import logger

T = TypeVar("T")


def ensure_list(obj: T | Iterable[T]) -> Iterable[T]:
    if isinstance(obj, Iterable):
        return obj
    return [obj]


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
