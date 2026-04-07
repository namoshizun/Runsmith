from collections.abc import Iterable
from typing import TypeVar

T = TypeVar("T")


def ensure_list(obj: T | Iterable[T]) -> Iterable[T]:
    if isinstance(obj, Iterable):
        return obj
    return [obj]
