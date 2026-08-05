import dataclasses
import signal
import time
from collections import deque
from typing import Any, Callable, Generic, Literal, Protocol, TypeVar

_mono_now = time.monotonic

EXIT_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT, signal.SIGABRT)

ExecutorCommand = Literal["tick", "stop"] | None

SyncTransitionEffectHook = Callable[[Any], None]


T = TypeVar("T")


class IEvent(Protocol):
    def set(self) -> None: ...
    def is_set(self) -> bool: ...


class IBlockingQueue(Protocol, Generic[T]):
    """Thread/process queue: blocking ``put``, non-blocking ``get_nowait``.

    Parameter names are positional-only so both ``queue.Queue`` (``item``) and
    ``multiprocessing.Queue`` (``obj``) structurally match.
    """

    def put(self, item: T, /) -> None: ...
    def get_nowait(self, /) -> T: ...


class IAsyncQueue(Protocol, Generic[T]):
    """Asyncio queue: awaitable ``put``, non-blocking ``get_nowait``."""

    async def put(self, item: T, /) -> None: ...
    def get_nowait(self, /) -> T: ...


@dataclasses.dataclass(slots=True)
class WorkerRunContext:
    cmd: ExecutorCommand = "tick"
    history: deque[tuple[str, str]] = dataclasses.field(
        default_factory=lambda: deque(maxlen=100),
        metadata={"help": "History of state transitions defined as (event, state)"},
    )
    exception: Exception | None = dataclasses.field(
        default=None,
        metadata={"help": "Exception that occurred during the most recent worker execution"},
    )
    data: Any = dataclasses.field(
        default=None,
        metadata={"help": "Data that can be used to store arbitrary results / side-effect outputs"},
    )

    def add_transition(self, event: str, state: str):
        self.history.append((event, state))

    def set_exception(self, exception: Exception):
        self.exception = exception

    def set_data(self, data: Any):
        self.data = data
