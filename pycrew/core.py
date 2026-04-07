import dataclasses
import time
from typing import Any, Callable

_mono_now = time.monotonic


@dataclasses.dataclass(slots=True)
class Heartbeat:
    timestamp: float = dataclasses.field(default_factory=_mono_now)


@dataclasses.dataclass(slots=True)
class Event:
    name: str
    timestamp: float = dataclasses.field(default_factory=_mono_now)


SyncTransitionEffectHook = Callable[[Any], None]

SyncActorEffectHook = Callable[[], Heartbeat | Event]
