import abc
import dataclasses
import time
from collections.abc import Callable, Generator
from typing import Generic, Literal, TypeVar

from loguru import logger

from pycrew.decorators import HOOK_ATTR
from pycrew.state import StateMachine

_mono_now = time.monotonic

TEvent = TypeVar("TEvent", bound=str)
TState = TypeVar("TState", bound=str)


@dataclasses.dataclass(slots=True)
class WorkerActivity(Generic[TState, TEvent]):
    kind: Literal["transition", "heartbeat"]
    transition: tuple[TState, TEvent, TState] | None = None
    timestamp: float = dataclasses.field(default_factory=_mono_now)


@dataclasses.dataclass(slots=True)
class _EffectsRegistry:
    pre: dict[tuple, list[Callable]] = dataclasses.field(default_factory=dict)
    post: dict[tuple, list[Callable]] = dataclasses.field(default_factory=dict)
    actors: dict[str, Callable] = dataclasses.field(default_factory=dict)


class WorkerBase(abc.ABC, Generic[TState, TEvent]):
    def __init__(self, name: str, fsm: StateMachine[TState, TEvent]):
        self.name = name
        self.fsm = fsm
        self._state: TState = self.fsm.get_initial_state()
        self._effects = self._build_effects_registry()

    def _build_effects_registry(self) -> _EffectsRegistry:
        registry = _EffectsRegistry()
        for cls in type(self).__mro__:
            for _, attr_val in vars(cls).items():
                hooks = getattr(attr_val, HOOK_ATTR, None)
                if not hooks:
                    continue
                bound = getattr(self, attr_val.__name__)
                for hook in hooks:
                    kind = hook[0]
                    if kind in ("pre", "post"):
                        _, state, event = hook
                        bucket = registry.pre if kind == "pre" else registry.post
                        bucket.setdefault((state, event), []).append(bound)
                    elif kind == "actor":
                        registry.actors.setdefault(hook[1], bound)
        return registry

    @abc.abstractmethod
    def after_start(self):
        raise NotImplementedError

    @abc.abstractmethod
    def before_exit(self, is_graceful: bool):
        raise NotImplementedError


ExecutorCommand = Literal["tick", "stop"]
SyncWorkerLoop = Generator[WorkerActivity[TState, TEvent], ExecutorCommand, None]


class CrewSyncWorker(WorkerBase[TState, TEvent]):
    def after_start(self):
        logger.opt(colors=True).info(f"<e>Worker [{self.name}] is started 🤗</e>")

    def before_exit(self, is_graceful: bool):
        logger.opt(colors=True).info(
            f"<e>Worker [{self.name}] is exiting {'(gracefully)' if is_graceful else '(abnormally)'} 👋</e>"
        )

    def run(self) -> SyncWorkerLoop:
        self.after_start()
        graceful_termination = False
        try:
            event = self.fsm.get_initial_event()

            while True:
                src = self._state
                tgt = self.fsm.get_target_state(src, event)

                for cb in self._effects.post.get((src, event), ()):
                    cb()
                for cb in self._effects.pre.get((tgt, event), ()):
                    cb()

                self._state = tgt

                cmd = yield WorkerActivity(
                    kind="transition",
                    transition=(src, event, tgt),
                )
                if cmd == "stop":
                    graceful_termination = True
                    return

                if tgt in self.fsm.get_terminal_states():
                    graceful_termination = True
                    return

                actor_fn = self._effects.actors.get(tgt)
                if actor_fn is None:
                    logger.warning(f"No actor registered for non-terminal state '{tgt}'")
                    return

                for result in actor_fn():
                    if result is None:
                        cmd = yield WorkerActivity(kind="heartbeat")
                        if cmd == "stop":
                            graceful_termination = True
                            return
                    else:
                        event = result
                        break
                else:
                    return
        finally:
            self.before_exit(graceful_termination)


class CrewAsyncWorker:
    async def run(self): ...
