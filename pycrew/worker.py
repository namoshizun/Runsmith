import abc
import copy
import dataclasses
import inspect
import sys
import time
from collections.abc import AsyncGenerator, Generator
from functools import cache
from typing import ClassVar, Generic, Literal, cast

from loguru import logger

if sys.version_info >= (3, 13):
    from typing import TypeVar  # pyright: ignore[reportUnreachable]
else:
    from typing_extensions import TypeVar  # pyright: ignore[reportUnreachable]

if sys.version_info >= (3, 11):
    from typing import Self  # pyright: ignore[reportUnreachable]
else:
    from typing_extensions import Self  # pyright: ignore[reportUnreachable]

from pycrew.core import ExecutorCommand, WorkerRunContext
from pycrew.decorators import HOOK_ATTR
from pycrew.defaults import DefaultWorkerEvent, DefaultWorkerFSM, DefaultWorkerState
from pycrew.state import StateMachine

_mono_now = time.monotonic

TEvent = TypeVar("TEvent", bound=str, default=DefaultWorkerEvent)
TState = TypeVar("TState", bound=str, default=DefaultWorkerState)


@dataclasses.dataclass(slots=True)
class WorkerActivity:
    kind: Literal["transition_begin", "transition_end", "heartbeat"]
    worker_name: str
    transition: tuple[str, str, str] | None = None  # (src, event, tgt)
    timestamp: float = dataclasses.field(default_factory=_mono_now)


@dataclasses.dataclass
class _HooksMap:
    pre: dict[tuple[str, str], list[str]] = dataclasses.field(default_factory=dict)
    post: dict[tuple[str, str], list[str]] = dataclasses.field(default_factory=dict)
    actors: dict[str, str] = dataclasses.field(default_factory=dict)


SyncWorkerLoop = Generator[WorkerActivity, ExecutorCommand, None]
AsyncWorkerLoop = AsyncGenerator[WorkerActivity, ExecutorCommand]


class WorkerBase(abc.ABC, Generic[TState, TEvent]):
    _hooks: ClassVar[_HooksMap]

    def __init_subclass__(cls, **kwargs: object):
        super().__init_subclass__(**kwargs)

        # Enforce custom clone method if __init__ is overridden
        if "__init__" in cls.__dict__ and "clone" not in cls.__dict__:
            raise TypeError(
                f"{cls.__name__} overrides __init__ but does not override clone(). "
                "You must implement clone() so the supervisor can safely reconstruct this worker."
            )

        # Initialize the hooks map
        hooks = copy.deepcopy(getattr(cls, "_hooks", _HooksMap()))
        for attr_val in vars(cls).values():
            # Per the decorated hook method
            for hook in getattr(attr_val, HOOK_ATTR, ()):
                match hook:
                    case ("pre", state, event):
                        hooks.pre.setdefault((state, event), []).append(attr_val.__name__)
                    case ("post", state, event):
                        hooks.post.setdefault((state, event), []).append(attr_val.__name__)
                    case ("actor", state):
                        hooks.actors[state] = attr_val.__name__

        cls._hooks = hooks

    def __init__(self, name: str, fsm: StateMachine[TState, TEvent] = DefaultWorkerFSM):
        self.name = name
        self.fsm = copy.deepcopy(fsm)
        self.ctx: WorkerRunContext = WorkerRunContext()
        self._state: TState = self.fsm.get_initial_state()

    @abc.abstractmethod
    def before_exit(self, is_graceful: bool):
        raise NotImplementedError

    @cache
    def get_actor_func(self, name: str):
        try:
            func_name = self._hooks.actors[name]
            func = getattr(self, func_name)

            params = inspect.signature(func).parameters
            return func, "cmd" in params
        except (KeyError, AttributeError):
            return None

    def clone(self) -> Self:
        return self.__class__(name=self.name, fsm=self.fsm)

    def emit(self, signal: TEvent | Literal["keepalive"]):
        # A thin wrapper to make the typing work
        return signal


class SyncWorker(WorkerBase[TState, TEvent]):
    def before_exit(self, is_graceful: bool):
        logger.opt(colors=True).info(
            f"<e>Worker [{self.name}] is exiting from {self._state} {'(gracefully)' if is_graceful else '(abnormally)'} 👋</e>"
        )

    def _exec_actor(self, state: TState, cmd: ExecutorCommand) -> TEvent | Literal["keepalive"]:
        if self._state is self.fsm.get_initial_state():
            return self.fsm.get_initial_event()

        actor_inspect = self.get_actor_func(state)
        if actor_inspect is None:
            # Fall through to the default event if no actor registered
            event_options = self.fsm.get_events(state)
            if len(event_options) != 1:
                raise RuntimeError(
                    f"No actor registered for non-terminal state [{state}], no fallback event available"
                )

            fallback_event = event_options[0]
            logger.warning(
                f"No actor registered for non-terminal state [{state}], fallback to the default event [{fallback_event}]"
            )
            return fallback_event

        assert actor_inspect is not None
        func, accepts_cmd = actor_inspect

        if accepts_cmd:
            return func(cmd=cmd)
        return func()

    def main_loop(self) -> SyncWorkerLoop:
        MakeActivity = lambda **kwargs: WorkerActivity(worker_name=self.name, **kwargs)  # pyright: ignore[reportUnknownVariableType]  # noqa: N806
        terminal_states = self.fsm.get_terminal_states()

        # Send the initial heartbeat to indicate the start of the loop
        cmd: ExecutorCommand = yield MakeActivity(kind="heartbeat")
        graceful_terminated = False

        try:
            while self._state not in terminal_states:
                event_or_beat = self._exec_actor(self._state, cmd)

                # Heart-beating
                if event_or_beat == "keepalive":
                    cmd = yield MakeActivity(kind="heartbeat")
                    continue

                # State transition
                event = cast(TEvent, event_or_beat)
                src = self._state
                tgt = self.fsm.get_target_state(src, event)
                transition = (src, event, tgt)
                cmd = yield MakeActivity(kind="transition_begin", transition=transition)

                # Invoke state transition hooks: src -> [post] -> [pre] -> tgt
                for name in self._hooks.post.get((src, event), ()):
                    getattr(self, name)()
                for name in self._hooks.pre.get((tgt, event), ()):
                    getattr(self, name)()

                self._state = tgt
                cmd = yield MakeActivity(kind="transition_end", transition=transition)
                self.ctx.add_transition(event, tgt)

            graceful_terminated = True
        except Exception as e:
            logger.exception(f"Worker [{self.name}] encountered an error: {e}")
            self.ctx.set_exception(e)
            raise e
        finally:
            self.before_exit(graceful_terminated)


class AsyncWorker(WorkerBase[TState, TEvent]):
    async def main_loop(self) -> AsyncWorkerLoop:
        worker_name = self.name
        yield WorkerActivity(worker_name=worker_name, kind="heartbeat")
