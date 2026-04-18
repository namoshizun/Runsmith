from __future__ import annotations

import abc
import copy
import dataclasses
import inspect
import sys
import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from functools import cache
from typing import ClassVar, Generic, Literal, cast, overload

from loguru import logger

from runsmith.errors import InvalidHookFunctionTypeError

if sys.version_info >= (3, 13):
    from typing import TypeVar  # pyright: ignore[reportUnreachable]
else:
    from typing_extensions import TypeVar  # pyright: ignore[reportUnreachable]

if sys.version_info >= (3, 11):
    from typing import Self  # pyright: ignore[reportUnreachable]
else:
    from typing_extensions import Self  # pyright: ignore[reportUnreachable]

from runsmith.core import ExecutorCommand, WorkerRunContext
from runsmith.decorators import HOOK_ATTR
from runsmith.defaults import DefaultWorkerFSM
from runsmith.state import StateMachine

_mono_now = time.monotonic

TEvent = TypeVar("TEvent", bound=str)
TState = TypeVar("TState", bound=str)


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
    execution_mode: ClassVar[Literal["sync", "async"]]

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
            is_coro = inspect.iscoroutinefunction(attr_val)

            # Per the decorated hook method
            for hook in getattr(attr_val, HOOK_ATTR, ()):
                # Ensure the hook function is compatible with the worker's execution mode
                if cls.execution_mode == "sync" and is_coro:
                    raise InvalidHookFunctionTypeError(
                        f"{cls.__name__}.{attr_val.__name__} is an async hook but the worker is a sync worker"
                    )

                if cls.execution_mode == "async" and not is_coro:
                    raise InvalidHookFunctionTypeError(
                        f"{cls.__name__}.{attr_val.__name__} is a sync hook but the worker is an async worker"
                    )

                # Register the hook function
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

    def before_exit(self, is_graceful: bool):
        logger.opt(colors=True).info(
            f"<e>Worker [{self.name}] is exiting from {self._state} {'(gracefully)' if is_graceful else '(abnormally)'} 👋</e>"
        )

    def clone(self) -> Self:
        return self.__class__(name=self.name, fsm=self.fsm)

    def emit(self, signal: TEvent | Literal["keepalive"]):
        # A thin wrapper to make the typing work
        return signal

    @overload
    def get_actor_func(
        self: SyncWorker[TState, TEvent], state: TState
    ) -> Callable[[], TEvent | Literal["keepalive"]]: ...

    @overload
    def get_actor_func(
        self: AsyncWorker[TState, TEvent], state: TState
    ) -> Callable[[], Awaitable[TEvent | Literal["keepalive"]]]: ...

    @cache
    def get_actor_func(self, state: TState):
        def make_single_event_actor(event: TEvent):
            async def async_inner():
                return event

            if self.execution_mode == "sync":
                return lambda: event
            return async_inner

        if state == self.fsm.get_initial_state():
            return make_single_event_actor(self.fsm.get_initial_event())

        try:
            func_name = self._hooks.actors[state]
            func = getattr(self, func_name)
            return func
        except (KeyError, AttributeError):
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
            return make_single_event_actor(fallback_event)

    def make_activity(self, **kwargs: object) -> WorkerActivity:
        return WorkerActivity(worker_name=self.name, **kwargs)  # pyright: ignore[reportArgumentType]


class SyncWorker(WorkerBase[TState, TEvent]):
    execution_mode = "sync"

    def main_loop(self) -> SyncWorkerLoop:
        terminal_states = self.fsm.get_terminal_states()

        # Send the initial heartbeat to indicate the start of the loop
        self.ctx.cmd = yield self.make_activity(kind="heartbeat")
        graceful_terminated = False

        try:
            while self._state not in terminal_states:
                actor_func = self.get_actor_func(self._state)
                event_or_beat = actor_func()

                # Heart-beating
                if event_or_beat == "keepalive":
                    self.ctx.cmd = yield self.make_activity(kind="heartbeat")
                    continue

                # State transition
                event = cast(TEvent, event_or_beat)
                src = self._state
                tgt = self.fsm.get_target_state(src, event)
                transition = (src, event, tgt)
                logger.info(f"State transition [{self.name}]: {src} -[{event}] -> {tgt}")
                self.ctx.cmd = yield self.make_activity(
                    kind="transition_begin", transition=transition
                )

                # Invoke state transition hooks: src -> [post] -> [pre] -> tgt
                for name in self._hooks.post.get((src, event), ()):
                    getattr(self, name)()
                for name in self._hooks.pre.get((tgt, event), ()):
                    getattr(self, name)()

                self._state = tgt
                self.ctx.cmd = yield self.make_activity(
                    kind="transition_end", transition=transition
                )
                self.ctx.add_transition(event, tgt)

            graceful_terminated = True
        except Exception as e:
            logger.exception(f"Worker [{self.name}] encountered an error: {e}")
            self.ctx.set_exception(e)
            raise e
        finally:
            self.before_exit(graceful_terminated)


class AsyncWorker(WorkerBase[TState, TEvent]):
    execution_mode = "async"

    async def main_loop(self) -> AsyncWorkerLoop:
        terminal_states = self.fsm.get_terminal_states()

        # Send the initial heartbeat to indicate the start of the loop
        self.ctx.cmd = yield self.make_activity(kind="heartbeat")
        graceful_terminated = False

        try:
            while self._state not in terminal_states:
                actor_func = self.get_actor_func(self._state)
                event_or_beat = await actor_func()

                # Heart-beating
                if event_or_beat == "keepalive":
                    self.ctx.cmd = yield self.make_activity(kind="heartbeat")
                    continue

                # State transition
                event = cast(TEvent, event_or_beat)
                src = self._state
                tgt = self.fsm.get_target_state(src, event)
                transition = (src, event, tgt)
                logger.info(f"State transition [{self.name}]: {src} -[{event}] -> {tgt}")
                self.ctx.cmd = yield self.make_activity(
                    kind="transition_begin", transition=transition
                )

                # Invoke state transition hooks: src -> [post] -> [pre] -> tgt
                for name in self._hooks.post.get((src, event), ()):
                    hook_func = getattr(self, name)
                    await hook_func()
                for name in self._hooks.pre.get((tgt, event), ()):
                    hook_func = getattr(self, name)
                    await hook_func()

                self._state = tgt
                self.ctx.cmd = yield self.make_activity(
                    kind="transition_end", transition=transition
                )
                self.ctx.add_transition(event, tgt)

            graceful_terminated = True
        except Exception as e:
            logger.exception(f"Worker [{self.name}] encountered an error: {e}")
            self.ctx.set_exception(e)
            raise e
        finally:
            self.before_exit(graceful_terminated)
