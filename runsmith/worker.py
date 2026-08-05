from __future__ import annotations

import abc
import asyncio
import copy
import dataclasses
import inspect
import sys
import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from functools import cache
from typing import Any, ClassVar, Generic, Literal, cast, overload

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


ExecutionMode = Literal["sync", "async"]
SyncActorFunc = Callable[[], TEvent | Literal["keepalive"]]
AsyncActorFunc = Callable[[], Awaitable[TEvent | Literal["keepalive"]]]


@dataclasses.dataclass(slots=True)
class WorkerActivity:
    kind: Literal["transition_begin", "transition_end", "heartbeat"]
    worker_name: str
    generation: int = 0  # incarnation id; fences activities across restarts
    transition: tuple[str, str, str] | None = None  # (src, event, tgt)
    timestamp: float = dataclasses.field(default_factory=_mono_now)


@dataclasses.dataclass
class _HooksMap:
    pre: dict[tuple[str, str], list[str]] = dataclasses.field(default_factory=dict)
    post: dict[tuple[str, str], list[str]] = dataclasses.field(default_factory=dict)
    # state => (method name, min_interval)
    actors: dict[str, tuple[str, float | int | None]] = dataclasses.field(default_factory=dict)


SyncWorkerLoop = Generator[WorkerActivity, ExecutorCommand, None]
AsyncWorkerLoop = AsyncGenerator[WorkerActivity, ExecutorCommand]


class ActorFunction(Generic[TEvent]):
    def __init__(
        self,
        mode: ExecutionMode,
        *,
        cb: Callable[..., Any] | None = None,
        always: TEvent | None = None,
        min_interval: int | float | None = None,
    ):
        if cb is None:
            assert always is not None

        self.always_evt = always
        self.min_interval = min_interval
        self.execution_mode = mode
        self.cb = cb
        self.__last_at = 0.0

    def _invoke(self, sleep_for: float):
        if sleep_for > 0:
            time.sleep(sleep_for)

        if self.always_evt:
            return self.always_evt

        assert self.cb is not None
        return self.cb()

    async def _ainvoke(self, sleep_for: float):
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)

        if self.always_evt:
            return self.always_evt

        assert self.cb is not None
        return await self.cb()

    def __call__(self):
        elapsed = (now := time.monotonic()) - self.__last_at
        self.__last_at = now

        # Handle throttling
        if self.min_interval and self.min_interval > 0:
            sleep_for = self.min_interval - elapsed
        else:
            sleep_for = 0

        sleep_for = max(0, sleep_for)
        if self.execution_mode == "sync":
            return self._invoke(sleep_for)
        return self._ainvoke(sleep_for)


class _WorkerMeta(abc.ABCMeta):
    exclude_init_params = frozenset({"self", "name", "fsm"})

    def __new__(
        mcls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, object],
        **kwargs: object,
    ):
        cls = super().__new__(mcls, name, bases, namespace, **kwargs)
        if name == "WorkerBase":
            return cls

        hooks = copy.deepcopy(getattr(cls, "_hooks", _HooksMap()))
        for _attr in vars(cls).values():
            is_coro = inspect.iscoroutinefunction(_attr)

            # Per the decorated hook method
            for hook in getattr(_attr, HOOK_ATTR, ()):
                exec_mode = getattr(cls, "execution_mode")
                # Ensure the hook function is compatible with the worker's execution mode
                if exec_mode == "sync" and is_coro:
                    raise InvalidHookFunctionTypeError(
                        f"{cls.__name__}.{_attr.__name__} is an async hook but the worker is a sync worker"
                    )
                if exec_mode == "async" and not is_coro:
                    raise InvalidHookFunctionTypeError(
                        f"{cls.__name__}.{_attr.__name__} is a sync hook but the worker is an async worker"
                    )

                # Register the hook function
                match hook:
                    case ("pre", state, event):
                        hooks.pre.setdefault((state, event), []).append(_attr.__name__)
                    case ("post", state, event):
                        hooks.post.setdefault((state, event), []).append(_attr.__name__)
                    case ("actor", state, min_interval):
                        hooks.actors[state] = (_attr.__name__, min_interval)

        setattr(cls, "_hooks", hooks)
        return cls

    def __call__(cls, *args: object, **kwargs: object):
        instance = super().__call__(*args, **kwargs)
        sig = inspect.signature(cls.__init__)
        bound = sig.bind(None, *args, **kwargs)
        bound.apply_defaults()
        meta: dict[str, object] = {}
        for key, value in bound.arguments.items():
            if key in cls.exclude_init_params:
                continue
            param = sig.parameters[key]
            if param.kind is inspect.Parameter.VAR_KEYWORD:
                meta.update({k: v for k, v in value.items() if k not in cls.exclude_init_params})
            elif param.kind is not inspect.Parameter.VAR_POSITIONAL:
                meta[key] = value
        instance._meta = meta
        return instance


class WorkerBase(abc.ABC, Generic[TState, TEvent], metaclass=_WorkerMeta):
    _hooks: ClassVar[_HooksMap]
    execution_mode: ClassVar[Literal["sync", "async"]]
    _meta: dict[str, object]

    def __init__(self, name: str, fsm: StateMachine[TState, TEvent] = DefaultWorkerFSM):
        self.name = name
        self.fsm = copy.deepcopy(fsm)
        self.ctx: WorkerRunContext = WorkerRunContext()
        self._state: TState = self.fsm.get_initial_state()
        self.generation: int = 0

    def before_exit(self, is_graceful: bool):
        logger.opt(colors=True).info(
            f"<e>Worker [{self.name}] is exiting from {self._state} {'(gracefully)' if is_graceful else '(abnormally)'} 👋</e>"
        )

    def clone(self) -> Self:
        kwargs = dict(self._meta)
        params = inspect.signature(self.__class__.__init__).parameters
        accepts_fsm = "fsm" in params or any(
            p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()
        )
        if accepts_fsm:
            kwargs["fsm"] = self.fsm
        return self.__class__(name=self.name, **kwargs)  # pyright: ignore[reportArgumentType]

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
        if state == self.fsm.get_initial_state():
            return ActorFunction[TEvent](self.execution_mode, always=self.fsm.get_initial_event())

        try:
            method_name, min_interval = self._hooks.actors[state]
            return ActorFunction[TEvent](
                self.execution_mode,
                cb=getattr(self, method_name),
                min_interval=min_interval,
            )
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
            return ActorFunction[TEvent](self.execution_mode, always=fallback_event)

    def make_activity(self, **kwargs: object) -> WorkerActivity:
        return WorkerActivity(
            worker_name=self.name,
            generation=self.generation,
            **kwargs,  # pyright: ignore[reportArgumentType]
        )


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
