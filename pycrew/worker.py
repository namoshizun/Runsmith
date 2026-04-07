import abc
import copy
import dataclasses
import time
from collections.abc import Generator
from functools import cache
from typing import ClassVar, Generic, Literal, TypeVar, cast

from loguru import logger

from pycrew.core import Event, Heartbeat, SyncActorEffectHook
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


@dataclasses.dataclass
class _HooksMap:
    pre: dict[tuple[str, str], list[str]] = dataclasses.field(default_factory=dict)
    post: dict[tuple[str, str], list[str]] = dataclasses.field(default_factory=dict)
    actors: dict[str, str] = dataclasses.field(default_factory=dict)


class WorkerBase(abc.ABC, Generic[TState, TEvent]):
    _hooks: ClassVar[_HooksMap]

    def __init_subclass__(cls, **kwargs: object):
        super().__init_subclass__(**kwargs)
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

    def __init__(self, name: str, fsm: StateMachine[TState, TEvent]):
        self.name = name
        self.fsm = fsm
        self._state: TState = self.fsm.get_initial_state()

    @abc.abstractmethod
    def after_start(self):
        raise NotImplementedError

    @abc.abstractmethod
    def before_exit(self, last_state: TState, is_graceful: bool):
        raise NotImplementedError

    def on_transition(self, src: TState, event: TEvent, tgt: TState):
        logger.opt(colors=True).info(
            f"<e>Worker [{self.name}] state transitioning: {src} -[{event}]-> {tgt}</e>"
        )


ExecutorCommand = Literal["tick", "stop"]
SyncWorkerLoop = Generator[WorkerActivity[TState, TEvent], ExecutorCommand, None]


class CrewSyncWorker(WorkerBase[TState, TEvent]):
    @cache
    def get_actor_func(self, name: str) -> SyncActorEffectHook | None:
        try:
            func_name = self._hooks.actors[name]
            func = getattr(self, func_name)
            return func
        except (KeyError, AttributeError):
            return None

    def after_start(self):
        logger.opt(colors=True).info(f"<e>Worker [{self.name}] is started 🤗</e>")

    def before_exit(self, last_state: TState, is_graceful: bool):
        logger.opt(colors=True).info(
            f"<e>Worker [{self.name}] is exiting from {last_state} {'(gracefully)' if is_graceful else '(abnormally)'} 👋</e>"
        )

    def _exec_actor(self, state: TState) -> Heartbeat | Event | None:
        actor_func = self.get_actor_func(state)
        if actor_func is None:
            # Fall through to the default event if no actor registered
            event_options = self.fsm.get_events(state)
            if len(event_options) != 1:
                logger.warning(
                    f"No actor registered for non-terminal state [{state}], no fallback event available"
                )
                return

            fallback_event = event_options[0]
            logger.warning(
                f"No actor registered for non-terminal state [{state}], fallback to the default event [{fallback_event}]"
            )
            return Event(name=fallback_event)

        assert actor_func is not None
        return actor_func()

    def run(self) -> SyncWorkerLoop:
        self.after_start()
        graceful_termination = False

        try:
            event = self.fsm.get_initial_event()
            cmd: ExecutorCommand = "tick"

            while True:
                if cmd == "stop":
                    # Instructed by executor to exit
                    graceful_termination = True
                    return

                src = self._state
                tgt = self.fsm.get_target_state(src, event)

                # Invoke state transition hooks: src -> [post] -> [pre] -> tgt
                for name in self._hooks.post.get((src, event), ()):
                    getattr(self, name)()
                for name in self._hooks.pre.get((tgt, event), ()):
                    getattr(self, name)()

                self._state = tgt
                self.on_transition(src, event, tgt)

                yield WorkerActivity(
                    kind="transition",
                    transition=(src, event, tgt),
                )

                if tgt in self.fsm.get_terminal_states():
                    # FSM has halted
                    graceful_termination = True
                    return

                # Stay in tgt state until the actor yields an event
                while True:
                    result = self._exec_actor(tgt)
                    match result:
                        case Heartbeat():
                            cmd = yield WorkerActivity(kind="heartbeat")
                        case Event(name=next_event):
                            event = cast(TEvent, next_event)
                            break
                        case None:
                            return

        except Exception as e:
            logger.exception(f"Worker [{self.name}] encountered an error: {e}")
            raise e
        finally:
            self.before_exit(self._state, graceful_termination)


class CrewAsyncWorker:
    async def run(self): ...
