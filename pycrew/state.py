import copy
import sys
from collections.abc import Iterable
from types import EllipsisType
from typing import Generic, Protocol, TypeVar

from pycrew.constraints import HeartbeatTimeout, StateTimeout, Timeout, TransitionTimeout
from pycrew.errors import InvalidStateMachineError, InvalidTransitionError

TState = TypeVar("TState", bound=str)
TEvent = TypeVar("TEvent", bound=str)

TransitionTable = dict[TState, dict[TEvent, TState] | EllipsisType]


class IPrettyPrinter(Protocol):
    def print(self) -> None: ...


class StateMachine(Generic[TState, TEvent]):
    def __init__(
        self,
        transitions: TransitionTable[TState, TEvent],
        initial_event: TEvent,
        constraints: Iterable[Timeout] = tuple(),
        pretty_printer: IPrettyPrinter | None = None,
    ):
        self._transitions: TransitionTable[TState, TEvent] = copy.deepcopy(transitions)
        self._initial_state: TState | None = None
        self._initial_event = initial_event
        self._terminal_states: set[TState] = set()
        self._constraints: tuple[Timeout, ...] = tuple(constraints)

        # Parse the state machine
        self.__build_states()

        # Run verifications
        self.__verify_constraints()

        try:
            assert self._initial_state is not None
            self.get_target_state(self._initial_state, self._initial_event)
        except InvalidTransitionError:
            raise ValueError(
                f"Invalid initial event: {self._initial_event} given initial state {self._initial_state}"
            )

        self._pretty_printer = pretty_printer

    def __verify_constraints(self):
        for c in self._constraints:
            match c:
                case HeartbeatTimeout():
                    if c.when not in self._transitions:
                        raise ValueError(f"Heartbeat timeout for unknown state: {c.when}")
                case TransitionTimeout():
                    src, tgt = c.get_src_and_tgt()
                    options = self._transitions.get(src, {})  # pyright: ignore
                    if tgt not in options.values():
                        raise ValueError(
                            f"Invalid transition options for the transition timeout constraint: {c.when}"
                        )
                case StateTimeout():
                    if c.when not in self._transitions:
                        raise ValueError(f"State timeout for unknown state: {c.when}")

    def __build_states(self):
        # Walk through the FSM to collect the initial and terminal states
        initial_states = set(self._transitions.keys())
        for src_state, trans in self._transitions.items():
            if isinstance(trans, EllipsisType):
                self._terminal_states.add(src_state)
                continue

            for _, tgt_state in trans.items():
                initial_states.discard(tgt_state)

        if len(initial_states) != 1:
            raise InvalidStateMachineError(
                f"Expected exactly one initial state, got {len(initial_states)}"
            )

        self._initial_state = next(iter(initial_states))

        if not self._terminal_states:
            raise InvalidStateMachineError("No terminal states found")

    def get_initial_state(self) -> TState:
        assert self._initial_state is not None
        return self._initial_state

    def get_initial_event(self) -> TEvent:
        return self._initial_event

    def get_terminal_states(self) -> set[TState]:
        return self._terminal_states

    def get_transitions(self) -> TransitionTable[TState, TEvent]:
        return self._transitions

    def get_constraints(self) -> Iterable[Timeout]:
        return self._constraints

    def get_target_state(self, state: TState, event: TEvent) -> TState:
        try:
            return self._transitions[state][event]  # pyright: ignore[reportIndexIssue]
        except KeyError:
            raise InvalidTransitionError(state, event)

    def get_events(self, state: TState) -> list[TEvent]:
        try:
            return list(self._transitions[state].keys())  # pyright: ignore[reportAttributeAccessIssue]
        except (AttributeError, KeyError):
            return []

    @property
    def pretty_printer(self) -> IPrettyPrinter:
        if self._pretty_printer is None:
            from pycrew.defaults import DefaultFSNPrettyPrinter

            self._pretty_printer = DefaultFSNPrettyPrinter(self, file=sys.stdout)
        return self._pretty_printer

    def pretty_print(self) -> None:
        self.pretty_printer.print()
