from collections.abc import Iterable
from typing import Literal, TextIO

from runsmith.constraints import HeartbeatTimeout, StateTimeout, Timeout, TransitionTimeout
from runsmith.state import StateMachine, Terminal, TransitionTable

DefaultWorkerState = Literal["idle", "starting", "running", "terminating", "crashed", "stopped"]
DefaultWorkerEvent = Literal["start", "run", "complete", "error"]


DefaultTransitionTable: TransitionTable[DefaultWorkerState, DefaultWorkerEvent] = {
    "idle": {"start": "starting"},
    "starting": {"run": "running", "error": "crashed"},
    "running": {"complete": "terminating", "error": "crashed"},
    "terminating": {"complete": "stopped", "error": "crashed"},
    "crashed": Terminal.ERROR,
    "stopped": Terminal.OK,
}

# ---- Worker
DefaultWorkerConstraints: Iterable[Timeout] = [
    # Actor liveness
    HeartbeatTimeout(timeout=2, when="running"),
    # Transition hook guards
    TransitionTimeout(timeout=1, when="idle -> starting"),
    TransitionTimeout(timeout=1, when="starting -> running"),
    TransitionTimeout(timeout=1, when="running -> terminating"),
    TransitionTimeout(timeout=1, when="terminating -> stopped"),
    # State residence caps
    StateTimeout(timeout=10, when="starting"),
    StateTimeout(timeout=10, when="terminating"),
]

DefaultWorkerFSM = StateMachine[DefaultWorkerState, DefaultWorkerEvent](
    transitions=DefaultTransitionTable,
    initial_event="start",
    constraints=DefaultWorkerConstraints,
)

# ---- Supervisor
# Supervisors may own a subtree, so they get one extra state: a failing supervisor must reap its
# children before crashing to avoid orphan children.
SupervisorState = DefaultWorkerState | Literal["reaping"]

SupervisorTransitionTable: TransitionTable[SupervisorState, DefaultWorkerEvent] = {
    "idle": {"start": "starting"},
    "starting": {"run": "running", "error": "crashed"},
    "running": {"complete": "terminating", "error": "reaping"},
    "reaping": {"complete": "crashed", "error": "crashed"},
    "terminating": {"complete": "stopped", "error": "crashed"},
    "crashed": Terminal.ERROR,
    "stopped": Terminal.OK,
}


SupervisorConstraints: Iterable[Timeout] = [
    *DefaultWorkerConstraints,
    TransitionTimeout(timeout=1, when="running -> reaping"),
    TransitionTimeout(timeout=1, when="reaping -> crashed"),
    StateTimeout(timeout=10, when="reaping"),
]


SupervisorFSM = StateMachine[SupervisorState, DefaultWorkerEvent](
    transitions=SupervisorTransitionTable,
    initial_event="start",
    constraints=SupervisorConstraints,
)


class DefaultFSNPrettyPrinter:
    def __init__(self, fsm: StateMachine, *, file: TextIO):
        self.fsm = fsm
        self.file = file

    def _state_names(self) -> set:
        names: set = set()
        for source, row in self.fsm.get_transitions().items():
            names.add(source)
            if isinstance(row, Terminal):
                continue
            names.update(row.values())  # pyright: ignore[reportAttributeAccessIssue]
        return names

    def _states_in_print_order(self) -> list:
        initial = self.fsm.get_initial_state()
        return sorted(self._state_names(), key=lambda s: (s != initial, s))

    def _constraint_print_hints(
        self,
    ) -> tuple[dict[str, float], dict[tuple[str, str], float], dict[str, float]]:
        """Per-state keepalives, per-(source,target) transition timeouts, and state timeouts."""
        keepalive: dict[str, float] = {}
        transition: dict[tuple[str, str], float] = {}
        state_timeout: dict[str, float] = {}
        for c in self.fsm.get_constraints():
            match c:
                case HeartbeatTimeout():
                    keepalive[c.when] = c.timeout
                case TransitionTimeout():
                    src, tgt = c.get_src_and_tgt()
                    transition[(src, tgt)] = c.timeout
                case StateTimeout():
                    state_timeout[c.when] = c.timeout
                case _:
                    pass
        return keepalive, transition, state_timeout

    def print(self) -> None:
        """
        Example output:

        → idle  (initial)
              start → starting

          crashed  (terminal, error)

          running  (keepalive=2)
              error → crashed
              terminate → terminating  (timeout=3)

          starting
              run → running  (timeout=10)

          stopped  (terminal)

          terminating
              complete → stopped  (timeout=10)
              error → crashed
        """
        initial = self.fsm.get_initial_state()
        keepalives, trans_timeouts, state_timeouts = self._constraint_print_hints()

        lines: list[str] = []
        for state in self._states_in_print_order():
            if lines:
                lines.append("")

            outcome = self.fsm.get_terminal_outcome(state)
            tags = (
                (["initial"] if state == initial else [])
                + (["terminal"] if outcome is not None else [])
                + (["error"] if outcome is Terminal.ERROR else [])
                + ([f"keepalive={keepalives[state]}"] if state in keepalives else [])
                + ([f"state_timeout={state_timeouts[state]}"] if state in state_timeouts else [])
            )
            prefix = "→ " if state == initial else "  "
            suffix = f"  ({', '.join(tags)})" if tags else ""
            lines.append(f"{prefix}{state}{suffix}")

            row = self.fsm.get_transitions().get(state)
            if row is None or isinstance(row, Terminal):
                continue
            for event in sorted(row, key=str):
                target = row[event]
                timeout = trans_timeouts.get((str(state), str(target)))
                detail = f"  (timeout={timeout})" if timeout is not None else ""
                lines.append(f"      {event} → {target}{detail}")

        print("\n".join(lines), file=self.file)
