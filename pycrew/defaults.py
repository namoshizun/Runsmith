from collections.abc import Iterable
from types import EllipsisType
from typing import Literal, TextIO

from pycrew.constraints import HeartbeatTimeout, Timeout, TransitionTimeout
from pycrew.state import StateMachine, TransitionTable

DefaultWorkerState = Literal["idle", "starting", "running", "terminating", "crashed", "stopped"]
DefaultWorkerEvent = Literal["start", "run", "terminate", "complete", "error"]


DefaultWorkerTransitionTable: TransitionTable[DefaultWorkerState, DefaultWorkerEvent] = {
    "idle": {"start": "starting"},
    "starting": {"run": "running"},
    "running": {"terminate": "terminating", "error": "crashed"},
    "terminating": {"complete": "stopped", "error": "crashed"},
    "crashed": ...,
    "stopped": ...,
}

DefaultWorkerConstraints: Iterable[Timeout] = [
    HeartbeatTimeout(timeout=2, when="running"),
    TransitionTimeout(timeout=10, when="starting -> running"),
    TransitionTimeout(timeout=3, when="running -> terminating"),
    TransitionTimeout(timeout=10, when="terminating -> stopped"),
]


DefaultWorkerFSM = StateMachine[DefaultWorkerState, DefaultWorkerEvent](
    transitions=DefaultWorkerTransitionTable,
    initial_event="start",
    constraints=DefaultWorkerConstraints,
)


class DefaultFSNPrettyPrinter:
    def __init__(self, fsm: StateMachine, *, file: TextIO):
        self.fsm = fsm
        self.file = file

    def _state_names(self) -> set:
        names: set = set()
        for source, row in self.fsm.get_transitions().items():
            names.add(source)
            if isinstance(row, EllipsisType):
                continue
            names.update(row.values())  # pyright: ignore[reportAttributeAccessIssue]
        return names

    def _states_in_print_order(self) -> list:
        initial = self.fsm.get_initial_state()
        return sorted(self._state_names(), key=lambda s: (s != initial, s))

    def _constraint_print_hints(
        self,
    ) -> tuple[dict[str, float], dict[tuple[str, str], float]]:
        """Per-state keepalives and per-(source,target) transition timeouts."""
        keepalive: dict[str, float] = {}
        transition: dict[tuple[str, str], float] = {}
        for c in self.fsm.get_constraints():
            match c:
                case HeartbeatTimeout():
                    keepalive[c.when] = c.timeout
                case TransitionTimeout():
                    src, _, tgt = c.when.partition("->")
                    transition[(src.strip(), tgt.strip())] = c.timeout
                case _:
                    pass
        return keepalive, transition

    def print(self) -> None:
        """
        Example output:

        → idle  (initial)
              start → starting

          crashed  (terminal)

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
        keepalives, trans_timeouts = self._constraint_print_hints()

        lines: list[str] = []
        for state in self._states_in_print_order():
            if lines:
                lines.append("")

            prefix = "→ " if state == initial else "  "
            tags = (
                (["initial"] if state == initial else [])
                + (["terminal"] if state in self.fsm.get_terminal_states() else [])
                + ([f"keepalive={keepalives[state]}"] if state in keepalives else [])
            )
            suffix = f"  ({', '.join(tags)})" if tags else ""
            lines.append(f"{prefix}{state}{suffix}")

            row = self.fsm.get_transitions().get(state)
            if row is None or isinstance(row, EllipsisType):
                continue
            for event in sorted(row, key=str):
                target = row[event]
                timeout = trans_timeouts.get((str(state), str(target)))
                detail = f"  (timeout={timeout})" if timeout is not None else ""
                lines.append(f"      {event} → {target}{detail}")

        print("\n".join(lines), file=self.file)
