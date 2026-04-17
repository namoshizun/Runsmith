import copy
import dataclasses
import math

from pycrew.constraints import HeartbeatTimeout, StateTimeout, TransitionTimeout
from pycrew.state import StateMachine
from pycrew.worker import WorkerActivity


@dataclasses.dataclass(slots=True)
class Expectation:
    state: str
    transition_deadline: float = math.inf
    next_heartbeat: float = math.inf
    state_expiry: float = math.inf

    def reset_heartbeat(self):
        self.next_heartbeat = math.inf

    def reset_transition(self):
        self.transition_deadline = math.inf

    def reset_state_expiry(self):
        self.state_expiry = math.inf


@dataclasses.dataclass(slots=True)
class WorkerConstraints:
    heartbeat_timeouts: dict[str, float]  # state => timeout
    transition_timeouts: dict[tuple[str, str], float]  # (src, tgt) => timeout
    state_timeouts: dict[str, float]  # state => max residence time
    terminal_states: frozenset[str]

    @classmethod
    def from_fsm(cls, fsm: StateMachine) -> "WorkerConstraints":
        heartbeat_timeouts: dict[str, float] = {}
        transition_timeouts: dict[tuple[str, str], float] = {}
        state_timeouts: dict[str, float] = {}

        for c in fsm.get_constraints():
            match c:
                case HeartbeatTimeout():
                    heartbeat_timeouts[c.when] = c.timeout
                case TransitionTimeout():
                    src, tgt = c.get_src_and_tgt()
                    transition_timeouts[(src, tgt)] = c.timeout
                case StateTimeout():
                    state_timeouts[c.when] = c.timeout

        return cls(
            heartbeat_timeouts=heartbeat_timeouts,
            transition_timeouts=transition_timeouts,
            state_timeouts=state_timeouts,
            terminal_states=frozenset(map(str, fsm.get_terminal_states())),
        )


class WorkerStatusEvaluator:
    """Tracks a single worker's liveness by recording activities and detecting timeout violations."""

    def __init__(self, fsm: StateMachine):
        self._expectation = Expectation(state=fsm.get_initial_state())
        self._constraints = WorkerConstraints.from_fsm(copy.deepcopy(fsm))

    def record(self, activity: WorkerActivity):
        exp = self._expectation
        constraints = self._constraints
        ts = activity.timestamp

        match activity.kind:
            case "transition_begin":
                assert activity.transition is not None
                src, _, tgt = activity.transition
                timeout = constraints.transition_timeouts.get((src, tgt))

                exp.transition_deadline = ts + timeout if timeout else math.inf
                exp.reset_heartbeat()
                exp.reset_state_expiry()

            case "transition_end":
                assert activity.transition is not None
                _, _, tgt = activity.transition
                exp.state = tgt
                exp.reset_transition()
                exp.reset_heartbeat()
                exp.reset_state_expiry()

                if tgt not in constraints.terminal_states:
                    if timeout := constraints.heartbeat_timeouts.get(tgt):
                        # The new state expects periodic heartbeats
                        exp.next_heartbeat = ts + timeout

                    if timeout := constraints.state_timeouts.get(tgt):
                        # The new state has a residence timeout
                        exp.state_expiry = ts + timeout

            case "heartbeat":
                # Update the next heartbeat's expected arrival time
                timeout = constraints.heartbeat_timeouts.get(exp.state)
                exp.next_heartbeat = ts + timeout if timeout else math.inf

    def is_healthy(self, now: float) -> bool:
        exp = self._expectation
        return (
            now <= exp.transition_deadline and now <= exp.next_heartbeat and now <= exp.state_expiry
        )
