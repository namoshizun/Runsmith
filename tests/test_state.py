from __future__ import annotations

import pytest

from runsmith.constraints import HeartbeatTimeout, StateTimeout, TransitionTimeout
from runsmith.errors import InvalidStateMachineError, InvalidTransitionError
from runsmith.state import StateMachine, Terminal


def _simple_fsm() -> StateMachine:
    return StateMachine(
        transitions={"idle": {"go": "done"}, "done": Terminal.OK},
        initial_event="go",
    )


def test_fsm_identifies_initial_terminal_and_outcomes() -> None:
    fsm = StateMachine(
        transitions={
            "idle": {"go": "done", "fail": "crashed"},
            "done": Terminal.OK,
            "crashed": Terminal.ERROR,
        },
        initial_event="go",
    )
    assert fsm.get_initial_state() == "idle"
    assert fsm.get_initial_event() == "go"
    assert fsm.get_terminal_states() == {"done", "crashed"}
    assert fsm.get_terminal_outcome("done") is Terminal.OK
    assert fsm.get_terminal_outcome("crashed") is Terminal.ERROR
    assert fsm.get_terminal_outcome("idle") is None


def test_fsm_rejects_deprecated_ellipsis_terminal_marker() -> None:
    with pytest.raises(InvalidStateMachineError, match="Terminal.OK or Terminal.ERROR"):
        StateMachine(
            transitions={"idle": {"go": "done"}, "done": ...},
            initial_event="go",
        )


def test_fsm_raises_when_multiple_initial_states() -> None:
    with pytest.raises(InvalidStateMachineError, match="exactly one initial state"):
        StateMachine(
            transitions={"a": {"x": "c"}, "b": {"y": "c"}, "c": Terminal.OK},
            initial_event="x",
        )


def test_fsm_raises_when_no_terminal_states() -> None:
    with pytest.raises(InvalidStateMachineError, match="No terminal states"):
        StateMachine(
            transitions={"idle": {"start": "running"}, "running": {"keep": "running"}},
            initial_event="start",
        )


def test_fsm_raises_on_invalid_initial_event() -> None:
    with pytest.raises(ValueError, match="Invalid initial event"):
        StateMachine(
            transitions={"idle": {"go": "done"}, "done": Terminal.OK},
            initial_event="nonexistent",
        )


def test_get_target_state_and_events() -> None:
    fsm = _simple_fsm()
    assert fsm.get_target_state("idle", "go") == "done"
    assert fsm.get_events("idle") == ["go"]
    assert fsm.get_events("done") == []

    with pytest.raises(InvalidTransitionError):
        fsm.get_target_state("idle", "bogus")
    with pytest.raises(InvalidTransitionError):
        fsm.get_target_state("done", "go")


def test_constraint_validation_rejects_unknown_targets() -> None:
    with pytest.raises(ValueError, match="Heartbeat timeout for unknown state"):
        StateMachine(
            transitions={"idle": {"go": "done"}, "done": Terminal.OK},
            initial_event="go",
            constraints=[HeartbeatTimeout(timeout=1, when="ghost")],
        )

    with pytest.raises(ValueError, match="State timeout for unknown state"):
        StateMachine(
            transitions={"idle": {"go": "done"}, "done": Terminal.OK},
            initial_event="go",
            constraints=[StateTimeout(timeout=1, when="ghost")],
        )

    with pytest.raises(ValueError, match="Invalid transition options"):
        StateMachine(
            transitions={"idle": {"go": "done"}, "done": Terminal.OK},
            initial_event="go",
            constraints=[TransitionTimeout(timeout=1, when="idle -> ghost")],
        )


def test_get_constraints_returns_registered_constraints() -> None:
    c = HeartbeatTimeout(timeout=2, when="idle")
    fsm = StateMachine(
        transitions={"idle": {"go": "done"}, "done": Terminal.OK},
        initial_event="go",
        constraints=[c],
    )
    assert list(fsm.get_constraints()) == [c]
