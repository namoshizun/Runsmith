from __future__ import annotations

import pytest

from runsmith.constraints import HeartbeatTimeout, StateTimeout, TransitionTimeout
from runsmith.errors import InvalidStateMachineError, InvalidTransitionError
from runsmith.state import StateMachine


def _simple_fsm() -> StateMachine:
    return StateMachine(
        transitions={"idle": {"go": "done"}, "done": ...},
        initial_event="go",
    )


def test_fsm_identifies_initial_and_terminal_states() -> None:
    fsm = _simple_fsm()
    assert fsm.get_initial_state() == "idle"
    assert fsm.get_terminal_states() == {"done"}
    assert fsm.get_initial_event() == "go"


def test_fsm_raises_when_multiple_initial_states() -> None:
    # Both "a" and "b" are never targets, so two initial states exist.
    with pytest.raises(InvalidStateMachineError, match="exactly one initial state"):
        StateMachine(
            transitions={"a": {"x": "c"}, "b": {"y": "c"}, "c": ...},
            initial_event="x",
        )


def test_fsm_raises_when_no_terminal_states() -> None:
    # Exactly one initial state ("idle"), but "running" loops back — no `...` entry.
    with pytest.raises(InvalidStateMachineError, match="No terminal states"):
        StateMachine(
            transitions={"idle": {"start": "running"}, "running": {"keep": "running"}},
            initial_event="start",
        )


def test_fsm_raises_on_invalid_initial_event() -> None:
    with pytest.raises(ValueError, match="Invalid initial event"):
        StateMachine(
            transitions={"idle": {"go": "done"}, "done": ...},
            initial_event="nonexistent",
        )


def test_get_target_state_returns_correct_transition() -> None:
    fsm = _simple_fsm()
    assert fsm.get_target_state("idle", "go") == "done"


def test_get_target_state_raises_for_unknown_event() -> None:
    fsm = _simple_fsm()
    with pytest.raises(InvalidTransitionError):
        fsm.get_target_state("idle", "bogus")


def test_get_target_state_raises_on_terminal_state() -> None:
    fsm = _simple_fsm()
    with pytest.raises(InvalidTransitionError):
        fsm.get_target_state("done", "go")


def test_get_events_returns_empty_for_terminal_state() -> None:
    fsm = _simple_fsm()
    assert fsm.get_events("done") == []


def test_get_events_returns_transitions_for_non_terminal_state() -> None:
    fsm = _simple_fsm()
    assert fsm.get_events("idle") == ["go"]


def test_get_constraints_returns_registered_constraints() -> None:
    c = HeartbeatTimeout(timeout=2, when="idle")
    fsm = StateMachine(
        transitions={"idle": {"go": "done"}, "done": ...},
        initial_event="go",
        constraints=[c],
    )
    assert list(fsm.get_constraints()) == [c]


def test_constraint_validation_heartbeat_on_unknown_state() -> None:
    with pytest.raises(ValueError, match="Heartbeat timeout for unknown state"):
        StateMachine(
            transitions={"idle": {"go": "done"}, "done": ...},
            initial_event="go",
            constraints=[HeartbeatTimeout(timeout=1, when="ghost")],
        )


def test_constraint_validation_state_timeout_on_unknown_state() -> None:
    with pytest.raises(ValueError, match="State timeout for unknown state"):
        StateMachine(
            transitions={"idle": {"go": "done"}, "done": ...},
            initial_event="go",
            constraints=[StateTimeout(timeout=1, when="ghost")],
        )


def test_constraint_validation_transition_timeout_invalid_edge() -> None:
    with pytest.raises(ValueError, match="Invalid transition options"):
        StateMachine(
            transitions={"idle": {"go": "done"}, "done": ...},
            initial_event="go",
            constraints=[TransitionTimeout(timeout=1, when="idle -> ghost")],
        )


def test_pretty_printer_lazily_created_and_cached() -> None:
    fsm = _simple_fsm()
    assert fsm._pretty_printer is None  # pyright: ignore[reportPrivateUsage]
    p1 = fsm.pretty_printer
    p2 = fsm.pretty_printer
    assert p1 is p2


def test_pretty_print_outputs_all_states(capsys: pytest.CaptureFixture) -> None:
    fsm = _simple_fsm()
    fsm.pretty_print()
    out = capsys.readouterr().out
    assert "idle" in out
    assert "done" in out
