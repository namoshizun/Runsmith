from __future__ import annotations

import io

from runsmith.defaults import DefaultFSNPrettyPrinter, DefaultWorkerFSM
from runsmith.state import StateMachine


def _print(fsm: StateMachine) -> str:
    buf = io.StringIO()
    DefaultFSNPrettyPrinter(fsm, file=buf).print()
    return buf.getvalue()


def test_output_contains_all_default_states() -> None:
    out = _print(DefaultWorkerFSM)
    for state in ("idle", "starting", "running", "terminating", "stopped", "crashed"):
        assert state in out


def test_output_marks_initial_state_with_arrow() -> None:
    out = _print(DefaultWorkerFSM)
    assert "→ idle" in out


def test_output_labels_initial_state() -> None:
    out = _print(DefaultWorkerFSM)
    assert "initial" in out


def test_output_labels_terminal_states() -> None:
    out = _print(DefaultWorkerFSM)
    assert "terminal" in out


def test_output_shows_keepalive_constraint() -> None:
    out = _print(DefaultWorkerFSM)
    assert "keepalive" in out


def test_output_shows_state_timeout_constraint() -> None:
    out = _print(DefaultWorkerFSM)
    assert "state_timeout" in out


def test_output_shows_transition_timeouts() -> None:
    out = _print(DefaultWorkerFSM)
    assert "timeout" in out


def test_simple_fsm_without_constraints() -> None:
    """Covers branches where no keepalive/state_timeout/transition_timeout hints exist."""
    fsm = StateMachine(
        transitions={"idle": {"go": "done"}, "done": ...},
        initial_event="go",
    )
    out = _print(fsm)
    assert "→ idle" in out
    assert "done" in out
    assert "terminal" in out
    assert "initial" in out


def test_output_lists_transitions_under_each_state() -> None:
    out = _print(DefaultWorkerFSM)
    assert "start → starting" in out
