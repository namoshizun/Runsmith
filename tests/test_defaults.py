from __future__ import annotations

import io

from runsmith.defaults import DefaultFSNPrettyPrinter, DefaultWorkerFSM, SupervisorFSM
from runsmith.state import Terminal


def test_default_worker_fsm_structure() -> None:
    assert DefaultWorkerFSM.get_initial_state() == "idle"
    assert DefaultWorkerFSM.get_terminal_outcome("stopped") is Terminal.OK
    assert DefaultWorkerFSM.get_terminal_outcome("crashed") is Terminal.ERROR
    assert DefaultWorkerFSM.get_target_state("running", "error") == "crashed"


def test_supervisor_fsm_reaps_before_crash() -> None:
    """Failing supervisors must drain their subtree before landing in crashed."""
    assert SupervisorFSM.get_target_state("running", "error") == "reaping"
    assert SupervisorFSM.get_target_state("reaping", "complete") == "crashed"
    assert SupervisorFSM.get_target_state("reaping", "error") == "crashed"


def test_pretty_printer_renders_states_and_constraints() -> None:
    buf = io.StringIO()
    DefaultFSNPrettyPrinter(DefaultWorkerFSM, file=buf).print()
    out = buf.getvalue()

    assert "→ idle" in out
    assert "initial" in out
    assert "terminal" in out
    assert "crashed" in out and "error" in out
    assert "keepalive" in out
    assert "start → starting" in out
