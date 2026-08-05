from __future__ import annotations

from runsmith.defaults import DefaultWorkerFSM
from runsmith.evaluator import WorkerStatusEvaluator
from runsmith.state import Terminal
from runsmith.worker import WorkerActivity


def _begin(src: str, event: str, tgt: str, ts: float) -> WorkerActivity:
    return WorkerActivity(
        worker_name="w",
        kind="transition_begin",
        transition=(src, event, tgt),
        timestamp=ts,
    )


def _end(src: str, event: str, tgt: str, ts: float) -> WorkerActivity:
    return WorkerActivity(
        worker_name="w",
        kind="transition_end",
        transition=(src, event, tgt),
        timestamp=ts,
    )


def _heartbeat(ts: float) -> WorkerActivity:
    return WorkerActivity(worker_name="w", kind="heartbeat", timestamp=ts)


def test_evaluator_tracks_worker_expectations_across_lifecycle() -> None:
    evaluator = WorkerStatusEvaluator(DefaultWorkerFSM)

    evaluator.record(_begin("idle", "start", "starting", 1.0))
    assert evaluator.is_healthy(1.9)

    evaluator.record(_end("idle", "start", "starting", 1.1))
    assert evaluator.is_healthy(2.0)

    evaluator.record(_begin("starting", "run", "running", 2.0))
    evaluator.record(_end("starting", "run", "running", 2.1))
    evaluator.record(_heartbeat(3.0))

    assert evaluator.is_healthy(4.9)
    assert evaluator.terminal_outcome() is None


def test_evaluator_reports_terminal_outcomes() -> None:
    ok = WorkerStatusEvaluator(DefaultWorkerFSM)
    ok.record(_end("terminating", "complete", "stopped", 1.0))
    assert ok.terminal_outcome() is Terminal.OK

    err = WorkerStatusEvaluator(DefaultWorkerFSM)
    err.record(_end("running", "error", "crashed", 1.0))
    assert err.terminal_outcome() is Terminal.ERROR


def test_evaluator_unhealthy_when_transition_exceeds_deadline() -> None:
    """DefaultWorkerFSM: idle -> starting has TransitionTimeout(1)."""
    evaluator = WorkerStatusEvaluator(DefaultWorkerFSM)
    evaluator.record(_begin("idle", "start", "starting", 0.0))

    assert evaluator.is_healthy(0.9)
    assert not evaluator.is_healthy(1.1)


def test_evaluator_unhealthy_when_heartbeat_missed() -> None:
    """DefaultWorkerFSM: running has HeartbeatTimeout(2)."""
    evaluator = WorkerStatusEvaluator(DefaultWorkerFSM)
    evaluator.record(_end("starting", "run", "running", 0.0))

    assert evaluator.is_healthy(1.9)
    assert not evaluator.is_healthy(2.1)


def test_evaluator_heartbeat_extends_deadline() -> None:
    evaluator = WorkerStatusEvaluator(DefaultWorkerFSM)
    evaluator.record(_end("starting", "run", "running", 0.0))
    evaluator.record(_heartbeat(1.5))

    assert evaluator.is_healthy(3.4)
    assert not evaluator.is_healthy(3.6)


def test_evaluator_unhealthy_when_state_residence_expires() -> None:
    """DefaultWorkerFSM: starting has StateTimeout(10)."""
    evaluator = WorkerStatusEvaluator(DefaultWorkerFSM)
    evaluator.record(_end("idle", "start", "starting", 0.0))

    assert evaluator.is_healthy(9.9)
    assert not evaluator.is_healthy(10.1)
