from __future__ import annotations

from runsmith.defaults import DefaultWorkerFSM
from runsmith.evaluator import WorkerStatusEvaluator
from runsmith.worker import WorkerActivity


def test_evaluator_tracks_worker_expectations_across_lifecycle() -> None:
    evaluator = WorkerStatusEvaluator(DefaultWorkerFSM)

    evaluator.record(
        WorkerActivity(
            worker_name="w",
            kind="transition_begin",
            transition=("idle", "start", "starting"),
            timestamp=1.0,
        )
    )
    assert evaluator.is_healthy(1.9)

    evaluator.record(
        WorkerActivity(
            worker_name="w",
            kind="transition_end",
            transition=("idle", "start", "starting"),
            timestamp=1.1,
        )
    )
    assert evaluator.is_healthy(2.0)

    evaluator.record(
        WorkerActivity(
            worker_name="w",
            kind="transition_begin",
            transition=("starting", "run", "running"),
            timestamp=2.0,
        )
    )
    evaluator.record(
        WorkerActivity(
            worker_name="w",
            kind="transition_end",
            transition=("starting", "run", "running"),
            timestamp=2.1,
        )
    )
    evaluator.record(
        WorkerActivity(
            worker_name="w",
            kind="heartbeat",
            timestamp=3.0,
        )
    )

    assert evaluator.is_healthy(4.9)
