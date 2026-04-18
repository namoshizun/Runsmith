from __future__ import annotations

from runsmith.decorators import actor
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.worker import SyncWorker, WorkerActivity


class DemoSyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    def setup(self) -> DefaultWorkerEvent:
        return self.emit("run")

    @actor("running")
    def running(self) -> DefaultWorkerEvent | str:
        cycles = int(self.ctx.data or 0)
        self.ctx.set_data(cycles + 1)
        if cycles == 0:
            return self.emit("keepalive")
        return self.emit("terminate")

    @actor("terminating")
    def teardown(self) -> DefaultWorkerEvent:
        return self.emit("complete")


def _collect_activities(worker: DemoSyncWorker) -> list[WorkerActivity]:
    activities: list[WorkerActivity] = []
    execution = worker.main_loop()
    activities.append(next(execution))

    while True:
        try:
            activities.append(execution.send("tick"))
        except StopIteration:
            break

    return activities


def test_sync_worker_main_loop_transitions_to_terminal_state() -> None:
    worker = DemoSyncWorker("demo-worker")
    activities = _collect_activities(worker)

    transition_begins = [a for a in activities if a.kind == "transition_begin"]
    transition_ends = [a for a in activities if a.kind == "transition_end"]
    heartbeats = [a for a in activities if a.kind == "heartbeat"]

    assert len(transition_begins) == 4
    assert len(transition_ends) == 4
    assert len(heartbeats) >= 2
    assert list(worker.ctx.history) == [
        ("start", "starting"),
        ("run", "running"),
        ("terminate", "terminating"),
        ("complete", "stopped"),
    ]
