from __future__ import annotations

import time
from queue import Queue

import pytest

import runsmith.supervisor as supervisor_module
from runsmith.decorators import actor
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.settings import RunsmithSettings
from runsmith.supervisor import AsyncSupervisor, SyncSupervisor
from runsmith.worker import SyncWorker, WorkerActivity


class QuickSyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    def setup(self):
        return self.emit("run")

    @actor("running")
    def running(self):
        return self.emit("terminate")

    @actor("terminating")
    def teardown(self):
        return self.emit("complete")


def test_sync_supervisor_materializes_units_and_drains_activity() -> None:
    supervisor = SyncSupervisor("root-sync", "thread")
    supervisor.register_workers(QuickSyncWorker("child-sync"))
    supervisor._activity_queue = Queue[WorkerActivity]()  # pyright: ignore[reportPrivateUsage]

    supervisor.materialize_units()
    assert set(supervisor.units) == {"child-sync"}

    supervisor.start_executors()
    unit = supervisor.units["child-sync"]
    assert hasattr(unit.executor, "join")
    unit.executor.join(timeout=1.0)  # pyright: ignore[reportAttributeAccessIssue]

    supervisor.drain_activity_queue()
    assert not unit.executor.is_alive()
    assert unit.evaluator.is_healthy(time.monotonic())
    assert list(unit.worker.ctx.history)[-1] == ("complete", "stopped")


@pytest.mark.asyncio
async def test_async_supervisor_dispatches_callback_for_each_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "EXIT_SIGNALS", tuple())
    monkeypatch.setattr(
        supervisor_module,
        "settings",
        RunsmithSettings(activity_callback_task_queue_size=8),
    )

    async def fake_driver(_execution, _term_event):
        for index in range(3):
            yield WorkerActivity(
                worker_name="child-async",
                kind="heartbeat",
                transition=("s", str(index), "t"),
            )

    monkeypatch.setattr(supervisor_module, "drive_async_worker", fake_driver)

    seen_indices: list[int] = []

    async def on_activity(activity: WorkerActivity) -> None:
        assert activity.transition is not None
        seen_indices.append(int(activity.transition[1]))

    supervisor = AsyncSupervisor("root-async")
    await supervisor.run(on_activity)

    assert seen_indices == [0, 1, 2]
