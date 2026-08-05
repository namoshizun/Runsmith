from __future__ import annotations

import threading
import time
from queue import Queue
from unittest.mock import MagicMock

import pytest

import runsmith.supervisor as supervisor_module
from runsmith.decorators import actor
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerFSM, DefaultWorkerState
from runsmith.errors import (
    IncompatibleExecutorTypeError,
    IncompatibleWorkerTypeError,
    NoWorkersRegisteredError,
)
from runsmith.evaluator import WorkerStatusEvaluator
from runsmith.execution import ProcessExecutor, drive_sync_worker
from runsmith.settings import RunsmithSettings
from runsmith.supervisor import AsyncSupervisor, SupervisionUnit, SyncSupervisor, UnitStatus
from runsmith.worker import AsyncWorker, SyncWorker, WorkerActivity


class QuickSyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    def setup(self):
        return self.emit("run")

    @actor("running")
    def running(self):
        return self.emit("complete")

    @actor("terminating")
    def teardown(self):
        return self.emit("complete")


class QuickAsyncWorker(AsyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    async def setup(self):
        return self.emit("run")

    @actor("running")
    async def running(self):
        return self.emit("complete")

    @actor("terminating")
    async def teardown(self):
        return self.emit("complete")


class OneShotSyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    def clone(self) -> "OneShotSyncWorker":
        return OneShotSyncWorker(self.name)

    @actor("starting")
    def setup(self):
        return self.emit("run")

    @actor("running")
    def run_once(self):
        return self.emit("complete")

    @actor("terminating")
    def teardown(self):
        return self.emit("complete")


class CrashingSyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    def setup(self):
        return self.emit("error")


class OneShotAsyncWorker(AsyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    def clone(self) -> "OneShotAsyncWorker":
        return OneShotAsyncWorker(self.name)

    @actor("starting")
    async def setup(self):
        return self.emit("run")

    @actor("running")
    async def run_once(self):
        return self.emit("complete")

    @actor("terminating")
    async def teardown(self):
        return self.emit("complete")


def _poison_evaluator(unit: SupervisionUnit) -> None:
    """Make the unit's evaluator report unhealthy relative to wall-clock now."""
    unit.evaluator.record(
        WorkerActivity(
            worker_name=unit.worker.name,
            kind="transition_begin",
            transition=("idle", "start", "starting"),
            timestamp=time.monotonic() - 100,
        )
    )


# ── Unit status ───────────────────────────────────────────────────────────────


def test_supervision_unit_status_classification() -> None:
    alive = MagicMock()
    alive.is_alive.return_value = True
    dead = MagicMock()
    dead.is_alive.return_value = False

    healthy = SupervisionUnit(
        worker=MagicMock(),
        executor=alive,
        evaluator=WorkerStatusEvaluator(DefaultWorkerFSM),
        restart_quota=1,
    )
    assert healthy.status(time.monotonic()) is UnitStatus.HEALTHY
    assert healthy.retryable() is True

    lingering = SupervisionUnit(
        worker=MagicMock(name="w"),
        executor=alive,
        evaluator=WorkerStatusEvaluator(DefaultWorkerFSM),
        restart_quota=1,
        restart_count=1,
    )
    lingering.worker.name = "w"
    _poison_evaluator(lingering)
    assert lingering.status(time.monotonic()) is UnitStatus.LINGERING
    assert lingering.retryable() is False

    completed = SupervisionUnit(
        worker=MagicMock(),
        executor=dead,
        evaluator=WorkerStatusEvaluator(DefaultWorkerFSM),
        restart_quota=1,
    )
    completed.evaluator.record(
        WorkerActivity(
            worker_name="w",
            kind="transition_end",
            transition=("terminating", "complete", "stopped"),
            timestamp=time.monotonic(),
        )
    )
    assert completed.status(time.monotonic()) is UnitStatus.COMPLETED

    failed = SupervisionUnit(
        worker=MagicMock(),
        executor=dead,
        evaluator=WorkerStatusEvaluator(DefaultWorkerFSM),
        restart_quota=1,
    )
    assert failed.status(time.monotonic()) is UnitStatus.FAILED


# ── Shared supervisor plumbing ────────────────────────────────────────────────


def test_start_executors_raises_when_no_units() -> None:
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(NoWorkersRegisteredError):
        sup.start_executors()


def test_drain_activity_queue_is_bounded_and_ignores_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        supervisor_module,
        "settings",
        RunsmithSettings(activity_queue_maxsize=2),
    )
    sup = SyncSupervisor("s", "thread")
    q: Queue[WorkerActivity] = Queue()
    for _ in range(5):
        q.put_nowait(WorkerActivity(worker_name="ghost", kind="heartbeat"))
    sup._activity_queue = q  # pyright: ignore[reportPrivateUsage]

    sup.drain_activity_queue()
    assert q.qsize() == 3
    sup.drain_activity_queue()
    assert q.qsize() == 1


def test_drain_drops_stale_generation_after_restart() -> None:
    """Activities from a killed incarnation must not poison the replacement unit."""
    sup = SyncSupervisor("s", "thread")
    q: Queue[WorkerActivity] = Queue()
    sup._activity_queue = q  # pyright: ignore[reportPrivateUsage]
    sup.register_workers(QuickSyncWorker("w"))
    sup.materialize_units()

    old_generation = sup.units["w"].worker.generation
    unit = sup.restart_unit("w")
    unit.executor.join(timeout=2.0)  # type: ignore[union-attr]

    while not q.empty():
        q.get_nowait()

    q.put_nowait(
        WorkerActivity(
            worker_name="w",
            generation=old_generation,
            kind="transition_end",
            transition=("terminating", "complete", "stopped"),
        )
    )
    sup.drain_activity_queue()

    assert unit.worker.generation == old_generation + 1
    assert unit.evaluator.terminal_outcome() is None
    assert unit.status(time.monotonic()) is not UnitStatus.COMPLETED


def test_sync_supervisor_materializes_units_and_drains_activity() -> None:
    supervisor = SyncSupervisor("root-sync", "thread")
    supervisor.register_workers(QuickSyncWorker("child-sync"))
    supervisor._activity_queue = Queue[WorkerActivity]()  # pyright: ignore[reportPrivateUsage]

    supervisor.materialize_units()
    assert set(supervisor.units) == {"child-sync"}

    supervisor.start_executors()
    unit = supervisor.units["child-sync"]
    unit.executor.join(timeout=1.0)  # type: ignore[union-attr]

    supervisor.drain_activity_queue()
    assert not unit.executor.is_alive()
    assert unit.evaluator.is_healthy(time.monotonic())
    assert list(unit.worker.ctx.history)[-1] == ("complete", "stopped")


# ── API contracts ─────────────────────────────────────────────────────────────


def test_sync_supervisor_rejects_incompatible_types() -> None:
    with pytest.raises(IncompatibleExecutorTypeError):
        SyncSupervisor("s", "coroutine")  # type: ignore[arg-type]

    with pytest.raises(IncompatibleWorkerTypeError):
        SyncSupervisor("s", "thread").register_workers(QuickAsyncWorker("a"))  # type: ignore[arg-type]

    with pytest.raises(IncompatibleWorkerTypeError):
        AsyncSupervisor("s").register_workers(QuickSyncWorker("w"))  # type: ignore[arg-type]


def test_sync_supervisor_run_raises_on_async_callback() -> None:
    async def async_cb(activity: WorkerActivity) -> None:
        pass

    with pytest.raises(TypeError):
        SyncSupervisor("s", "thread").run(async_cb)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_async_supervisor_run_raises_on_sync_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "EXIT_SIGNALS", tuple())
    with pytest.raises(TypeError):
        await AsyncSupervisor("s").run(lambda activity: None)  # type: ignore[arg-type]


# ── Supervision decisions ─────────────────────────────────────────────────────


def test_supervise_restarts_dead_retryable_and_lingering_units(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "settings", RunsmithSettings(supervision_interval=0.001))
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    sup.register_workers(QuickSyncWorker("dead"), QuickSyncWorker("linger"))
    sup.materialize_units()

    dead = sup.units["dead"]
    dead.executor = MagicMock()
    dead.executor.is_alive.return_value = False

    linger = sup.units["linger"]
    linger.executor = MagicMock()
    linger.executor.is_alive.return_value = True
    _poison_evaluator(linger)

    assert sup._supervise() == "keepalive"
    assert sup.units["dead"].restart_count == 1
    assert sup.units["linger"].restart_count == 1
    for name in ("dead", "linger"):
        sup.units[name].executor.join(timeout=2.0)  # type: ignore[union-attr]


def test_supervise_retires_completed_and_escalates_when_quota_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from runsmith.settings import settings as real_settings

    monkeypatch.setattr(supervisor_module, "settings", RunsmithSettings(supervision_interval=0.001))
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    sup.register_workers(OneShotSyncWorker("done"), CrashingSyncWorker("crashy"))
    sup.materialize_units()

    done = sup.units["done"]
    done.executor = MagicMock()
    done.executor.is_alive.return_value = False
    done.evaluator.record(
        WorkerActivity(
            worker_name="done",
            kind="transition_end",
            transition=("terminating", "complete", "stopped"),
            timestamp=time.monotonic(),
        )
    )

    crashy = sup.units["crashy"]
    crashy.executor = MagicMock()
    crashy.executor.is_alive.return_value = False
    crashy.restart_count = real_settings.worker_restart_quota
    crashy.evaluator.record(
        WorkerActivity(
            worker_name="crashy",
            kind="transition_end",
            transition=("starting", "error", "crashed"),
            timestamp=time.monotonic(),
        )
    )

    assert sup._supervise() == "error"
    assert done.retired
    assert done.restart_count == 0
    assert sup.fsm.get_target_state("running", "error") == "reaping"


def test_shutdown_stops_alive_units_and_kills_lingering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "settings", RunsmithSettings(supervision_interval=0.001))
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    assert sup._shutdown() == "complete"

    sup.register_workers(QuickSyncWorker("healthy"), QuickSyncWorker("linger"))
    sup.materialize_units()

    healthy = MagicMock()
    healthy.is_alive.return_value = True
    linger = MagicMock()
    linger.is_alive.return_value = True
    sup.units["healthy"].executor = healthy
    sup.units["linger"].executor = linger
    _poison_evaluator(sup.units["linger"])

    assert sup._shutdown() == "keepalive"
    healthy.stop.assert_called_once()
    linger.stop.assert_called_once()
    linger.kill.assert_called_once()
    healthy.kill.assert_not_called()


def test_restart_unit_increments_generation_and_restart_count() -> None:
    sup = SyncSupervisor("s", "thread")
    sup.register_workers(QuickSyncWorker("w"))
    sup._boot()

    unit = sup.units["w"]
    assert unit.worker.generation == 0
    unit.executor.join(timeout=2.0)  # type: ignore[union-attr]

    new_unit = sup.restart_unit("w")
    new_unit.executor.join(timeout=2.0)  # type: ignore[union-attr]

    assert new_unit.restart_count == 1
    assert new_unit.worker.generation == 1
    assert new_unit is sup.units["w"]


# ── End-to-end lifecycle ──────────────────────────────────────────────────────


def test_sync_supervisor_run_returns_after_one_shot_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "EXIT_SIGNALS", tuple())
    monkeypatch.setattr(
        supervisor_module,
        "settings",
        RunsmithSettings(supervision_interval=0.01, worker_restart_quota=1),
    )

    supervisor = SyncSupervisor("root", "thread")
    supervisor.register_workers(OneShotSyncWorker("one-shot"))
    supervisor.run()

    assert list(supervisor.units["one-shot"].worker.ctx.history)[-1] == ("complete", "stopped")
    assert supervisor.units["one-shot"].retired
    assert supervisor.units["one-shot"].restart_count == 0


def test_sync_supervisor_reaches_crashed_after_reaping_subtree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "EXIT_SIGNALS", tuple())
    monkeypatch.setattr(
        supervisor_module,
        "settings",
        RunsmithSettings(supervision_interval=0.01, worker_restart_quota=1),
    )

    supervisor = SyncSupervisor("root", "thread")
    supervisor.register_workers(CrashingSyncWorker("crashy"))
    supervisor.run()

    assert list(supervisor.ctx.history)[-2:] == [
        ("error", "reaping"),
        ("complete", "crashed"),
    ]
    assert supervisor.all_executors_down


def test_nested_parent_restarts_a_crashed_child_supervisor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "EXIT_SIGNALS", tuple())
    monkeypatch.setattr(
        supervisor_module,
        "settings",
        RunsmithSettings(
            supervision_interval=0.01, worker_restart_quota=1, supervisor_restart_quota=1
        ),
    )

    child = SyncSupervisor("child", "thread")
    child.register_workers(CrashingSyncWorker("crashy"))
    root = SyncSupervisor("root", "thread")
    root.register_workers(child)
    root.run()

    assert root.units["child"].restart_count == 1
    assert root.all_executors_down


def test_plain_worker_reports_failure_without_reaping() -> None:
    """Reaping is a supervisor concern; a leaf worker has no subtree to reap."""

    class FailingSyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
        @actor("starting")
        def setup(self):
            return self.emit("run")

        @actor("running")
        def work(self):
            return self.emit("error")

    worker = FailingSyncWorker("plain")
    for _ in drive_sync_worker(worker.main_loop(), threading.Event()):
        pass

    assert list(worker.ctx.history)[-1] == ("error", "crashed")
    assert "reaping" not in worker.fsm.get_transitions()


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

    await AsyncSupervisor("root-async").run(on_activity)
    assert seen_indices == [0, 1, 2]


@pytest.mark.asyncio
async def test_async_supervisor_run_returns_after_one_shot_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "EXIT_SIGNALS", tuple())
    monkeypatch.setattr(
        supervisor_module,
        "settings",
        RunsmithSettings(supervision_interval=0.05, worker_restart_quota=1),
    )

    supervisor = AsyncSupervisor("root")
    supervisor.register_workers(OneShotAsyncWorker("one-shot"))
    await supervisor.run()

    assert list(supervisor.units["one-shot"].worker.ctx.history)[-1] == ("complete", "stopped")
    assert supervisor.units["one-shot"].retired
    assert supervisor.units["one-shot"].restart_count == 0


def test_process_executor_path_materializes_and_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Smoke the process executor branch — most coverage uses threads."""
    monkeypatch.setattr(supervisor_module, "EXIT_SIGNALS", tuple())
    monkeypatch.setattr(
        supervisor_module,
        "settings",
        RunsmithSettings(supervision_interval=0.01, worker_restart_quota=1),
    )

    supervisor = SyncSupervisor("root", "process")
    supervisor.register_workers(OneShotSyncWorker("one-shot"))
    supervisor.run()

    assert isinstance(supervisor.units["one-shot"].executor, ProcessExecutor)
    assert supervisor.units["one-shot"].retired
