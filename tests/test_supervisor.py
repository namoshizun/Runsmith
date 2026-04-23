from __future__ import annotations

import asyncio
import multiprocessing
import time
from queue import Queue
from unittest.mock import MagicMock

import pytest

import runsmith.supervisor as supervisor_module
from runsmith.decorators import actor
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.errors import (
    IncompatibleExecutorTypeError,
    IncompatibleWorkerTypeError,
    NoWorkersRegisteredError,
)
from runsmith.evaluator import WorkerStatusEvaluator
from runsmith.execution import ProcessExecutor
from runsmith.settings import RunsmithSettings
from runsmith.supervisor import AsyncSupervisor, SupervisionUnit, SyncSupervisor
from runsmith.worker import AsyncWorker, SyncWorker, WorkerActivity


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


class QuickAsyncWorker(AsyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    async def setup(self):
        return self.emit("run")

    @actor("running")
    async def running(self):
        return self.emit("terminate")

    @actor("terminating")
    async def teardown(self):
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


# ── SupervisionUnit ───────────────────────────────────────────────────────────


def test_supervision_unit_retryable_within_quota() -> None:
    from runsmith.defaults import DefaultWorkerFSM

    unit = SupervisionUnit(
        worker=MagicMock(),
        executor=MagicMock(),
        evaluator=WorkerStatusEvaluator(DefaultWorkerFSM),
        restart_quota=3,
        restart_count=2,
    )
    assert unit.retryable() is True


def test_supervision_unit_not_retryable_at_quota() -> None:
    from runsmith.defaults import DefaultWorkerFSM

    unit = SupervisionUnit(
        worker=MagicMock(),
        executor=MagicMock(),
        evaluator=WorkerStatusEvaluator(DefaultWorkerFSM),
        restart_quota=3,
        restart_count=3,
    )
    assert unit.retryable() is False


# ── SupervisorBase helpers ────────────────────────────────────────────────────


def test_start_executors_raises_when_no_units() -> None:
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(NoWorkersRegisteredError):
        sup.start_executors()


def test_drain_activity_queue_ignores_unknown_worker() -> None:
    sup = SyncSupervisor("s", "thread")
    q: Queue[WorkerActivity] = Queue()
    q.put_nowait(WorkerActivity(worker_name="ghost", kind="heartbeat"))
    sup._activity_queue = q  # pyright: ignore[reportPrivateUsage]
    sup.drain_activity_queue()  # must not raise


def test_kill_executors_kills_alive_executors() -> None:
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    sup.register_workers(QuickSyncWorker("w"))
    sup.materialize_units()

    mock_executor = MagicMock()
    mock_executor.is_alive.return_value = True
    sup.units["w"].executor = mock_executor

    sup.kill_executors()
    mock_executor.kill.assert_called_once()


def test_kill_executors_skips_dead_executors() -> None:
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    sup.register_workers(QuickSyncWorker("w"))
    sup.materialize_units()

    mock_executor = MagicMock()
    mock_executor.is_alive.return_value = False
    sup.units["w"].executor = mock_executor

    sup.kill_executors()
    mock_executor.kill.assert_not_called()


def test_stop_executors_calls_stop_on_all_units() -> None:
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    sup.register_workers(QuickSyncWorker("w"))
    sup.materialize_units()

    mock_executor = MagicMock()
    sup.units["w"].executor = mock_executor

    sup.stop_executors()
    mock_executor.stop.assert_called_once()


def test_materialize_units_creates_process_executor() -> None:
    sup = SyncSupervisor("s", "process")
    sup._activity_queue = multiprocessing.Queue()  # pyright: ignore[reportPrivateUsage]
    sup.register_workers(QuickSyncWorker("w"))
    sup.materialize_units()
    assert isinstance(sup.units["w"].executor, ProcessExecutor)


# ── SyncSupervisor ────────────────────────────────────────────────────────────


def test_sync_supervisor_raises_on_invalid_executor_type() -> None:
    with pytest.raises(IncompatibleExecutorTypeError):
        SyncSupervisor("s", "coroutine")  # type: ignore[arg-type]


def test_sync_supervisor_raises_when_registering_async_worker() -> None:
    sup = SyncSupervisor("s", "thread")
    with pytest.raises(IncompatibleWorkerTypeError):
        sup.register_workers(QuickAsyncWorker("a"))  # type: ignore[arg-type]


def test_sync_supervisor_clone_preserves_workers() -> None:
    sup = SyncSupervisor("root", "thread")
    sup.register_workers(QuickSyncWorker("w1"), QuickSyncWorker("w2"))
    cloned = sup.clone()
    assert cloned.name == "root"
    assert set(cloned._worker_templates) == {"w1", "w2"}  # pyright: ignore[reportPrivateUsage]
    assert cloned is not sup


def test_sync_supervisor_run_raises_on_async_callback() -> None:
    async def async_cb(activity: WorkerActivity) -> None:
        pass

    sup = SyncSupervisor("s", "thread")
    with pytest.raises(TypeError):
        sup.run(async_cb)  # type: ignore[arg-type]


def test_sync_supervisor_boot_emits_run() -> None:
    sup = SyncSupervisor("s", "thread")
    sup.register_workers(QuickSyncWorker("w"))
    result = sup._boot()
    for unit in sup.units.values():
        unit.executor.join(timeout=2.0)  # type: ignore[union-attr]
    assert result == "run"


def test_sync_supervisor_boot_emits_error_when_no_workers() -> None:
    sup = SyncSupervisor("s", "thread")
    result = sup._boot()
    assert result == "error"


def test_sync_supervisor_supervise_emits_terminate_on_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "settings", RunsmithSettings(supervision_interval=0.001))
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    sup.ctx.cmd = "stop"
    assert sup._supervise() == "terminate"


def test_sync_supervisor_supervise_emits_keepalive_with_healthy_units(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "settings", RunsmithSettings(supervision_interval=0.001))
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    assert sup._supervise() == "keepalive"


def test_sync_supervisor_supervise_restarts_dead_retryable_unit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "settings", RunsmithSettings(supervision_interval=0.001))
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    sup.register_workers(QuickSyncWorker("w"))
    sup.materialize_units()

    mock_executor = MagicMock()
    mock_executor.is_alive.return_value = False
    sup.units["w"].executor = mock_executor

    result = sup._supervise()

    assert result == "keepalive"
    assert sup.units["w"].restart_count == 1
    sup.units["w"].executor.join(timeout=2.0)  # type: ignore[union-attr]


def test_sync_supervisor_supervise_terminates_when_restart_quota_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from runsmith.settings import settings as real_settings

    monkeypatch.setattr(supervisor_module, "settings", RunsmithSettings(supervision_interval=0.001))
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    sup.register_workers(QuickSyncWorker("w"))
    sup.materialize_units()

    mock_executor = MagicMock()
    mock_executor.is_alive.return_value = False
    sup.units["w"].executor = mock_executor
    sup.units["w"].restart_count = real_settings.worker_restart_quota

    assert sup._supervise() == "terminate"


def test_sync_supervisor_shutdown_emits_complete_when_no_alive_executors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "settings", RunsmithSettings(supervision_interval=0.001))
    sup = SyncSupervisor("s", "thread")
    sup._activity_queue = Queue()  # pyright: ignore[reportPrivateUsage]
    assert sup._shutdown() == "complete"


def test_sync_supervisor_restart_unit_increments_restart_count() -> None:
    sup = SyncSupervisor("s", "thread")
    sup.register_workers(QuickSyncWorker("w"))
    sup._boot()

    unit = sup.units["w"]
    unit.executor.join(timeout=2.0)  # type: ignore[union-attr]

    new_unit = sup.restart_unit("w")
    new_unit.executor.join(timeout=2.0)  # type: ignore[union-attr]

    assert new_unit.restart_count == 1
    assert new_unit is sup.units["w"]


# ── AsyncSupervisor ───────────────────────────────────────────────────────────


def test_async_supervisor_raises_when_registering_sync_worker() -> None:
    sup = AsyncSupervisor("s")
    with pytest.raises(IncompatibleWorkerTypeError):
        sup.register_workers(QuickSyncWorker("w"))  # type: ignore[arg-type]


def test_async_supervisor_clone_preserves_workers() -> None:
    sup = AsyncSupervisor("root")
    sup.register_workers(QuickAsyncWorker("a1"), QuickAsyncWorker("a2"))
    cloned = sup.clone()
    assert cloned.name == "root"
    assert set(cloned._worker_templates) == {"a1", "a2"}  # pyright: ignore[reportPrivateUsage]
    assert cloned is not sup


@pytest.mark.asyncio
async def test_async_supervisor_run_raises_on_sync_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "EXIT_SIGNALS", tuple())
    sup = AsyncSupervisor("s")
    with pytest.raises(TypeError):
        await sup.run(lambda activity: None)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_async_supervisor_boot_emits_error_when_no_workers() -> None:
    sup = AsyncSupervisor("s")
    assert await sup._boot() == "error"


@pytest.mark.asyncio
async def test_async_supervisor_boot_emits_run() -> None:
    sup = AsyncSupervisor("s")
    sup.register_workers(QuickAsyncWorker("a"))
    result = await sup._boot()
    sup.stop_executors()
    await asyncio.sleep(0.05)
    assert result == "run"


@pytest.mark.asyncio
async def test_async_supervisor_supervise_emits_terminate_on_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "settings", RunsmithSettings(supervision_interval=0.001))
    sup = AsyncSupervisor("s")
    sup._activity_queue = asyncio.Queue()  # pyright: ignore[reportPrivateUsage]
    sup.ctx.cmd = "stop"
    assert await sup._supervise() == "terminate"


@pytest.mark.asyncio
async def test_async_supervisor_supervise_emits_keepalive_with_no_units(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "settings", RunsmithSettings(supervision_interval=0.001))
    sup = AsyncSupervisor("s")
    sup._activity_queue = asyncio.Queue()  # pyright: ignore[reportPrivateUsage]
    assert await sup._supervise() == "keepalive"


@pytest.mark.asyncio
async def test_async_supervisor_shutdown_emits_complete_with_no_alive_executors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(supervisor_module, "settings", RunsmithSettings(supervision_interval=0.001))
    sup = AsyncSupervisor("s")
    sup._activity_queue = asyncio.Queue()  # pyright: ignore[reportPrivateUsage]
    assert await sup._shutdown() == "complete"
