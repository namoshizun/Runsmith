from __future__ import annotations

import asyncio
import threading
from queue import Queue

import pytest

from runsmith.decorators import actor
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.execution import CoroutineExecutor, ThreadExecutor
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


class StuckSyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    def setup(self):
        return self.emit("run")

    @actor("running")
    def running(self):
        while True:
            pass


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


def test_thread_executor_runs_worker_and_emits_activities() -> None:
    worker = QuickSyncWorker("thread-worker")
    activity_queue: Queue[WorkerActivity] = Queue()
    executor = ThreadExecutor(
        worker=worker,
        term_event=threading.Event(),
        activity_queue=activity_queue,
    )

    executor.start()
    executor.join(timeout=1.0)

    assert not executor.is_alive()
    assert not activity_queue.empty()
    first_activity = activity_queue.get_nowait()
    assert first_activity.worker_name == "thread-worker"


def test_thread_executor_kill_stops_thread_running_python_code() -> None:
    worker = StuckSyncWorker("stuck-thread-worker")
    activity_queue: Queue[WorkerActivity] = Queue()
    executor = ThreadExecutor(
        worker=worker,
        term_event=threading.Event(),
        activity_queue=activity_queue,
    )

    executor.start()
    assert activity_queue.get(timeout=1).worker_name == "stuck-thread-worker"

    executor.kill()
    executor.join(timeout=1)

    assert not executor.is_alive()


@pytest.mark.asyncio
async def test_coroutine_executor_runs_and_stops_cleanly() -> None:
    worker = QuickAsyncWorker("coroutine-worker")
    activity_queue: asyncio.Queue[WorkerActivity] = asyncio.Queue()
    executor = CoroutineExecutor(
        worker=worker,
        term_event=asyncio.Event(),
        activity_queue=activity_queue,
    )

    executor.start()
    first_activity = await asyncio.wait_for(activity_queue.get(), timeout=1.0)
    assert first_activity.worker_name == "coroutine-worker"

    executor.stop()
    for _ in range(50):
        if not executor.is_alive():
            break
        await asyncio.sleep(0.01)

    assert not executor.is_alive()
