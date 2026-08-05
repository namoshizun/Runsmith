from __future__ import annotations

import asyncio
import multiprocessing
import threading
import time
from queue import Queue

import pytest

from runsmith.decorators import actor
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.execution import CoroutineExecutor, ProcessExecutor, ThreadExecutor
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


class StuckSyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    def setup(self):
        return self.emit("run")

    @actor("running")
    def running(self):
        while True:
            pass


class ChattySyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    def setup(self):
        return self.emit("run")

    @actor("running")
    def running(self):
        if self.ctx.cmd == "stop":
            return self.emit("complete")
        return self.emit("keepalive")

    @actor("terminating")
    def teardown(self):
        return self.emit("complete")


class ChattyAsyncWorker(AsyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    async def setup(self):
        return self.emit("run")

    @actor("running")
    async def running(self):
        if self.ctx.cmd == "stop":
            return self.emit("complete")
        return self.emit("keepalive")

    @actor("terminating")
    async def teardown(self):
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


class CrashAsyncWorker(AsyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    async def setup(self):
        return self.emit("run")

    @actor("running")
    async def running(self):
        raise RuntimeError("boom")


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


def test_thread_executor_blocks_on_full_queue_without_crashing() -> None:
    worker = ChattySyncWorker("chatty-thread")
    activity_queue: Queue[WorkerActivity] = Queue(maxsize=1)
    executor = ThreadExecutor(
        worker=worker,
        term_event=threading.Event(),
        activity_queue=activity_queue,
    )

    executor.start()
    # Let the producer fill the queue and block on put().
    time.sleep(0.1)
    assert executor.is_alive()
    assert activity_queue.full()

    # Drain one slot — the blocked put must resume and refill without dying.
    first = activity_queue.get(timeout=1)
    assert first.worker_name == "chatty-thread"
    second = activity_queue.get(timeout=1)
    assert second.worker_name == "chatty-thread"
    assert executor.is_alive()

    executor.kill()
    executor.join(timeout=1)
    assert not executor.is_alive()


def test_process_executor_blocks_on_full_queue_without_crashing() -> None:
    worker = ChattySyncWorker("chatty-process")
    activity_queue = multiprocessing.Queue(maxsize=1)
    executor = ProcessExecutor(
        worker=worker,
        term_event=multiprocessing.Event(),
        activity_queue=activity_queue,
    )

    executor.start()
    time.sleep(0.2)
    assert executor.is_alive()

    first = activity_queue.get(timeout=1)
    assert first.worker_name == "chatty-process"
    second = activity_queue.get(timeout=1)
    assert second.worker_name == "chatty-process"
    assert executor.is_alive()

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


@pytest.mark.asyncio
async def test_coroutine_executor_awaits_on_full_queue_without_crashing() -> None:
    worker = ChattyAsyncWorker("chatty-coro")
    activity_queue: asyncio.Queue[WorkerActivity] = asyncio.Queue(maxsize=1)
    executor = CoroutineExecutor(
        worker=worker,
        term_event=asyncio.Event(),
        activity_queue=activity_queue,
    )

    executor.start()
    await asyncio.sleep(0.05)
    assert executor.is_alive()
    assert activity_queue.full()

    first = await asyncio.wait_for(activity_queue.get(), timeout=1.0)
    assert first.worker_name == "chatty-coro"
    second = await asyncio.wait_for(activity_queue.get(), timeout=1.0)
    assert second.worker_name == "chatty-coro"
    assert executor.is_alive()

    executor.kill()
    for _ in range(50):
        if not executor.is_alive():
            break
        await asyncio.sleep(0.01)
    assert not executor.is_alive()


@pytest.mark.asyncio
async def test_coroutine_executor_retrieves_crash_exception() -> None:
    """A crashed worker must not leak an un-retrieved asyncio task exception.

    asyncio reports un-retrieved task exceptions ("Task exception was never
    retrieved") at garbage-collection time, which makes the leak timing-
    dependent. The executor's done-callback contract is deterministic: the
    exception is consumed as soon as the task completes, so the warning can
    never fire.
    """
    worker = CrashAsyncWorker("crash-coro")
    activity_queue: asyncio.Queue[WorkerActivity] = asyncio.Queue()
    executor = CoroutineExecutor(
        worker=worker,
        term_event=asyncio.Event(),
        activity_queue=activity_queue,
    )

    executor.start()
    for _ in range(50):
        if not executor.is_alive():
            break
        await asyncio.sleep(0.01)

    assert not executor.is_alive()
    assert worker.ctx.exception is not None

    task = executor._CoroutineExecutor__task  # pyright: ignore[reportPrivateUsage]
    assert task._log_traceback is False
    assert task.exception() is not None
