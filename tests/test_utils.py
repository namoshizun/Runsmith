from __future__ import annotations

import asyncio
import threading
from queue import Queue

import pytest

from runsmith.execution import ThreadExecutor
from runsmith.utils import CoroutineQueue, kill_thread
from runsmith.worker import SyncWorker


def test_kill_thread_raises_for_unknown_thread_id() -> None:
    with pytest.raises(ValueError, match="No active thread"):
        kill_thread(-1)


def test_kill_thread_stops_thread_running_python_code() -> None:
    started = threading.Event()

    def busy_loop() -> None:
        started.set()
        while True:
            pass

    thread = threading.Thread(target=busy_loop)
    thread.start()
    assert started.wait(timeout=1)
    assert thread.ident is not None

    kill_thread(thread.ident)
    thread.join(timeout=1)
    assert not thread.is_alive()


def test_thread_executor_kill_tolerates_already_exited_thread() -> None:
    executor = ThreadExecutor(
        worker=SyncWorker("gone"), term_event=threading.Event(), activity_queue=Queue()
    )
    executor.start()
    executor.join(timeout=2.0)
    assert not executor.is_alive()
    executor.kill()  # must not raise


def test_coroutine_queue_raises_on_nonpositive_max_pending() -> None:
    with pytest.raises(ValueError, match="positive"):
        CoroutineQueue(max_pending=0)


@pytest.mark.asyncio
async def test_coroutine_queue_submit_drain_and_overflow() -> None:
    results: list[int] = []
    barrier = asyncio.Event()

    async def record(n: int) -> None:
        results.append(n)

    async def blocking() -> None:
        await barrier.wait()

    q = CoroutineQueue(max_pending=1)
    q.submit(record(0))
    await q.drain()
    assert results == [0]

    q.submit(blocking())
    overflowed = q.submit(blocking())
    assert overflowed is True

    barrier.set()
    await q.drain()


@pytest.mark.asyncio
async def test_coroutine_queue_flush_errors_logs_failing_tasks() -> None:
    async def failing() -> None:
        raise RuntimeError("oops")

    q = CoroutineQueue(max_pending=2)
    q.submit(failing())
    await asyncio.sleep(0.02)
    q.flush_errors()  # must log without raising
