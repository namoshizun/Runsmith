from __future__ import annotations

import asyncio
import threading
import time

import pytest

from runsmith.utils import CoroutineQueue, Timer, kill_thread

# ── Timer ────────────────────────────────────────────────────────────────────


def test_timer_measures_duration_in_milliseconds() -> None:
    with Timer() as t:
        time.sleep(0.01)
    assert t.duration >= 8


def test_timer_measures_duration_in_seconds() -> None:
    with Timer(unit="s") as t:
        time.sleep(0.01)
    assert t.duration >= 0.008


def test_timer_rounds_duration() -> None:
    with Timer(rounded=True) as t:
        time.sleep(0.01)
    assert isinstance(t.duration, int)


def test_timer_elapsed_returns_nonnegative_during_context() -> None:
    with Timer() as t:
        assert t.elapsed() >= 0


def test_timer_warning_fires_above_threshold() -> None:
    # Should not raise even when the warning threshold is exceeded.
    with Timer(warning_thresh=0, warning_message="slow: {duration}"):
        pass


def test_timer_warning_with_unformattable_message() -> None:
    # If the message format fails loguru still logs (no {duration} placeholder).
    with Timer(warning_thresh=0, warning_message="no placeholder here"):
        pass


# ── kill_thread ────────────────────────────────────────────────────────────────


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


# ── CoroutineQueue ────────────────────────────────────────────────────────────


def test_coroutine_queue_raises_on_nonpositive_max_pending() -> None:
    with pytest.raises(ValueError, match="positive"):
        CoroutineQueue(max_pending=0)


@pytest.mark.asyncio
async def test_coroutine_queue_submit_and_drain() -> None:
    results: list[int] = []

    async def task(n: int) -> None:
        results.append(n)

    q = CoroutineQueue(max_pending=4)
    for i in range(3):
        q.submit(task(i))

    await q.drain()
    assert sorted(results) == [0, 1, 2]


@pytest.mark.asyncio
async def test_coroutine_queue_returns_false_when_not_overflowed() -> None:
    q = CoroutineQueue(max_pending=4)

    async def noop() -> None:
        pass

    overflowed = q.submit(noop())
    assert overflowed is False
    await q.drain()


@pytest.mark.asyncio
async def test_coroutine_queue_drops_oldest_on_overflow() -> None:
    barrier = asyncio.Event()

    async def blocking() -> None:
        await barrier.wait()

    q = CoroutineQueue(max_pending=1)
    q.submit(blocking())  # fills the queue
    overflowed = q.submit(blocking())  # should overflow and cancel the first

    assert overflowed is True
    barrier.set()
    await q.drain()


@pytest.mark.asyncio
async def test_coroutine_queue_flush_errors_logs_failing_tasks() -> None:
    async def failing() -> None:
        raise RuntimeError("oops")

    q = CoroutineQueue(max_pending=2)
    q.submit(failing())
    await asyncio.sleep(0.02)  # let task complete and fail

    # flush_errors should log the error without raising
    q.flush_errors()


@pytest.mark.asyncio
async def test_coroutine_queue_drain_on_empty_queue() -> None:
    q = CoroutineQueue(max_pending=2)
    await q.drain()  # must not raise


@pytest.mark.asyncio
async def test_coroutine_queue_cancelled_task_not_treated_as_error() -> None:
    barrier = asyncio.Event()

    async def blocking() -> None:
        await barrier.wait()

    q = CoroutineQueue(max_pending=1)
    q.submit(blocking())  # fills queue
    q.submit(blocking())  # overflows → first task cancelled

    barrier.set()
    await q.drain()
    # No errors should have been collected from the cancelled task.
    q.flush_errors()
