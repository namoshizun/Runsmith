from __future__ import annotations

import threading

import pytest

from runsmith.decorators import actor, pre
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.errors import InvalidHookFunctionTypeError
from runsmith.execution import drive_sync_worker
from runsmith.state import StateMachine
from runsmith.worker import AsyncWorker, SyncWorker, WorkerActivity


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


# ── AsyncWorker ───────────────────────────────────────────────────────────────


class DemoAsyncWorker(AsyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    async def setup(self) -> DefaultWorkerEvent:
        return self.emit("run")

    @actor("running")
    async def running(self) -> DefaultWorkerEvent:
        return self.emit("terminate")

    @actor("terminating")
    async def teardown(self) -> DefaultWorkerEvent:
        return self.emit("complete")


@pytest.mark.asyncio
async def test_async_worker_main_loop_transitions_to_terminal_state() -> None:
    worker = DemoAsyncWorker("demo-async")
    execution = worker.main_loop()
    await execution.asend(None)

    while True:
        try:
            await execution.asend("tick")
        except StopAsyncIteration:
            break

    assert list(worker.ctx.history) == [
        ("start", "starting"),
        ("run", "running"),
        ("terminate", "terminating"),
        ("complete", "stopped"),
    ]


# ── Hook validation ───────────────────────────────────────────────────────────


def test_sync_worker_raises_on_async_hook() -> None:
    with pytest.raises(InvalidHookFunctionTypeError):

        class BadSyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
            @actor("starting")
            async def setup(self):  # type: ignore[override]
                ...


def test_async_worker_raises_on_sync_hook() -> None:
    with pytest.raises(InvalidHookFunctionTypeError):

        class BadAsyncWorker(AsyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
            @actor("starting")
            def setup(self):  # type: ignore[override]
                ...


def test_pre_hook_invoked_during_state_entry() -> None:
    entered: list[str] = []

    class WorkerWithPre(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
        @pre("starting", "start")
        def on_entering_starting(self) -> None:
            entered.append("starting")

        @actor("starting")
        def setup(self) -> DefaultWorkerEvent:
            return self.emit("run")

        @actor("running")
        def running(self) -> DefaultWorkerEvent:
            return self.emit("terminate")

        @actor("terminating")
        def teardown(self) -> DefaultWorkerEvent:
            return self.emit("complete")

    term = threading.Event()
    term.set()
    for _ in drive_sync_worker(WorkerWithPre("w").main_loop(), term):
        pass

    assert entered == ["starting"]


# ── get_actor_func fallbacks ──────────────────────────────────────────────────


def _custom_fsm() -> StateMachine:
    return StateMachine(
        transitions={
            "idle": {"begin": "middle"},
            "middle": {"done": "final"},
            "final": ...,
        },
        initial_event="begin",
    )


def test_worker_falls_back_to_single_available_event_when_no_actor() -> None:
    worker = SyncWorker("w", fsm=_custom_fsm())
    # "middle" has no @actor registered; only one event "done" → fallback
    func = worker.get_actor_func("middle")  # type: ignore[arg-type]
    assert func() == "done"


def test_worker_raises_when_no_actor_and_multiple_events() -> None:
    fsm = StateMachine(
        transitions={
            "idle": {"begin": "fork"},
            "fork": {"left": "done", "right": "done"},
            "done": ...,
        },
        initial_event="begin",
    )
    worker = SyncWorker("w", fsm=fsm)
    with pytest.raises(RuntimeError, match="No actor registered"):
        worker.get_actor_func("fork")  # type: ignore[arg-type]
