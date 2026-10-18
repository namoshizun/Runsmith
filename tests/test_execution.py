from __future__ import annotations

import asyncio
import threading
from contextlib import aclosing, closing
from typing import Literal

import pytest

from runsmith.decorators import actor
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.execution import drive_async_worker, drive_sync_worker
from runsmith.worker import AsyncWorker, SyncWorker


class CooperativeSyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    def setup(self) -> DefaultWorkerEvent:
        return self.emit("run")

    @actor("running")
    def work(self) -> DefaultWorkerEvent | Literal["keepalive"]:
        return self.emit("complete" if self.ctx.cmd == "stop" else "keepalive")

    @actor("terminating")
    def cleanup(self) -> DefaultWorkerEvent:
        assert self.ctx.cmd == "stop"
        return self.emit("complete")


class CooperativeAsyncWorker(AsyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    async def setup(self) -> DefaultWorkerEvent:
        return self.emit("run")

    @actor("running")
    async def work(self) -> DefaultWorkerEvent | Literal["keepalive"]:
        return self.emit("complete" if self.ctx.cmd == "stop" else "keepalive")

    @actor("terminating")
    async def cleanup(self) -> DefaultWorkerEvent:
        assert self.ctx.cmd == "stop"
        return self.emit("complete")


@pytest.mark.parametrize("stop_after", range(7))
def test_sync_stop_persists_through_startup_and_cleanup(stop_after: int) -> None:
    worker = CooperativeSyncWorker("worker")
    term_event = threading.Event()
    with closing(worker.main_loop()) as execution:
        driver = drive_sync_worker(execution, term_event)
        for _ in range(stop_after):
            next(driver)
        term_event.set()
        # Bound the regression: a lost stop would otherwise keep running forever.
        for _ in range(12):
            try:
                next(driver)
            except StopIteration:
                break

        assert list(worker.ctx.history)[-1] == ("complete", "stopped")


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_after", range(7))
async def test_async_stop_persists_through_startup_and_cleanup(stop_after: int) -> None:
    worker = CooperativeAsyncWorker("worker")
    term_event = asyncio.Event()
    async with aclosing(worker.main_loop()) as execution:
        driver = drive_async_worker(execution, term_event)
        for _ in range(stop_after):
            await driver.__anext__()
        term_event.set()
        for _ in range(12):
            try:
                await driver.__anext__()
            except StopAsyncIteration:
                break

        assert list(worker.ctx.history)[-1] == ("complete", "stopped")
