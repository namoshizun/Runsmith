import asyncio
import multiprocessing
import signal
import threading
from collections.abc import AsyncGenerator, Generator
from typing import Any, Protocol, TypeVar

from loguru import logger

from runsmith.core import EXIT_SIGNALS, IEvent, IQueue
from runsmith.worker import (
    AsyncWorker,
    AsyncWorkerLoop,
    SyncWorker,
    SyncWorkerLoop,
    WorkerActivity,
    WorkerBase,
)

WorkerT = TypeVar("WorkerT", bound=WorkerBase, covariant=True)


class IExecutor(Protocol[WorkerT]):
    @property
    def worker(self) -> WorkerT: ...
    @property
    def name(self) -> str: ...
    @property
    def activity_queue(self) -> IQueue[WorkerActivity]: ...
    @property
    def term_event(self) -> IEvent: ...

    def is_alive(self) -> bool: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def kill(self) -> None: ...


def drive_sync_worker(
    execution: SyncWorkerLoop, term_event: IEvent
) -> Generator[WorkerActivity, None, None]:
    yield next(execution)

    try:
        # Phase 1: Executing activities
        while not term_event.is_set():
            yield execution.send("tick")
        # Phase 2: Stop-and-handshake
        activity = execution.send("stop")
        while activity.kind != "transition_begin":
            activity = execution.send("stop")
        yield activity
        # Phase 3: Termination activities
        while True:
            yield execution.send("tick")
    except StopIteration:
        return


async def drive_async_worker(
    execution: AsyncWorkerLoop, term_event: IEvent
) -> AsyncGenerator[WorkerActivity, None]:
    yield await execution.asend(None)

    try:
        # Phase 1: Executing activities
        while not term_event.is_set():
            yield await execution.asend("tick")
        # Phase 2: Stop-and-handshake
        activity = await execution.asend("stop")
        while activity.kind != "transition_begin":
            activity = await execution.asend("stop")
        yield activity
        # Phase 3: Termination activities
        while True:
            yield await execution.asend("tick")
    except StopAsyncIteration:
        return


class ThreadExecutor(threading.Thread):
    def __init__(
        self,
        worker: SyncWorker,
        term_event: IEvent,
        activity_queue: IQueue[WorkerActivity],
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

        self.name = worker.name
        self.worker = worker
        self.term_event = term_event
        self.activity_queue = activity_queue

    def run(self):
        for activity in drive_sync_worker(self.worker.main_loop(), self.term_event):
            self.activity_queue.put_nowait(activity)

    def stop(self):
        self.term_event.set()

    def kill(self):
        logger.warning("Killing a thread is risky and ill-defined 🤨 We won't do anything...")


class ProcessExecutor(multiprocessing.Process):
    def __init__(
        self,
        worker: SyncWorker,
        term_event: IEvent,
        activity_queue: IQueue[WorkerActivity],
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

        self.name = worker.name
        self.worker = worker
        self.term_event = term_event
        self.activity_queue = activity_queue
        self.__health_proxy = multiprocessing.Value("B", 0)

    def run(self):
        # Ignore exit signals, the supervisor process should take care of handling them
        for sig in EXIT_SIGNALS:
            signal.signal(sig, signal.Handlers.SIG_IGN)

        for activity in drive_sync_worker(self.worker.main_loop(), self.term_event):
            self.activity_queue.put_nowait(activity)

    def stop(self):
        self.term_event.set()


class CoroutineExecutor:
    def __init__(
        self,
        worker: AsyncWorker,
        term_event: IEvent,
        activity_queue: IQueue[WorkerActivity],
    ):
        self.worker = worker
        self.name = worker.name
        self.term_event = term_event
        self.activity_queue = activity_queue
        self.__task = None

    async def _task(self):
        async for activity in drive_async_worker(self.worker.main_loop(), self.term_event):
            self.activity_queue.put_nowait(activity)

    def start(self):
        self.__task = asyncio.create_task(self._task(), name=self.worker.name)

    def is_alive(self) -> bool:
        if not self.__task:
            return False

        return not self.__task.done()

    def stop(self):
        self.term_event.set()

    def kill(self):
        if self.__task:
            if not self.__task.cancelled():
                self.__task.cancel()
