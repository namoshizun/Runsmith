import asyncio
import multiprocessing
import signal
import threading
from multiprocessing.queues import Queue as MPQueue
from multiprocessing.synchronize import Event as MPEvent
from queue import Queue
from typing import Any, Protocol, TypeVar

from loguru import logger

from pycrew.core import EXIT_SIGNALS, IEvent, IQueue
from pycrew.worker import CrewAsyncWorker, CrewSyncWorker, WorkerActivity, WorkerBase

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


class ThreadExecutor(threading.Thread):
    def __init__(
        self,
        worker: CrewSyncWorker,
        term_event: threading.Event,
        activity_queue: Queue[WorkerActivity],
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

        self.name = worker.name
        self.worker = worker
        self.term_event = term_event
        self.activity_queue = activity_queue

    def run(self):
        execution = self.worker.main_loop()
        execution.send("tick")
        while not self.term_event.is_set():
            activity = execution.send("tick")
            self.activity_queue.put_nowait(activity)

        execution.send("stop")

    def stop(self):
        self.term_event.set()

    def kill(self):
        logger.warning("Killing a thread is risky and ill-defined 🤨 We won't do anything...")


class ProcessExecutor(multiprocessing.Process):
    def __init__(
        self,
        worker: CrewSyncWorker,
        term_event: MPEvent,
        activity_queue: MPQueue,
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

        execution = self.worker.main_loop()
        execution.send("tick")
        while not self.term_event.is_set():
            activity = execution.send("tick")
            self.activity_queue.put_nowait(activity)

        execution.send("stop")

    def stop(self):
        self.term_event.set()


class CoroutineExecutor:
    def __init__(
        self,
        worker: CrewAsyncWorker,
        term_event: asyncio.Event,
        activity_queue: asyncio.Queue[WorkerActivity],
    ):
        self.worker = worker
        self.name = worker.name
        self.term_event = term_event
        self.activity_queue = activity_queue
        self.__task = None

    async def _task(self):
        execution = self.worker.main_loop()

        await execution.asend("tick")
        while not self.term_event.is_set():
            activity = await execution.asend("tick")
            self.activity_queue.put_nowait(activity)

        await execution.asend("stop")

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
