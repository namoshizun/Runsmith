import asyncio
import enum
import inspect
import multiprocessing
import signal
import sys
import threading
import time
from collections.abc import Coroutine
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any, Callable, Generic, Literal, TypeVar, cast

from loguru import logger

from runsmith.defaults import DefaultWorkerEvent, SupervisorFSM, SupervisorState
from runsmith.errors import (
    IncompatibleExecutorTypeError,
    IncompatibleWorkerTypeError,
    NoWorkersRegisteredError,
    RetryExhaustedError,
)

if sys.version_info >= (3, 11):
    from typing import Self  # pyright: ignore[reportUnreachable]
else:
    from typing_extensions import Self  # pyright: ignore[reportUnreachable]

from runsmith.core import EXIT_SIGNALS, IQueue
from runsmith.decorators import actor, post
from runsmith.evaluator import WorkerStatusEvaluator
from runsmith.execution import (
    CoroutineExecutor,
    IExecutor,
    ProcessExecutor,
    ThreadExecutor,
    drive_async_worker,
    drive_sync_worker,
)
from runsmith.settings import settings
from runsmith.state import Terminal
from runsmith.utils import CoroutineQueue
from runsmith.worker import AsyncWorker, SyncWorker, WorkerActivity, WorkerBase

WorkerT = TypeVar("WorkerT", bound=WorkerBase)

SyncOnActivityCallback = Callable[[WorkerActivity], None]
AsyncOnActivityCallback = Callable[[WorkerActivity], Coroutine[Any, Any, None]]

noop = lambda *_: None


async def anoop(_: WorkerActivity) -> None:
    return None


class UnitStatus(enum.Enum):
    """The observed condition of a supervised unit."""

    HEALTHY = "healthy"  # alive and honoring its constraints
    LINGERING = "lingering"  # alive but violating its constraints
    COMPLETED = "completed"  # exited from a `Terminal.OK` state
    FAILED = "failed"  # exited without reaching a successful terminal state


@dataclass
class SupervisionUnit(Generic[WorkerT]):
    worker: WorkerT
    executor: IExecutor
    evaluator: WorkerStatusEvaluator
    restart_quota: int
    restart_count: int = 0
    retired: bool = False

    def retryable(self) -> bool:
        return self.restart_count < self.restart_quota

    def status(self, now: float) -> UnitStatus:
        if self.executor.is_alive():
            return UnitStatus.HEALTHY if self.evaluator.is_healthy(now) else UnitStatus.LINGERING

        if self.evaluator.terminal_outcome() is Terminal.OK:
            return UnitStatus.COMPLETED

        return UnitStatus.FAILED


class SupervisorBase(Generic[WorkerT]):
    def __init__(self, executor_type: Literal["thread", "process", "coroutine"]):
        self._activity_queue: IQueue[WorkerActivity] | None = None
        self._worker_templates: dict[str, WorkerT] = dict()
        self.executor_type = executor_type
        self.units: dict[str, SupervisionUnit[WorkerT]] = dict()  # worker name => unit

    @property
    def activity_queue(self) -> IQueue[WorkerActivity]:
        if self._activity_queue is None:
            raise RuntimeError("Activity queue not yet initialized")
        return self._activity_queue

    def materialize_units(self, *, worker_name: str | None = None):
        for worker in self._worker_templates.values():
            if worker_name and worker.name != worker_name:
                continue

            # Build the executor
            _worker = worker.clone()
            match self.executor_type:
                case "thread":
                    _executor = ThreadExecutor(
                        worker=cast(SyncWorker, _worker),
                        term_event=threading.Event(),
                        activity_queue=self.activity_queue,
                    )
                case "process":
                    _executor = ProcessExecutor(
                        worker=cast(SyncWorker, _worker),
                        term_event=multiprocessing.Event(),
                        activity_queue=self.activity_queue,
                    )
                case "coroutine":
                    _executor = CoroutineExecutor(
                        worker=cast(AsyncWorker, _worker),
                        term_event=asyncio.Event(),
                        activity_queue=self.activity_queue,
                    )
                case _:
                    raise ValueError(f"Invalid executor type: {self.executor_type}")

            # Build the supervision unit
            restart_quota = (
                settings.supervisor_restart_quota
                if isinstance(worker, SupervisorBase)
                else settings.worker_restart_quota
            )
            self.units[worker.name] = SupervisionUnit[WorkerT](
                worker=_worker,
                executor=_executor,
                evaluator=WorkerStatusEvaluator(worker.fsm),
                restart_quota=restart_quota,
            )

    def restart_unit(self, name: str) -> SupervisionUnit[WorkerT]:
        unit = self.units[name]

        # Destroy the original unit
        restart_count = unit.restart_count + 1
        logger.warning(f"Restarting crashed worker [{name}] for the {restart_count}th time...")

        if unit.executor.is_alive():
            unit.executor.kill()

        del self.units[name]

        # Replace it with the new unit
        self.materialize_units(worker_name=name)
        unit = self.units[name]
        unit.restart_count = restart_count
        unit.executor.start()
        return unit

    def start_executors(self):
        if not self.units:
            raise NoWorkersRegisteredError("No workers registered")

        for unit in self.units.values():
            unit.executor.start()

    def stop_executors(self):
        for unit in self.units.values():
            unit.executor.stop()

    def kill_executors(self):
        for unit in self.units.values():
            executor = unit.executor
            if executor.is_alive():
                executor.kill()

    def drain_activity_queue(self):
        while True:
            try:
                activity = self.activity_queue.get_nowait()
            except (Empty, asyncio.QueueEmpty):
                return

            if unit := self.units.get(activity.worker_name):
                unit.evaluator.record(activity)
            else:
                logger.warning(f"Received activity from unknown worker: {activity}")

    @property
    def all_units_retired(self) -> bool:
        return all(unit.retired for unit in self.units.values())

    @property
    def all_executors_down(self) -> bool:
        return all(not unit.executor.is_alive() for unit in self.units.values())

    def supervise_units(self):
        now = time.monotonic()
        for name, unit in tuple(self.units.items()):
            if unit.retired:
                continue

            match unit.status(now):
                case UnitStatus.HEALTHY:
                    continue
                case UnitStatus.COMPLETED:
                    logger.info(f"Worker [{name}] completed its work, retiring it")
                    unit.retired = True
                case UnitStatus.LINGERING | UnitStatus.FAILED:
                    if not unit.retryable():
                        logger.critical(
                            f"Worker [{name}] is beyond repair after {unit.restart_count} restarts"
                        )
                        raise RetryExhaustedError()

                    self.restart_unit(name)

    def drain_units(self):
        now = time.monotonic()
        for name, unit in self.units.items():
            if unit.status(now) is UnitStatus.LINGERING:
                logger.info(f"Killing lingering worker [{name}]")
                unit.executor.kill()

    def supervision_tick(self) -> DefaultWorkerEvent | Literal["keepalive"]:
        if not self.units:
            raise RuntimeError("No materialized units to supervise!")

        self.drain_activity_queue()
        try:
            self.supervise_units()
        except RetryExhaustedError:
            return "error"

        # Supervisor stops when all works proactively terminated
        if self.all_units_retired:
            return "complete"

        return "keepalive"

    def shutdown_tick(self) -> DefaultWorkerEvent | Literal["keepalive"]:
        self.drain_activity_queue()
        if self.all_executors_down:
            return "complete"

        self.stop_executors()
        self.drain_units()
        return "keepalive"


class SyncSupervisor(SupervisorBase[SyncWorker], SyncWorker[SupervisorState, DefaultWorkerEvent]):
    def __init__(self, name: str, executor_type: Literal["thread", "process"]):
        if executor_type not in ["thread", "process"]:
            raise IncompatibleExecutorTypeError(
                "Invalid executor type. "
                "SyncSupervisor only supports 'thread' and 'process' executors"
            )

        SupervisorBase.__init__(self, executor_type=executor_type)
        SyncWorker.__init__(self, name=name, fsm=SupervisorFSM)

    def clone(self) -> Self:
        instance = self.__class__(name=self.name, executor_type=self.executor_type)  # pyright: ignore[reportArgumentType]
        workers: list[SyncWorker] = [w.clone() for w in self._worker_templates.values()]
        instance.register_workers(*workers)
        return instance

    def register_workers(self, *workers: SyncWorker):
        all_sync_workers = workers and all(isinstance(w, SyncWorker) for w in workers)
        if not all_sync_workers:
            raise IncompatibleWorkerTypeError(
                "SyncSupervisor can only supervise instances of SyncWorkers"
            )

        for worker in workers:
            self._worker_templates[worker.name] = worker

    def run(self, on_activity: SyncOnActivityCallback = noop):
        if inspect.iscoroutinefunction(on_activity):
            raise TypeError("on_activity must be a sync callback for sync supervisors")

        # The root supervisor's entry point
        term_event = threading.Event()

        for sig in EXIT_SIGNALS:
            signal.signal(sig, lambda *_: term_event.set())

        for activity in drive_sync_worker(self.main_loop(), term_event):
            on_activity(activity)

    def before_exit(self, is_graceful: bool):
        if not is_graceful:
            self.kill_executors()

    # ── FSM actors ──────────────────────────────────────────────
    @actor("starting")
    def _boot(self):
        try:
            # Initialize the activity queue
            if self.executor_type == "thread":
                self._activity_queue = Queue[WorkerActivity](
                    maxsize=settings.activity_queue_maxsize
                )
            else:
                self._activity_queue = multiprocessing.Queue(
                    maxsize=settings.activity_queue_maxsize
                )

            # Materialize and start all the units
            self.materialize_units()
            self.start_executors()
            logger.opt(colors=True).info(
                f"<e>Supervisor [{self.name}] booted {len(self.units)} units</e>"
            )
            return self.emit("run")
        except NoWorkersRegisteredError:
            logger.critical(
                f"Supervisor [{self.name}] failed to start due to no workers registered!!"
            )
            return self.emit("error")

    @actor("running", min_interval=settings.supervision_interval)
    def _supervise(self):
        if self.ctx.cmd == "stop":
            return self.emit("complete")

        return self.emit(self.supervision_tick())

    @actor("reaping", min_interval=settings.supervision_interval)
    @actor("terminating", min_interval=settings.supervision_interval)
    def _shutdown(self):
        return self.emit(self.shutdown_tick())

    @post("reaping", "complete")
    @post("terminating", "complete")
    @post("terminating", "error")
    def _on_termination(self):
        logger.info(f"Supervisor [{self.name}] is shutting down... Units count: {len(self.units)}")


class AsyncSupervisor(
    SupervisorBase[AsyncWorker], AsyncWorker[SupervisorState, DefaultWorkerEvent]
):
    def __init__(self, name: str):
        SupervisorBase.__init__(self, executor_type="coroutine")
        AsyncWorker.__init__(self, name=name, fsm=SupervisorFSM)

    def clone(self) -> Self:
        instance = self.__class__(name=self.name)
        workers: list[AsyncWorker] = [w.clone() for w in self._worker_templates.values()]
        instance.register_workers(*workers)
        return instance

    def register_workers(self, *workers: AsyncWorker):
        all_async_workers = workers and all(isinstance(w, AsyncWorker) for w in workers)
        if not all_async_workers:
            raise IncompatibleWorkerTypeError(
                "AsyncSupervisor can only supervise instances of AsyncWorkers"
            )

        for worker in workers:
            self._worker_templates[worker.name] = worker

    async def run(self, on_activity: AsyncOnActivityCallback = anoop):
        if not inspect.iscoroutinefunction(on_activity):
            raise TypeError("on_activity must be an async callback for async supervisors")

        # The root supervisor's entry point
        term_event = asyncio.Event()
        callbacks = CoroutineQueue(max_pending=settings.activity_callback_task_queue_size)

        for sig in EXIT_SIGNALS:
            signal.signal(sig, lambda *_: term_event.set())

        async for activity in drive_async_worker(self.main_loop(), term_event):
            callbacks.flush_errors()
            overflowed = callbacks.submit(on_activity(activity))
            if overflowed:
                logger.warning(
                    f"Callback backlog saturated in supervisor [{self.name}], dropped oldest callback task"
                )

        await callbacks.drain()
        callbacks.flush_errors()

    def before_exit(self, is_graceful: bool):
        if not is_graceful:
            self.kill_executors()

    # ── FSM actors ──────────────────────────────────────────────
    @actor("starting")
    async def _boot(self):
        try:
            # Initialize the activity queue
            self._activity_queue = asyncio.Queue[WorkerActivity](
                maxsize=settings.activity_queue_maxsize
            )

            # Materialize and start all the units
            self.materialize_units()
            self.start_executors()
            logger.opt(colors=True).info(
                f"<e>Supervisor [{self.name}] booted {len(self.units)} units</e>"
            )
            return self.emit("run")
        except NoWorkersRegisteredError:
            logger.critical(
                f"Supervisor [{self.name}] failed to start due to no workers registered!!"
            )
            return self.emit("error")

    @actor("running", min_interval=settings.supervision_interval)
    async def _supervise(self):
        if self.ctx.cmd == "stop":
            return self.emit("complete")

        return self.emit(self.supervision_tick())

    @actor("reaping", min_interval=settings.supervision_interval)
    @actor("terminating", min_interval=settings.supervision_interval)
    async def _shutdown(self):
        return self.emit(self.shutdown_tick())

    @post("reaping", "complete")
    @post("terminating", "complete")
    @post("terminating", "error")
    async def _on_termination(self):
        logger.info(f"Supervisor [{self.name}] is shutting down... Units count: {len(self.units)}")
