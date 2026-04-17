import multiprocessing
import signal
import sys
import threading
import time
from dataclasses import dataclass
from multiprocessing.queues import Queue as MPQueue
from queue import Empty, Queue
from typing import Callable, Generic, Literal, TypeVar

from loguru import logger

from pycrew.defaults import DefaultWorkerEvent, DefaultWorkerState
from pycrew.errors import (
    IncompatibleExecutorTypeError,
    IncompatibleWorkerTypeError,
    NoWorkersRegisteredError,
)

if sys.version_info >= (3, 11):
    from typing import Self  # pyright: ignore[reportUnreachable]
else:
    from typing_extensions import Self  # pyright: ignore[reportUnreachable]

from pycrew.core import EXIT_SIGNALS, ExecutorCommand, IQueue
from pycrew.decorators import actor, post
from pycrew.evaluator import WorkerStatusEvaluator
from pycrew.execution import IExecutor, ProcessExecutor, ThreadExecutor, drive_sync_worker
from pycrew.settings import settings
from pycrew.utils import Timer
from pycrew.worker import SyncWorker, WorkerActivity, WorkerBase

WorkerT = TypeVar("WorkerT", bound=WorkerBase)

OnActivityCallback = Callable[[WorkerActivity], None]

noop = lambda *_: None


@dataclass
class SupervisionUnit(Generic[WorkerT]):
    worker: WorkerT
    executor: IExecutor
    evaluator: WorkerStatusEvaluator
    restart_quota: int
    restart_count: int = 0

    def retryable(self) -> bool:
        return self.restart_count < self.restart_quota


class SupervisorBase(Generic[WorkerT]):
    def __init__(self):
        self._activity_queue: IQueue[WorkerActivity] | None = None
        self.units: dict[str, SupervisionUnit[WorkerT]] = dict()  # worker name => unit

    @property
    def activity_queue(self) -> IQueue[WorkerActivity]:
        if self._activity_queue is None:
            raise RuntimeError("Activity queue not yet initialized")
        return self._activity_queue

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
            except Empty:
                return

            if unit := self.units.get(activity.worker_name):
                unit.evaluator.record(activity)
            else:
                logger.warning(f"Received activity from unknown worker: {activity}")


class SyncSupervisor(
    SupervisorBase[SyncWorker], SyncWorker[DefaultWorkerState, DefaultWorkerEvent]
):
    def __init__(self, name: str, executor_type: Literal["thread", "process"]):
        if executor_type not in ["thread", "process"]:
            raise IncompatibleExecutorTypeError(
                "Invalid executor type. "
                "SyncSupervisor only supports 'thread' and 'process' executors"
            )

        SupervisorBase.__init__(self)
        SyncWorker.__init__(self, name=name)
        self.executor_type = executor_type

        if executor_type == "thread":
            self._activity_queue = Queue[WorkerActivity]()
        else:
            self._activity_queue = multiprocessing.Queue()

    def clone(self) -> Self:
        instance = self.__class__(name=self.name, executor_type=self.executor_type)  # pyright: ignore[reportArgumentType]
        workers: list[SyncWorker] = [u.worker.clone() for u in self.units.values()]
        instance.register_workers(*workers)
        return instance

    def register_workers(self, *workers: SyncWorker):
        all_sync_workers = workers and all(isinstance(w, SyncWorker) for w in workers)
        if not all_sync_workers:
            raise IncompatibleWorkerTypeError(
                "SyncSupervisor can only supervise instances of SyncWorkers"
            )

        for worker in workers:
            # Build the executor
            if self.executor_type == "thread":
                _executor = ThreadExecutor(
                    worker=worker, term_event=threading.Event(), activity_queue=self.activity_queue
                )
            else:
                assert isinstance(self.activity_queue, MPQueue)
                _executor = ProcessExecutor(
                    worker=worker,
                    term_event=multiprocessing.Event(),
                    activity_queue=self.activity_queue,
                )

            # Build the supervision unit
            self.units[worker.name] = SupervisionUnit[SyncWorker](
                worker=worker,
                executor=_executor,
                evaluator=WorkerStatusEvaluator(worker.fsm),
                restart_quota=settings.supervisor_restart_quota
                if isinstance(worker, SyncSupervisor)
                else settings.worker_restart_quota,
            )

    def run(self, on_activity: OnActivityCallback = noop):
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

    @actor("running")
    def _supervise(self, *, cmd: ExecutorCommand):
        if cmd == "stop":
            return self.emit("terminate")

        with Timer("s") as timer:
            self.drain_activity_queue()
            now = time.monotonic()
            for name in tuple(self.units.keys()):
                unit = self.units[name]
                if unit.evaluator.is_healthy(now):
                    continue

                if not unit.retryable():
                    logger.critical(
                        f"Worker [{name}] has been restarted {unit.restart_count} times. "
                        "Going to give it up and terminate the entire session"
                    )
                    return self.emit("terminate")

                # Destroy the original unit
                restart_count = unit.restart_count + 1
                logger.warning(
                    f"Restarting unhealthy worker [{name}] for the {restart_count}th time..."
                )

                if unit.executor.is_alive():
                    unit.executor.kill()

                del self.units[name]

                # Replace it with the new unit
                self.register_workers(unit.worker.clone())

                unit = self.units[name]
                unit.restart_count = restart_count
                unit.executor.start()

        elapsed = timer.elapsed()
        if (sleep_for := (settings.supervision_interval - elapsed)) > 0.025:
            time.sleep(sleep_for)

        return self.emit("keepalive")

    @actor("terminating")
    def _shutdown(self):
        if all(not unit.executor.is_alive() for unit in self.units.values()):
            return self.emit("complete")

        self.stop_executors()
        """
        NOTE: Actually, we should expect each worker to emit a state transition
        event soon after the stop command is sent. If the worker's main loop
        isn't properly implemented, it may not respond to the stop command and just
        stays in the running state, in which case it should be evaluated as unhealthy.
        """
        time.sleep(2 * settings.supervision_interval)

        # Forcefully terminate workers that linger for too long
        self.drain_activity_queue()
        now = time.monotonic()
        for name, unit in self.units.items():
            if unit.executor.is_alive() and not unit.evaluator.is_healthy(now):
                logger.info(f"Killing lingering unhealthy worker [{name}]")
                unit.executor.kill()

        return self.emit("keepalive")

    @post("terminating", "complete")
    def _on_termination(self):
        logger.info(f"Supervisor [{self.name}] is shutting down... Units count: {len(self.units)}")
