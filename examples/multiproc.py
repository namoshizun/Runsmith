import multiprocessing
from multiprocessing.queues import Queue as MPQueue
from queue import Empty
from typing import Any

from runsmith.decorators import actor
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.supervisor import SyncSupervisor
from runsmith.worker import SyncWorker


class QueueReaderWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    def __init__(self, name: str, queue: MPQueue, **kwargs: Any):
        super().__init__(name, **kwargs)
        self.queue = queue

    @actor("starting")
    def on_start(self):
        return self.emit("run")

    @actor("running")
    def poll_queue(self):
        if self.ctx.cmd == "stop":
            return self.emit("terminate")

        try:
            self.queue.get(timeout=0.1)
        except Empty:
            pass

        return self.emit("keepalive")

    @actor("terminating")
    def on_terminate(self):
        return self.emit("complete")

    def clone(self) -> "QueueReaderWorker":
        return QueueReaderWorker(name=self.name, queue=self.queue)


if __name__ == "__main__":
    """
    Note: won't work for Python 3.11.5
    See CPython issue: https://github.com/python/cpython/issues/108520
    """
    queue = multiprocessing.Queue()

    root_sup = SyncSupervisor("root", "process")
    child_sup = SyncSupervisor("child", "process")

    worker1 = QueueReaderWorker("worker1", queue)
    worker2 = QueueReaderWorker("worker2", queue)
    worker3 = QueueReaderWorker("worker3", queue)

    child_sup.register_workers(worker1, worker2)
    root_sup.register_workers(child_sup, worker3)

    root_sup.run()
