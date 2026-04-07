import itertools
import time

from pycrew.core import Event, Heartbeat
from pycrew.decorators import actor, post
from pycrew.defaults import DefaultWorkerFSM
from pycrew.worker import CrewSyncWorker

count = itertools.count()


class BasicWorker(CrewSyncWorker):
    @post("running", "error")
    @post("terminating", "error")
    def on_error(self):
        print("something really bad happened!!!!")

    @actor("running")
    def tick(self):
        if next(count) > 3:
            return Event("terminate")

        time.sleep(1)
        return Heartbeat()

    @actor("terminating")
    def graceful_shutdown(self):
        time.sleep(0.1)
        return Event("complete")


if __name__ == "__main__":
    worker = BasicWorker("basic", DefaultWorkerFSM)
    for activity in worker.run():
        print(activity)
