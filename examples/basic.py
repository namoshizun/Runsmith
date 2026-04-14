import time

from pycrew.decorators import actor, post
from pycrew.defaults import DefaultWorkerFSM
from pycrew.worker import ExecutorCommand, SyncWorker


class BasicWorker(SyncWorker):
    @post("running", "error")
    @post("terminating", "error")
    def on_error(self):
        print(f"Something really bad happened!!!! Exception: {self.ctx.exception}")

    @actor("starting")
    def setup(self):
        time.sleep(0.5)
        print("Initialization done 🤗")
        return self.emit("run")

    @actor("running")
    def sleepy(self, *, cmd: ExecutorCommand):
        if cmd == "stop":
            return self.emit("terminate")

        time.sleep(1)
        return self.emit("keepalive")

    @actor("terminating")
    def graceful_shutdown(self):
        time.sleep(0.1)
        return self.emit("complete")


if __name__ == "__main__":
    worker = BasicWorker("foo", DefaultWorkerFSM)
    for activity in worker.main_loop():
        print(activity)
