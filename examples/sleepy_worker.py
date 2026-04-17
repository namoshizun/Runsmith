import time

from loguru import logger

from runsmith.decorators import actor, post
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.worker import ExecutorCommand, SyncWorker


class SleepyWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @post("running", "error")
    @post("terminating", "error")
    def on_error(self):
        logger.info(
            f"[{self.name}] Something really bad happened!!!! Exception: {self.ctx.exception}"
        )

    @actor("starting")
    def setup(self):
        time.sleep(0.5)
        logger.info(f"[{self.name}] Initialization done 🤗")
        return self.emit("run")

    @actor("running")
    def sleepy(self, *, cmd: ExecutorCommand):
        if cmd == "stop":
            return self.emit("terminate")

        time.sleep(1)
        logger.info(f"[{self.name}] Zzzzz...")
        return self.emit("keepalive")

    @actor("terminating")
    def graceful_shutdown(self):
        logger.info(f"[{self.name}] Peace out ✌️")
        time.sleep(0.1)
        return self.emit("complete")
