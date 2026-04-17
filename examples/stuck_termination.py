import time

from loguru import logger

from runsmith.decorators import actor
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.supervisor import SyncSupervisor
from runsmith.worker import ExecutorCommand, SyncWorker


class ReluctantWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    def setup(self):
        logger.info(f"[{self.name}] initialization done 🤗")
        return self.emit("run")

    @actor("running")
    def sleepy(self, *, cmd: ExecutorCommand):
        # Not responding to `stop` command at all...
        time.sleep(1)
        logger.info(f"[{self.name}] Zzzzz...")
        return self.emit("keepalive")


if __name__ == "__main__":
    supervisor = SyncSupervisor("my-supervisor", "process")
    supervisor.register_workers(ReluctantWorker("😈"))
    supervisor.run()
