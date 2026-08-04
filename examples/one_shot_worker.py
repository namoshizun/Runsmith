from loguru import logger

from runsmith.decorators import actor
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.supervisor import SyncSupervisor
from runsmith.worker import SyncWorker


class OneShotWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @actor("starting")
    def setup(self):
        logger.info(f"[{self.name}] setup done")
        return self.emit("run")

    @actor("running")
    def run_once(self):
        # Finite work — no need to wait for ctx.cmd == "stop"
        logger.info(f"[{self.name}] finished work")
        return self.emit("complete")

    @actor("terminating")
    def teardown(self):
        logger.info(f"[{self.name}] clean shutdown")
        return self.emit("complete")


if __name__ == "__main__":
    supervisor = SyncSupervisor("root", "thread")
    supervisor.register_workers(OneShotWorker("one-shot"))
    supervisor.run()  # returns after the worker reaches stopped
