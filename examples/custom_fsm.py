import itertools
import time
from typing import Literal

from loguru import logger

from pycrew.constraints import HeartbeatTimeout, StateTimeout, Timeout
from pycrew.core import ExecutorCommand
from pycrew.decorators import actor
from pycrew.state import StateMachine, TransitionTable
from pycrew.supervisor import SyncSupervisor
from pycrew.worker import SyncWorker

WorkerState = Literal["idle", "warming", "processing", "cleanup", "crashed", "stopped"]
WorkerEvent = Literal["preload", "start", "stop", "complete", "error"]

WorkerTransitionTable: TransitionTable[WorkerState, WorkerEvent] = {
    "idle": {"preload": "warming"},
    "warming": {"start": "processing", "error": "crashed"},
    "processing": {"stop": "cleanup", "error": "crashed"},
    "cleanup": {"complete": "stopped", "error": "crashed"},
    "crashed": ...,
    "stopped": ...,
}

HEARTBEAT_TIMEOUT = 2

WorkerConstraints: list[Timeout] = [
    HeartbeatTimeout(timeout=HEARTBEAT_TIMEOUT, when="processing"),
    StateTimeout(timeout=10, when="cleanup"),
]


WorkerFSM = StateMachine[WorkerState, WorkerEvent](
    transitions=WorkerTransitionTable,
    initial_event="preload",
    constraints=WorkerConstraints,
)


count = itertools.count()


class MyAlgorithmExecutor(SyncWorker[WorkerState, WorkerEvent]):
    @actor("warming")
    def load_models(self):
        time.sleep(1)
        logger.info(f"[{self.name}] is prepared")
        return self.emit("start")

    @actor("processing")
    def process(self, *, cmd: ExecutorCommand):
        if cmd == "stop":
            return self.emit("stop")

        if next(count) > 3:
            logger.info("Gonna sleep for a long time. Supervisor will think I am dead")
            time.sleep(HEARTBEAT_TIMEOUT + 1)
        else:
            time.sleep(1)
            logger.info("Zzzzz...")

        return self.emit("keepalive")


if __name__ == "__main__":
    supervisor = SyncSupervisor("my-supervisor", "process")
    supervisor.register_workers(
        MyAlgorithmExecutor("foo", fsm=WorkerFSM),
        MyAlgorithmExecutor("bar", fsm=WorkerFSM),
    )
    supervisor.run()
