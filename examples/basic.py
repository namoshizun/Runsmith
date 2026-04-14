import signal
import threading
import time

from loguru import logger

from pycrew.decorators import actor, post
from pycrew.execution import drive_sync_worker
from pycrew.worker import ExecutorCommand, SyncWorker

TERM_EVENT = threading.Event()


def handle_exit():
    def handler(sig_num: int, *_):
        TERM_EVENT.set()

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT, signal.SIGABRT):
        signal.signal(sig, handler)


class BasicWorker(SyncWorker):
    @post("running", "error")
    @post("terminating", "error")
    def on_error(self):
        logger.info(f"Something really bad happened!!!! Exception: {self.ctx.exception}")

    @actor("starting")
    def setup(self):
        time.sleep(0.5)
        logger.info("Initialization done 🤗")
        return self.emit("run")

    @actor("running")
    def sleepy(self, *, cmd: ExecutorCommand):
        if cmd == "stop":
            return self.emit("terminate")

        time.sleep(1)
        return self.emit("keepalive")

    @actor("terminating")
    def graceful_shutdown(self):
        logger.info("Peace out ✌️")
        time.sleep(0.1)
        return self.emit("complete")


if __name__ == "__main__":
    handle_exit()
    worker = BasicWorker("foo")
    for activity in drive_sync_worker(worker.main_loop(), TERM_EVENT):
        logger.info(activity)
