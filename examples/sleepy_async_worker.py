import asyncio

from loguru import logger

from runsmith.decorators import actor, post
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.supervisor import AsyncSupervisor
from runsmith.worker import AsyncWorker


class SleepyAsyncWorker(AsyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @post("running", "error")
    @post("terminating", "error")
    async def on_error(self):
        logger.info(
            f"[{self.name}] Something really bad happened!!!! Exception: {self.ctx.exception}"
        )

    @actor("starting")
    async def setup(self):
        await asyncio.sleep(0.5)
        logger.info(f"[{self.name}] Initialization done 🤗")
        return self.emit("run")

    @actor("running")
    async def sleepy(self):
        if self.ctx.cmd == "stop":
            return self.emit("terminate")

        await asyncio.sleep(1)
        logger.info(f"[{self.name}] Zzzzz...")
        return self.emit("keepalive")

    @actor("terminating")
    async def graceful_shutdown(self):
        logger.info(f"[{self.name}] Peace out ✌️")
        await asyncio.sleep(0.1)
        return self.emit("complete")


async def _run():
    supervisor = AsyncSupervisor("my-supervisor")
    supervisor.register_workers(
        SleepyAsyncWorker("foo"),
        SleepyAsyncWorker("bar"),
    )
    await supervisor.run()


if __name__ == "__main__":
    asyncio.run(_run())
