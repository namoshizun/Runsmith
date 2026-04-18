import asyncio
import sys

from examples.async_worker import SleepyAsyncWorker
from examples.sync_worker import SleepySyncWorker
from runsmith.supervisor import AsyncSupervisor, SyncSupervisor


def sync_example():
    supervisor = SyncSupervisor("my-supervisor", "thread")
    supervisor.register_workers(
        SleepySyncWorker("foo"),
        SleepySyncWorker("bar"),
    )
    supervisor.run()


async def async_example():
    supervisor = AsyncSupervisor("my-supervisor")
    supervisor.register_workers(
        SleepyAsyncWorker("foo"),
        SleepyAsyncWorker("bar"),
    )
    await supervisor.run()


if __name__ == "__main__":
    if sys.argv[1] == "sync":
        sync_example()
    elif sys.argv[1] == "async":
        asyncio.run(async_example())
