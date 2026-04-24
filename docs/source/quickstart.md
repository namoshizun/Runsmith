# Quickstart

## Supervise sync workers

A sync worker subclasses `SyncWorker[State, Event]` and uses decorators to define runtime behavior:

- `@actor(state)`: method called repeatedly while the worker is in `state`. Return `self.emit(event)` to transition, or `self.emit("keepalive")` to stay in the state.
- `@pre(state, event)`: hook called before entering `state` via `event`.
- `@post(state, event)`: hook called after leaving `state` via `event`; useful for cleanup, logging and error handling.

```python
import time
from loguru import logger
from runsmith.decorators import actor, post
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.supervisor import SyncSupervisor
from runsmith.worker import SyncWorker


class SleepySyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @post("running", "error")
    @post("terminating", "error")
    def on_error(self):
        logger.info(f"[{self.name}] crashed: {self.ctx.exception}")

    @actor("starting")
    def setup(self):
        time.sleep(0.5)
        logger.info(f"[{self.name}] initialization done")
        return self.emit("run")

    @actor("running")
    def work(self):
        if self.ctx.cmd == "stop":
            # Workers must be cooperative and honor the supervisor's stop signal.
            return self.emit("terminate")

        # Any blocking operation shall not exceed the `running` state's heartbeat timeout
        # (defaults to 2s)
        time.sleep(1)
        # Sends a heartbeat, remains in `running`
        return self.emit("keepalive")

    @actor("terminating")
    def graceful_shutdown(self):
        logger.info(f"[{self.name}] shutting down cleanly")
        return self.emit("complete")


supervisor = SyncSupervisor("my-supervisor", "process")  # change to "thread" will run workers in threads instead.
supervisor.register_workers(
    SleepySyncWorker("foo"),  # worker name must be unique
    SleepySyncWorker("bar"),
)
supervisor.run()  # blocks until shutdown signal or all workers reach a terminal state
```

- `self.ctx.cmd` becomes `"stop"` when the supervisor signals workers to shutdown, usually due to process exit signals like `SIGTERM`.
- `self.ctx.exception` is populated when an actor raises.
- `executor_type` is `"thread"` or `"process"`: runs the worker in a thread or a process.

:::{dropdown} What does the equivalent look like without Runsmith?
:icon: code

Notice the boilerplate required just to share state across processes (`multiprocessing.Event`, `multiprocessing.Value`), the manual `p.kill()` + `p.join()` dance for stall recovery, and the restart counter managed separately from the worker itself. **Runsmith handles all of this 💪, and the worker's lifecycle is explicitly declared rather than implied by control flow scattered across functions**.

```python
import time
import signal
import multiprocessing
from loguru import logger

HEARTBEAT_TIMEOUT = 2.0
MAX_RESTARTS = 3


def sleepy_worker(name: str, stop_event: multiprocessing.Event, heartbeat: multiprocessing.Value):
    # "starting"
    try:
        time.sleep(0.5)
        logger.info(f"[{name}] initialization done")
    except Exception as e:
        logger.info(f"[{name}] crashed: {e}")
        return

    # "running"
    while not stop_event.is_set():
        try:
            time.sleep(1)
            heartbeat.value = time.monotonic()
        except Exception as e:
            logger.info(f"[{name}] crashed: {e}")
            return

    # "terminating"
    logger.info(f"[{name}] shutting down cleanly")


processes: dict[str, multiprocessing.Process] = {}
stop_events: dict[str, multiprocessing.Event] = {}
heartbeats: dict[str, multiprocessing.Value] = {}
restart_counts: dict[str, int] = {}


def launch(name: str):
    stop_event = multiprocessing.Event()
    heartbeat = multiprocessing.Value("d", time.monotonic())
    stop_events[name] = stop_event
    heartbeats[name] = heartbeat
    p = multiprocessing.Process(target=sleepy_worker, args=(name, stop_event, heartbeat), daemon=True)
    processes[name] = p
    p.start()


def supervisor_loop(names: list[str]):
    while True:
        time.sleep(0.5)
        for name in names:
            p = processes[name]
            hb_age = time.monotonic() - heartbeats[name].value

            if not p.is_alive() or hb_age > HEARTBEAT_TIMEOUT:
                if hb_age > HEARTBEAT_TIMEOUT:
                    logger.warning(f"[{name}] heartbeat stalled, killing process")
                    p.kill()
                    p.join()

                count = restart_counts.get(name, 0)
                if count >= MAX_RESTARTS:
                    logger.error(f"[{name}] exceeded restart quota, giving up")
                    continue
                restart_counts[name] = count + 1
                logger.info(f"[{name}] restarting ({restart_counts[name]}/{MAX_RESTARTS})")
                launch(name)


def shutdown(sig, frame):
    for ev in stop_events.values():
        ev.set()
    for p in processes.values():
        p.join(timeout=10)


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

for name in ["foo", "bar"]:
    launch(name)

supervisor_loop(["foo", "bar"])
```
:::

## Supervise async workers

Async workers subclass `AsyncWorker[State, Event]` and implements asynchronous actors and hooks. `AsyncSupervisor` runs workers as asyncio tasks in one event loop; the API is otherwise the same.

```python
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
        logger.info(f"[{self.name}] crashed: {self.ctx.exception}")

    @actor("starting")
    async def setup(self):
        await asyncio.sleep(0.5)
        return self.emit("run")

    @actor("running")
    async def work(self):
        if self.ctx.cmd == "stop":
            return self.emit("terminate")
        # Be careful not to block the event loop for too long.
        await asyncio.sleep(1)
        return self.emit("keepalive")

    @actor("terminating")
    async def graceful_shutdown(self):
        return self.emit("complete")


async def main():
    supervisor = AsyncSupervisor("my-supervisor")
    supervisor.register_workers(
        SleepyAsyncWorker("foo"),
        SleepyAsyncWorker("bar"),
    )
    await supervisor.run()


asyncio.run(main())
```

`AsyncSupervisor` has no `executor_type` argument as workers always run the asyncio event loop.

## Supervisor tree

`SyncSupervisor` is itself a `SyncWorker`, so it can be registered as a child of another supervisor. This forms a supervisor tree where each supervisor node independently manages and restarts its direct children.

```python
from runsmith.supervisor import SyncSupervisor

root = SyncSupervisor("root", "thread")
child = SyncSupervisor("child", "process")

child.register_workers(worker1, worker2)
root.register_workers(child, worker3)

root.run()
```

- The root supervisor runs in the calling thread. Each unit is launched with the executor declared on its parent supervisor.
- Child supervisors use their own restart quota, separate from leaf workers (`RUNSMITH_SUPERVISOR_RESTART_QUOTA`).
- Mixing `"thread"` and `"process"` executors at different levels is fully supported.

```text
root
├── child  (thread)
│   ├── worker1  (process)
│   └── worker2  (process)
└── worker3  (thread)
```
