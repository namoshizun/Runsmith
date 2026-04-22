## Guide

You are required to generate the full Sphinx documentation for this library. You need to use the `book` theme. `sphinx-book-theme` has already been added to the `docs` dependency group. The doc must prioritize clarity and readability over completeness, especially for the quickstart and examples sections. The "advanced" section can be more thorough and comprehensive.

The library is called **Runsmith**. Its tagline: *"Supervisor-tree framework for building predictable and resilient programs."*

Use a warm but technically precise tone. Do not over-explain. Assume the reader knows Python and basic concurrency. All code examples should be self-contained and runnable.


## Sections

### Introduction

Write a concise introduction (3–4 paragraphs) covering the following:

- **What Runsmith is**: a Python 3.10+ library for building resilient, long-running programs using a supervisor-tree pattern. Workers are modeled as finite-state machines (FSMs). They can run in threads, OS processes, or asyncio tasks. A supervisor monitors them, detects failures via typed constraint violations, and restarts them automatically within configurable quotas.

- **How it differs from supervisord**: `supervisord` is an OS-level process control daemon — it manages external programs by PID using config files. Runsmith is an in-process, programmable Python library. The unit of supervision is not an OS process but a *typed, observable worker* with a declared lifecycle. Runsmith gives you structured concurrency with explicit state semantics, composable hierarchies, and real-time activity streams — not just process management.

- **Three design pillars** (render as an admonition or highlighted feature grid):
  - **Predictability** — Every worker's lifecycle is a finite-state machine. States, transitions, and terminal conditions are declared upfront using plain Python types. There is no implicit behavior or hidden control flow.
  - **Composability** — Supervisors are themselves workers. You can nest them to any depth, freely mixing thread, process, and asyncio execution in a single supervision tree.
  - **Resilience** — Workers emit a continuous stream of activity events. Constraint violations (heartbeat timeout, transition timeout, state residence cap) are detected automatically and trigger restarts within configured quotas.

- Close with the **default worker FSM diagram**, rendered using a code block. Explain it briefly: workers always start in `idle`, move through `starting` → `running` → `terminating` → `stopped`, and can crash at any point into the terminal `crashed` state. The `keepalive=2` annotation means the `running` actor must emit a heartbeat at least every 2 seconds or the supervisor will consider it unhealthy.

```
→ idle  (initial)
      start → starting

  crashed  (terminal)

  running  (keepalive=2)
      error → crashed
      terminate → terminating

  starting  (state_timeout=10)
      error → crashed
      run → running

  stopped  (terminal)

  terminating  (state_timeout=10)
      complete → stopped
      error → crashed
```


### Quickstart

Open with a one-liner install:

```bash
pip install runsmith
# or
uv add runsmith
```

Then walk through the three sub-sections below. Keep each sub-section focused on showing a single working pattern. Do not repeat concepts already introduced.

#### Supervise sync workers

Explain: A sync worker subclasses `SyncWorker[State, Event]` and uses three decorator types to wire up its FSM:

- `@actor(state)` — the method the supervisor calls repeatedly while the worker is in `state`. Must return `self.emit(event)` to trigger a transition, or `self.emit("keepalive")` to remain in the current state.
- `@pre(state, event)` — a hook called *before* entering `state` via `event`.
- `@post(state, event)` — a hook called *after* leaving `state` via `event`. Useful for cleanup and error logging.

Show the following complete example. Keep all the comments; they explain the intent:

```python
import time
from loguru import logger
from runsmith.decorators import actor, post
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.supervisor import SyncSupervisor
from runsmith.worker import SyncWorker


class SleepySyncWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    # Called whenever the worker leaves "running" or "terminating" via "error"
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
        # Honor the supervisor's stop signal
        if self.ctx.cmd == "stop":
            return self.emit("terminate")
        time.sleep(1)
        return self.emit("keepalive")

    @actor("terminating")
    def graceful_shutdown(self):
        logger.info(f"[{self.name}] shutting down cleanly")
        return self.emit("complete")


supervisor = SyncSupervisor("my-supervisor", "thread")
supervisor.register_workers(
    SleepySyncWorker("foo"),
    SleepySyncWorker("bar"),
)
supervisor.run()  # blocks until shutdown signal or all workers reach a terminal state
```

After the code block, call out:
- `self.ctx.cmd` is `"stop"` when the supervisor is shutting down. Workers are responsible for honoring it by transitioning toward a terminal state.
- `self.ctx.exception` is populated when an exception escapes an actor method. It is available inside `@post` hooks on error transitions.
- `executor_type` is `"thread"` or `"process"`. Threads share memory; processes provide isolation and bypass the GIL.
- `supervisor.run()` is a blocking call. It returns when all workers finish or when the process receives one of the standard shutdown signals (SIGTERM, SIGINT, SIGQUIT, SIGABRT).


#### Supervise async workers

Explain: Async workers subclass `AsyncWorker[State, Event]` and use `async def` actor/hook methods. The supervisor is `AsyncSupervisor`, which runs each worker as an asyncio task inside a single event loop. The API is otherwise identical.

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

Note: `AsyncSupervisor` has no `executor_type` argument — workers always run as asyncio tasks in the same event loop.


#### Supervisor tree

Explain: Because `SyncSupervisor` is itself a `SyncWorker`, it can be registered as a child of another supervisor. This forms a **supervisor tree**: a hierarchy where each node independently manages and restarts its direct children.

```python
from runsmith.supervisor import SyncSupervisor

root = SyncSupervisor("root", "thread")
child = SyncSupervisor("child", "process")

child.register_workers(worker1, worker2)
root.register_workers(child, worker3)

root.run()
```

Render the resulting tree as a diagram (use a code block):

```
root  (thread)
├── child  (process)
│   ├── worker1  (thread)
│   └── worker2  (thread)
└── worker3  (process)
```

Call out:
- The root supervisor runs in the calling thread. Each unit (supervisor or worker) is launched in the executor type declared on the *parent* supervisor.
- Child supervisors have their own restart quota, independent of leaf workers (configured via `RUNSMITH_SUPERVISOR_RESTART_QUOTA`).
- Mixing `"thread"` and `"process"` executors at any level of the tree is fully supported.


### Advanced

#### Custom FSMs

Explain: Workers default to `DefaultWorkerFSM` (idle → starting → running → terminating → stopped), but any lifecycle can be modeled. Define your own `StateMachine` using a `TransitionTable` — a dict mapping each source state to `{event: target_state}`, or `...` (Ellipsis) to mark it as a terminal state.

```python
from typing import Literal
from runsmith.constraints import HeartbeatTimeout, StateTimeout
from runsmith.state import StateMachine, TransitionTable

WorkerState = Literal["idle", "warming", "processing", "cleanup", "crashed", "stopped"]
WorkerEvent = Literal["preload", "start", "stop", "complete", "error"]

WorkerTransitionTable: TransitionTable[WorkerState, WorkerEvent] = {
    "idle":       {"preload": "warming"},
    "warming":    {"start": "processing", "error": "crashed"},
    "processing": {"stop": "cleanup",     "error": "crashed"},
    "cleanup":    {"complete": "stopped", "error": "crashed"},
    "crashed": ...,
    "stopped": ...,
}

WorkerFSM = StateMachine[WorkerState, WorkerEvent](
    transitions=WorkerTransitionTable,
    initial_event="preload",
    constraints=[
        HeartbeatTimeout(timeout=2, when="processing"),
        StateTimeout(timeout=10, when="cleanup"),
    ],
)
```

Pass the FSM instance to the worker at construction:

```python
supervisor.register_workers(MyWorker("w1", fsm=WorkerFSM))
```

Rules for a valid `StateMachine` (surface as a note/admonition):
- Exactly one state must have no incoming transitions (the initial state, derived automatically).
- At least one terminal state (`...`) must be declared.
- `initial_event` must be a valid outgoing event from the initial state.
- All constraint `when` fields must reference states or transitions that exist in the table.


#### Timeout Constraints

Explain that constraints are what turn an FSM into a *supervised* FSM. The `WorkerStatusEvaluator` compiles them into a schedule of expected activities and declares a worker unhealthy the moment a deadline is missed.

Present the three constraint types as a table:

| Constraint                         | `when` format  | Effect                                                                                                                                                                        |
| ---------------------------------- | -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HeartbeatTimeout(timeout, when)`  | state name     | Worker must return from its actor method (emitting keepalive or a transition) within `timeout` seconds while in this state. Guards against infinite loops and blocking calls. |
| `TransitionTimeout(timeout, when)` | `"src -> tgt"` | The transition hooks (`@pre`/`@post`) and state bookkeeping for this edge must complete within `timeout` seconds. Guards against hooks that hang.                             |
| `StateTimeout(timeout, when)`      | state name     | Worker may not remain in this state for longer than `timeout` seconds total, regardless of heartbeat activity. Guards against states that are reachable but never exited.     |

Note: `HeartbeatTimeout` measures time-per-actor-call; `StateTimeout` measures total elapsed time in a state. A worker stuck in `starting` for 10 seconds will violate `StateTimeout` even if it keeps emitting heartbeats.


#### Lifecycle Hooks

The `@pre` and `@post` decorators register callbacks that fire on FSM transitions. They complement actors by allowing side-effects to be separated from business logic.

```python
class MyWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    @pre("running", "run")
    def before_running(self):
        logger.info(f"[{self.name}] about to enter running state")

    @post("running", "error")
    @post("terminating", "error")
    def on_error(self):
        # self.ctx.exception is set here
        logger.error(f"[{self.name}] error: {self.ctx.exception}")

    @post("terminating", "complete")
    def on_clean_exit(self):
        logger.info(f"[{self.name}] exited cleanly")
```

Rules:
- Multiple decorators can be stacked on one method to handle several transitions.
- Hooks for `AsyncWorker` must be `async def`.
- `@post` hooks on error transitions are the idiomatic place to log exceptions and release resources.


#### The Worker Run Context (`self.ctx`)

Inside any `@actor`, `@pre`, or `@post` method, `self.ctx` is a `WorkerRunContext` providing runtime information:

| Attribute   | Type                       | Description                                                                                               |
| ----------- | -------------------------- | --------------------------------------------------------------------------------------------------------- |
| `cmd`       | `"tick" \| "stop" \| None` | Command from the supervisor. Check `cmd == "stop"` in your main actor to initiate graceful shutdown.      |
| `exception` | `BaseException \| None`    | Populated when an exception escaped the last actor call. Available in `@post` hooks on error transitions. |
| `history`   | `deque[WorkerActivity]`    | Ring buffer (max 100) of recent activities emitted by this worker.                                        |
| `data`      | `Any`                      | Arbitrary worker-owned data slot for carrying state across actor calls.                                   |


#### Cloning Workers for Process Execution

When a supervisor uses `"process"` executors, workers are pickled across process boundaries. If your worker holds constructor arguments or other initialization state, you must override `clone()` so the supervisor can reconstruct it correctly on restart:

```python
class QueueReaderWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    def __init__(self, name: str, queue: MPQueue):
        super().__init__(name)
        self.queue = queue

    def clone(self) -> "QueueReaderWorker":
        return QueueReaderWorker(name=self.name, queue=self.queue)
```

Note: if `__init__` has required arguments and `clone()` is not overridden, the supervisor will raise an error on the first restart attempt.


#### Activity Callbacks

`SyncSupervisor.run()` and `AsyncSupervisor.run()` accept an optional `on_activity` callback. It is called for every `WorkerActivity` event emitted by any worker in the tree — heartbeats and state transitions alike. Use it for telemetry, structured logging, or reactive coordination.

```python
from runsmith.worker import WorkerActivity

def on_activity(activity: WorkerActivity) -> None:
    match activity.kind:
        case "heartbeat":
            logger.debug(f"[{activity.worker_name}] alive")
        case "transition_end":
            src, event, tgt = activity.transition
            logger.info(f"[{activity.worker_name}] {src} --{event}--> {tgt}")

supervisor.run(on_activity=on_activity)
```

`WorkerActivity` fields:

| Field         | Type                                                    | Description                                                       |
| ------------- | ------------------------------------------------------- | ----------------------------------------------------------------- |
| `kind`        | `"heartbeat" \| "transition_begin" \| "transition_end"` | Type of activity.                                                 |
| `worker_name` | `str`                                                   | Name of the originating worker.                                   |
| `transition`  | `tuple[str, str, str] \| None`                          | `(src, event, tgt)` for transition events; `None` for heartbeats. |
| `timestamp`   | `float`                                                 | Monotonic timestamp of the event.                                 |

For `AsyncSupervisor`, `on_activity` may be an `async def` coroutine. Concurrent callback invocations are bounded by `activity_callback_task_queue_size` (default: 16); overflow drops the oldest pending task.


#### Settings

Runsmith reads configuration from environment variables prefixed with `RUNSMITH_`. All settings resolve in priority order: **explicit override → environment variable → field default**.

| Setting                             | Default | Env var                                      | Description                                                                  |
| ----------------------------------- | ------- | -------------------------------------------- | ---------------------------------------------------------------------------- |
| `supervision_interval`              | `0.25`  | `RUNSMITH_SUPERVISION_INTERVAL`              | Seconds between supervisor health-check cycles.                              |
| `worker_restart_quota`              | `3`     | `RUNSMITH_WORKER_RESTART_QUOTA`              | Max restarts for a leaf worker before the supervisor gives up and escalates. |
| `supervisor_restart_quota`          | `3`     | `RUNSMITH_SUPERVISOR_RESTART_QUOTA`          | Max restarts for a child supervisor node.                                    |
| `activity_queue_maxsize`            | `100`   | `RUNSMITH_ACTIVITY_QUEUE_MAXSIZE`            | Max buffered `WorkerActivity` events in the shared queue.                    |
| `activity_callback_task_queue_size` | `16`    | `RUNSMITH_ACTIVITY_CALLBACK_TASK_QUEUE_SIZE` | Max concurrent `on_activity` coroutine tasks in async mode.                  |

Show a quick environment-variable example:

```bash
RUNSMITH_WORKER_RESTART_QUOTA=10 RUNSMITH_SUPERVISION_INTERVAL=0.1 python my_app.py
```

And the programmatic equivalent (useful for tests):

```python
from runsmith.settings import RunsmithSettings

custom = RunsmithSettings(worker_restart_quota=10, supervision_interval=0.1)
```

Note that `RunsmithSettings` is a frozen dataclass — once constructed it is immutable.


### Examples

#### Reluctant Worker

See `examples/stuck_termination.py`.

Demonstrate a `ReluctantWorker` whose `running` actor completely ignores the `stop` command — it just sleeps and emits `keepalive` indefinitely. When the supervisor signals shutdown, the worker never transitions toward `terminating`.

Explain what happens: the default FSM's `TransitionTimeout(timeout=1, when="running -> terminating")` fires after 1 second. The supervisor marks the worker unhealthy and forcibly terminates and restarts it. After exhausting the restart quota, the supervisor tears down the whole tree.

The lesson: **workers do not have to be cooperative for the supervisor to maintain system health**. Runsmith's constraint model catches unresponsive behavior regardless of whether the worker checks `ctx.cmd`.

#### LLM App with FastAPI backend

See `examples/fastapi_llm_worker.py`.

This is a sophisticated example demonstrating Runsmith in a real-world application. A FastAPI HTTP server runs in a `ThreadExecutor`; a mock LLM model worker runs in a `ProcessExecutor`. They communicate via a shared `multiprocessing.Queue`.

Describe the failure scenarios Runsmith handles automatically:
- **Event loop starvation**: if the FastAPI server thread stops serving requests (e.g., blocked by a slow synchronous call), its `HeartbeatTimeout` fires and the server worker is restarted.
- **LLM worker hang**: if the LLM worker gets stuck during inference and stops emitting heartbeats, its `HeartbeatTimeout` on the `running` state fires and the process is killed and restarted.

Both failure modes are detected independently and handled by individual restarts — the rest of the tree continues unaffected.

The takeaway: Runsmith enables **heterogeneous supervision** — workers with entirely different execution models, failure modes, and lifecycle shapes, all governed by the same declarative FSM-and-constraint model.


### API Reference

Auto generated, no manual edition needed.

### CHANGELOG

Auto generated, no manual edition needed.
