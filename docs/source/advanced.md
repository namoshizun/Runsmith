# Advanced

## Custom FSMs

In addition to the provided `DefaultWorkerFSM` (`idle -> starting -> running -> terminating -> stopped`). You may build your own `StateMachine` where: 

- State transition is expressed with a `TransitionTable`. 
- Initial event must be declared to start off the state machine 
- Constraints are optional but highly recommended, otherwise the worker is actually unsupervised.

```python
from typing import Literal
from runsmith import HeartbeatTimeout, StateMachine, StateTimeout, TransitionTable, TransitionTimeout

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
        TransitionTimeout(timeout=0.5, when="processing -> cleanup"),
    ],
)
```

Pass `fsm` to worker constructor:

```python
supervisor.register_workers(MyWorker("w1", fsm=WorkerFSM))
```

```{note}
A valid `StateMachine` must satisfy all of the following:

- Exactly one state has no incoming edges (derived initial state).
- At least one terminal state is declared with `...`.
- `initial_event` is an outgoing event from the initial state.
- Every constraint `when` target references an existing state or transition.
```

## Timeout constraints

**Constraints are what turn an FSM into a supervised working unit** . Under the hood, `WorkerStatusEvaluator` compiles constraints into expected-activity deadlines and marks a worker unhealthy as soon as a deadline is missed.

| Constraint          | `when`       | Effect                                                                                                     | Guards against                                                                           |
| ------------------- | ------------ | ---------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `HeartbeatTimeout`  | state name   | Worker must return from its actor method (emitting keepalive or a transition) within `timeout` seconds.    | Silent stalls where the worker appears alive but stops making meaningful progress.       |
| `TransitionTimeout` | `src -> tgt` | Transition hooks (`@pre`/`@post`) and transition bookkeeping for this edge must complete within `timeout`. | Lifecycle edges that begin but never complete, breaking deterministic state progression. |
| `StateTimeout`      | state name   | Worker may not remain in this state longer than `timeout` seconds total, regardless of heartbeat activity. | Indefinite residency in non-terminal states, even if heartbeats continue.                |


```{admonition} Subtle difference
:class: tip

- `HeartbeatTimeout` caps the duration of **each actor invocation** in a state.
- `StateTimeout` caps the **total time spent in the state**, regardless of how many heartbeats are emitted.
- Use `StateTimeout` for setup/teardown states where periodic heartbeats don't make sense.

```

## Lifecycle hooks

`@pre` and `@post` decorators register callbacks that run on transitions. They keep side effects and bookkeeping out of core actor logic.

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

- You can stack decorators to handle multiple transitions with one method.
- `AsyncWorker` hooks must be defined with `async def`.

## Worker run context

Inside any `@actor`, `@pre`, or `@post` method, `self.ctx` exposes a `WorkerRunContext`:

| Attribute   | Type                       | Description                                                                       |
| ----------- | -------------------------- | --------------------------------------------------------------------------------- |
| `cmd`       | `"tick" \| "stop" \| None` | Supervisor command. Check `cmd == "stop"` to start graceful shutdown.             |
| `exception` | `BaseException \| None`    | Set when the last actor raised. Available in `@post` hooks for error transitions. |
| `history`   | `deque[tuple[str, str]]`   | Ring buffer (max 100) of recent transition tuples `(event, state)`.               |
| `data`      | `Any`                      | Arbitrary worker-owned payload for carrying state across actor calls.             |

## Cloning workers for process execution

When a supervisor uses `"process"` executors, workers are pickled across process boundaries. If your worker has required constructor arguments or external state, override `clone()` so restarts can rebuild it correctly.

```python
class QueueReaderWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    def __init__(self, name: str, queue: MPQueue):
        super().__init__(name)
        self.queue = queue

    def clone(self) -> "QueueReaderWorker":
        return QueueReaderWorker(name=self.name, queue=self.queue)
```

If `__init__` has required arguments and `clone()` is missing, restart attempts fail.

## Activity callbacks

`SyncSupervisor.run()` and `AsyncSupervisor.run()` accept an optional `on_activity` callback that receives every `WorkerActivity` event emitted by the supervisor. It makes a clean integration point for service-level liveness signals (for example `systemd` watchdog pings) and telemetry pipelines

```python
from runsmith import WorkerActivity

def on_activity(activity: WorkerActivity) -> None:
    match activity.kind:
        case "heartbeat":
            report_service_alive()

supervisor.run(on_activity=on_activity)
```
