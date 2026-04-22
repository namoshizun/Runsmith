# Introduction

Runsmith is a Python 3.10+ library for building resilient, long-running programs using a supervisor-tree architecture. Workers are **finite-state machines** (FSMs) with explicit states, typed events, and declared terminal conditions. Workers are **backend-agnostic** and Runsmith supports threads, OS processes, and asyncio tasks. Supervisors continuously evaluate health and restart unhealthy workers within configurable quotas.

```{admonition} Design pillars
:class: tip

- **Predictability**: every worker lifecycle is modeled as a finite-state machine. States, transitions, and terminal conditions are declared upfront with plain Python types.
- **Composability**: supervisors are workers too, so you can nest supervision nodes at any depth and mix thread, process, and asyncio execution in one tree.
- **Resilience**: workers emit continuous activity. Constraint violations such as heartbeat timeout, transition timeout, and state residence timeout are detected automatically and recovered via restart quotas.
```

## Another `supervisord`?

Runsmith and supervisord solve different problems. `supervisord` is an OS-level process control daemon that manages external programs by PID and static config. Runsmith is an in-process, programmable Python library where the supervised unit is a typed worker with an explicit lifecycle.

That gives a few advantages not present in `supervisord`:

- **Rich concurrency models**: beyond process-only orchestration, workers can run in threads or co-routines, or even custom execution backends.
- **Fine-grained health probes**: failure is not just an abnormal process exit, but a constraint violation that can be detected and recovered from.
- **Supervisor-tree**: Erlang/OTP style supervisor-tree for nested fault domains.


## The default worker FSM

Default constraints in this FSM:

- **Heartbeat timeout (`keepalive=2s` on `running`)**: the state actor method must emit a heartbeat at least every 2 seconds, or the worker is marked unhealthy.
- **Transition timeout (`1s` on core edges)**: transition hooks methods on each transition path (e.g., `starting -> running`, `running -> terminating`) must complete quickly. 
- **State timeout (`10s` on `starting` and `terminating`)**: a worker cannot remain in these states longer than 10 seconds total, even if heartbeats are on time.

When any constraint is violated, the supervisor restarts the worker (subject to restart quota).

```{mermaid}
stateDiagram-v2
    direction TB

    [*] --> idle : initial
    idle --> starting : start

    state "starting<br/>(state_timeout=10s)" as starting
    state "running<br/>(keepalive=2s)" as running
    state "terminating<br/>(state_timeout=10s)" as terminating

    starting --> running : run
    starting --> crashed : error

    running --> terminating : terminate
    running --> crashed : error

    terminating --> stopped : complete
    terminating --> crashed : error

    crashed --> [*]
    stopped --> [*]
```
