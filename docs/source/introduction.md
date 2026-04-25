# Introduction

Not every Python service is a web app. Many systems are composed of multiple independently running units — think of an ETL service with a data poller, a transformer, and a result notifier, each with its own lifecycle, failure modes, and recovery needs. Wiring this by hand with retry loops, watchdog threads, and scattered state flags brittle glue code that is hard to reason about.

Runsmith brings structure to this problem. Each unit becomes a **worker** with an explicit FSM lifecycle. A supervisor tree monitors every worker continuously — detecting stalls and timeouts, not just crashes — and confines restarts to the failed unit so the rest of the system keeps running.

```{admonition} Design pillars
:class: tip

- **Predictable**: worker lifecycles are finite-state machines declared upfront with plain Python types. No hidden control flow.
- **Composable**: supervisors are workers too — nest them at any depth, freely mixing thread, process, and asyncio execution in one tree.
- **Resilient**: heartbeat, transition, and state-residence timeouts are enforced automatically. Failed workers are restarted within configurable quotas.
```

## Another `supervisord`?

Runsmith and supervisord solve different problems. `supervisord` is an OS-level process control daemon that manages external programs by PID and static config. Runsmith is an in-process, programmable and opinionated Python library where the supervised unit is a typed worker with an explicit lifecycle.

That gives a few advantages not present in `supervisord`:

- **Rich concurrency models**: beyond process-only orchestration, workers can run in threads or co-routines
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
