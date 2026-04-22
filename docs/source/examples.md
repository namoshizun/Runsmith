# Examples

## Reluctant Worker

This example shows a `ReluctantWorker` whose `running` actor ignores `self.ctx.cmd == "stop"` and keeps sleeping, then emitting `keepalive`. During shutdown, the worker never moves toward `terminating`.

The default FSM includes `TransitionTimeout(timeout=1, when="running -> terminating")`. Once shutdown starts, the supervisor expects that transition promptly. Because it never completes, the worker is marked unhealthy, terminated, and restarted. After restart quota exhaustion, the supervisor tears down the tree.

The key lesson is that workers do not need to be cooperative for supervision to remain effective. Runsmith enforces liveness contracts through constraints, even when actor logic does not check stop commands.

```{eval-rst}
.. dropdown:: Example source: ``examples/stuck_termination.py``

   .. literalinclude:: ../../examples/stuck_termination.py
      :language: python
```

## LLM app with FastAPI backend

This example demonstrates heterogeneous supervision in a realistic setup: a FastAPI HTTP worker runs in a thread executor while a mock LLM worker runs in a process executor. They communicate through shared `multiprocessing.Queue` objects.

Runsmith handles both classes of failures independently:

- **Event loop starvation**: if the FastAPI worker stops serving the loop (for example due to blocking synchronous work), its heartbeat contract is violated and the worker is restarted.
- **LLM worker hang**: if the inference worker stalls and stops heartbeating in `running`, its heartbeat timeout triggers process termination and restart.

The rest of the supervision tree remains healthy while individual failed nodes are recycled. That is the core benefit of Runsmith's declarative FSM-and-constraint model across mixed execution environments.

```{eval-rst}
.. dropdown:: Example source: ``examples/fastapi_llm_worker.py``

   .. literalinclude:: ../../examples/fastapi_llm_worker.py
      :language: python
```
