# Examples

## Incorporative Worker

This example shows a `ReluctantWorker` whose `running` actor never checks `self.ctx.cmd == "stop"`. It never initiates the `running → terminating` transition.

Once supervisor shutdown begins, Runsmith stops accepting heartbeats — the only expected activity is the worker initiating an immediate state transition. Failed to do so will trigger heartbeat timeout, automatically transitioning the worker into unhealthy state, then the **forceful termination**.

The critical consequence: **the clean-up actor never runs**. Any cleanup logic placed there — closing file handles, flushing buffers, releasing locks — is skipped entirely. This is the same outcome as an unclean crash.

```{admonition} Write cooperative workers
:class: warning

Always check `self.ctx.cmd == "stop"` in your main working actor and emit the transition toward the termination state. This is the only way to guarantee your cleanup path executes on shutdown.
```

```{eval-rst}
.. dropdown:: Example source: ``examples/stuck_termination.py``

   .. literalinclude:: ../../examples/stuck_termination.py
      :language: python
```

## LLM app with FastAPI backend

This example demonstrates heterogeneous supervision in a realistic setup: 

- **Task Producer**: a FastAPI app runs in a process worker, exposing an HTTP API to generate LLM tasks.
- **Task Consumer**: another standalone process worker polling tasks from the shared `multiprocessing.Queue`.

Runsmith handles both classes of failures independently:

- **Event loop starvation**: if the FastAPI worker stops serving the loop (for example due to blocking synchronous work), its heartbeat contract is violated and the worker is restarted.
- **LLM worker hang**: if the inference worker stalls and stops heartbeating in `running`, its heartbeat timeout triggers process termination and restart.

Worker restarts don't interfere with each other. The fastapi app crash does not affect the LLM worker. That is the core benefit of Runsmith's declarative FSM-and-constraint model across mixed execution environments.

```{eval-rst}
.. dropdown:: Example source: ``examples/fastapi_llm_worker.py``

   .. literalinclude:: ../../examples/fastapi_llm_worker.py
      :language: python
```
