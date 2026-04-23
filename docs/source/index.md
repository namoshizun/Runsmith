# Runsmith

*Supervisor-tree framework for building predictable and resilient programs.*  
*Version: {{ version }}*

```bash
pip install runsmith
```

::::{grid} 3
:gutter: 2
:margin: 4 4 0 0

:::{grid-item-card} Predictable
:class-card: sd-border-0 sd-shadow-sm

Workers are **finite-state machines**. States, transitions, and terminal conditions are declared upfront — no hidden control flow.
:::

:::{grid-item-card} Composable
:class-card: sd-border-0 sd-shadow-sm

Supervisors are workers too. Nest them at any depth and freely mix **thread**, **process**, and **asyncio** execution in one tree.
:::

:::{grid-item-card} Resilient
:class-card: sd-border-0 sd-shadow-sm

Heartbeat, transition, and state-residence timeouts are enforced automatically. Failed workers are **restarted** within configurable quotas.
:::
::::

```{toctree}
:maxdepth: 2
:caption: Documentation
:hidden:

introduction
quickstart
advanced
settings
examples
changelog
api/index
```
