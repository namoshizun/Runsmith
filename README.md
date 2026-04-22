<div align="center">
  <img src="docs/logo.svg" alt="Runsmith Logo" width="170"/>

### Supervisor-tree framework for building predictable and resilient programs.

</div>

<p float="left">
  <img src="https://github.com/namoshizun/Runsmith/actions/workflows/deploy-docs.yml/badge.svg?branch=main" />
  <img src="https://github.com/namoshizun/Runsmith/actions/workflows/tests.yml/badge.svg?branch=main&event=push" /> 
</p>


Runsmith is a Python 3.10+ library for building resilient, long-running programs. Workers are modeled as **finite-state machines** and run in threads, OS processes, or asyncio tasks. A supervisor monitors them continuously, detects failures through typed constraints, and restarts them automatically within configurable quotas.

**Predictable** — every worker lifecycle is declared upfront as an FSM. No hidden control flow.

**Composable** — supervisors are workers too. Nest them at any depth, freely mixing thread, process, and asyncio execution in one tree.

**Resilient** — heartbeat, transition, and state-residence timeouts are enforced automatically. Failed workers are restarted; the rest of the tree stays healthy.

## Install

```bash
pip install runsmith
```

## Documentation

Full documentation, quickstart guide, and examples at **https://namoshizun.github.io/Runsmith**.
