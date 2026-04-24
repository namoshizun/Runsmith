<div align="center">
  <img src="docs/logo.svg" alt="Runsmith Logo" width="170"/>

### Supervisor-tree framework for building predictable and resilient programs.

</div>

<p float="left">
  <img src="https://github.com/namoshizun/Runsmith/actions/workflows/deploy-docs.yml/badge.svg?branch=main" />
  <img src="https://github.com/namoshizun/Runsmith/actions/workflows/tests.yml/badge.svg?branch=main&event=push" />
  <a href="https://codecov.io/gh/namoshizun/Runsmith"><img src="https://codecov.io/gh/namoshizun/Runsmith/graph/badge.svg?branch=main" alt="codecov coverage" /></a>
</p>


**Not every Python service is a web app. Many systems are composed of multiple independently running units** — think of an ETL service with a data poller, a transformer, and a result notifier, each with its own lifecycle, failure modes, and recovery needs. Wiring this by hand with retry loops, watchdog threads, and scattered state flags brittle glue code that is hard to reason about.

Runsmith brings structure to this problem. Each unit becomes a **worker** with an explicit FSM lifecycle. A **supervisor** tree monitors every worker continuously — detecting stalls and timeouts, not just crashes — and confines restarts to the failed unit so the rest of the system keeps running.

**Predictable** — every worker lifecycle is declared upfront as an FSM. No hidden control flow.

**Composable** — supervisors are workers too. Nest them at any depth, freely mixing thread, process, and asyncio execution in one tree.

**Resilient** — heartbeat, transition, and state-residence timeouts are enforced automatically. Failed workers are restarted within configurable quotas.

## Install

```bash
pip install runsmith
```

## Documentation

Full documentation, quickstart guide, and examples at **https://runsmith.lu-d.com**.
