## Changelog

### 1.2.1 (2026-08-05)

- feat: activity queue now applies backpressure — executors block on a full queue
- fix: cap each supervision-tick drain pass so a chatty worker won't starve the supervision loop
- fix: suppress asyncio "task exception was never retrieved" noise for crashed async workers
- fix: fence worker activities by generation to prevent ingesting stale activities from dead workers
- misc: slim test suite, add more critical tests.

### 1.2.0 (2026-08-04)

**Breaking changes**:

- feat: mark terminals with `Terminal.OK` / `Terminal.ERROR` instead of `...`
  (`"stopped": ...` → `Terminal.OK`, `"crashed": ...` → `Terminal.ERROR`)
- refactor: in addition to responding to "stop" command, the worker can proactively terminate. Supervisor will also exit when all workers exited with `Terminal.OK` outcome.
- refactor: leave `running` with `complete` instead of `terminate` (updated default FSM)
- feat: rate-limit actors with `@actor(..., min_interval=...)`
- fix: escalate restart-quota exhaustion to `crashed` so nested parents restart failed children
  supervisors

### 1.1.0 (2026-04-25)

- fix: async supervision did not check executor liveness
- feat: add best-effort thread killing mechanism

### 1.0.0 (2026-04-22)

- Initial release
