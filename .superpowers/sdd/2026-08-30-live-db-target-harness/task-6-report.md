# Task 6 Report: Runner, Aggregation, and CLI

## What I implemented

- Added `LiveDbRunner` and `run_many()`.
  - Wires unresolved configuration, backend selection, locks, transport checks, authenticated `IbisBackend` checks, Compose leases, pytest, cleanup, and result aggregation.
  - Public backend operations acquire one target/backend lock. The `run` path keeps its lease and connection check inside that lock without reacquiring it.
  - `check` performs configuration, destructive-policy, transport, connection, and close phases in order.
  - `test` accepts running/pre-provisioned Compose services without applying the startup listener-collision rule, then checks connectivity before pytest.
  - `up` and `down` are Compose-only. `down` independently attempts service-scoped `stop` and `rm`.
  - `run` starts a selected service, runs the connection check and pytest, and closes runner-owned resources while preserving an earlier pytest failure when cleanup also fails.
- Added credential-free pytest handoff.
  - Removes all `IBIS_TEST_*` and harness context keys from the child environment.
  - Injects only the four documented invocation keys and preserves the selected configuration-file order.
  - Uses the target timeout unless a positive CLI `--timeout` override is supplied.
- Added bounded aggregate scheduling.
  - Defaults to one worker; otherwise uses `min(--jobs, target.max_parallel)`.
  - Submits only the effective number of jobs at a time and schedules the next backend after completion.
  - Continues after failures by default, supports `--fail-fast`, preserves configured order, and reports `PASS`, `FAIL`, `UNAVAILABLE`, and `NOT_RUN`.
  - Handles aggregate interruption by stopping new submissions, terminating active command processes, waiting for worker cleanup, and returning 130.
- Added `build_parser()` and `main()`.
  - Requires `--target` on every command.
  - Supports `status`, `check`, `up`, `test`, `down`, and `run`, with the required backend/`--all`, `--config`, `--jobs`, `--fail-fast`, `--wait-lock`, and `--timeout` validation.
  - Explicit config paths are absolute, ordered, and checked before runner construction; defaults come from `default_config_files()`.
  - Configuration errors return usage status 2; attempted backend failures return nonzero; interruptions return 130.
- Added the exact thin `scripts/live_db.py` module wrapper.

## What I tested and results

Focused Task 6 tests:

```text
hatch run test:test-target-quick tests/test_unit/live_db/test_runner.py tests/test_unit/live_db/test_cli.py -q
......                                                                   [100%]
6 passed in 0.09s
```

Scoped lint:

```text
hatch run ruff:check scripts/live_db_harness/runner.py scripts/live_db_harness/cli.py scripts/live_db.py tests/test_unit/live_db/test_runner.py tests/test_unit/live_db/test_cli.py
All checks passed!
```

All live database unit tests:

```text
hatch run test:test-target-quick tests/test_unit/live_db -q
74 passed in 1.60s
```

Status smoke test:

```text
hatch run test:python -m scripts.live_db status --target docker
TARGET docker: 3 configured
postgres       PASS
mysql          PASS
oracle         PASS
```

Supported syntax and module compilation also passed for Python 3.10 parsing and `py_compile`.

## TDD Evidence

### RED

Before the Task 6 production modules existed, the required focused command failed during test collection:

```text
hatch run test:test-target-quick tests/test_unit/live_db/test_runner.py tests/test_unit/live_db/test_cli.py -q
```

Observed failures:

```text
ModuleNotFoundError: No module named 'scripts.live_db_harness.runner'
ModuleNotFoundError: No module named 'scripts.live_db_harness.cli'
2 errors in 0.14s
```

### GREEN

After implementing the modules and the entry point, the focused tests passed (6 passed), the complete existing live database unit directory passed (74 passed), and the status command reported all three configured Docker suites without connecting or resolving credentials.

## Files changed

- `scripts/live_db.py`
- `scripts/live_db_harness/runner.py`
- `scripts/live_db_harness/cli.py`
- `tests/test_unit/live_db/test_runner.py`
- `tests/test_unit/live_db/test_cli.py`
- `.superpowers/sdd/2026-08-30-live-db-target-harness/task-6-report.md`

## Self-review findings

- All required command forms are represented by the parser, with explicit target enforcement.
- `--config` is defined only on the root parser. Explicit paths replace defaults, remain in repeated-argument order, and are resolved before runner construction.
- `status` uses the unresolved settings load and validates registered backend/auth profile classes and allowlists without registering a secret provider or constructing an authenticated selection.
- Compose lifecycle commands use only service-scoped commands and never `docker compose down`.
- Aggregate scheduling is completion-driven and never submits more than the effective limit.
- Unavailable suites are rendered with their backlog item and do not affect the aggregate exit status.
- `NOT_RUN` is emitted only for runnable work withheld by fail-fast.
- Child pytest environments never receive complete environments in diagnostics and remove the legacy `IBIS_TEST_*` contract.

## Issues or concerns

- The focused tests added in this task cover the core parser, environment, and scheduler seams; the broader hidden acceptance tests remain the authoritative coverage for every named aggregate failure/cleanup scenario.

## Commit

`5163567 feat(test): add explicit live database runner`
 
## Coverage completion
 
The follow-up coverage pass added all 18 previously missing named tests, bringing the required Task 6 set to 23 named tests (plus the credential-free pytest environment test). The tests exercise:
 
- Parser usage errors for Compose-only commands, unavailable direct backends, required `--all` scheduling options, and backend operation lock waits.
- Explicit configuration replacement, repeated-path ordering, absolute path resolution, and missing-file rejection.
- Ordered aggregate rendering, default continuation, bounded completion-driven scheduling with synchronization events, fail-fast admission and `NOT_RUN` results, unavailable exit semantics, cleanup error precedence, and interrupt termination/cleanup.
 
Evidence:
 
```text
hatch run test:test-target-quick tests/test_unit/live_db/test_runner.py tests/test_unit/live_db/test_cli.py -q
........................                                                 [100%]
24 passed in 0.19s

hatch run ruff:check
All checks passed!
```
