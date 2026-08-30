# Task 5 Report: Transport and Docker Safety

## What I implemented

- Added `scripts/live_db_harness/transports.py`.
  - `check_transport()` dispatches policy from the selected target transport.
  - Direct targets require a local TCP listener and skip SSH process-identity rules.
  - SSH-tunnel targets resolve the listener PID, require the exact `launchd -> autossh -> ssh` ancestry, verify the SSH `-L local_port:remote_host:remote_port` tuple and destination, and verify the launchd job PID belongs to the expected `autossh` ancestor.
  - Launchd inspection uses only `launchctl print gui/<uid>/<label>`.
  - External target ports are compared with resolved Compose published TCP ports.
  - `ComposeInspector` reads `docker compose --profile <profile> config --format json`, extracts published TCP ports from the selected service, and reads service state with the required status-scoped `ps` command.
  - Compose startup preflight rejects unrelated listeners on required published ports and treats a running service as pre-existing.
- Added `scripts/live_db_harness/docker.py`.
  - `DockerServiceLease` records pre-start state before startup.
  - Startup uses only `docker compose --profile <profile> up -d --wait <service>`.
  - Post-start inspection records ownership only when the service was absent before and exists after the attempt.
  - Owned cleanup independently attempts service-scoped `stop` and `rm -f`.
  - Cleanup errors are combined into one redacted `HarnessError`.
  - Context-manager and `run()` paths clean up after pass, test failure, startup failure, timeout, and interruption.
  - Pre-existing services are never removed.
- Added all required transport and Docker lifecycle tests with fake listener/process inspectors, Compose inspectors, and command runners.

## What I tested and results

Focused command:

```text
hatch run test:test-target-quick tests/test_unit/live_db/test_transports.py tests/test_unit/live_db/test_docker.py -q
```

Final result:

```text
19 passed in 0.07s
```

Scoped Ruff command:

```text
hatch run ruff:check scripts/live_db_harness/transports.py scripts/live_db_harness/docker.py tests/test_unit/live_db/test_transports.py tests/test_unit/live_db/test_docker.py
```

Result:

```text
All checks passed!
```

`git diff --cached --check` completed without output before commit. The four implementation/test files are committed; this required report remains as the only intentionally untracked artifact.

## TDD Evidence

### RED

Before the production modules existed, the required focused command failed during test collection because both imports were missing:

```text
hatch run test:test-target-quick tests/test_unit/live_db/test_transports.py tests/test_unit/live_db/test_docker.py -q
```

Observed failure:

```text
ModuleNotFoundError: No module named 'scripts.live_db_harness.transports'
ModuleNotFoundError: No module named 'scripts.live_db_harness.docker'
2 errors in 0.17s
```

### GREEN

After implementing both modules, the focused command passed:

```text
...................                                                      [100%]
19 passed in 0.07s
```

The same focused tests and scoped Ruff check were rerun after the commit.

## Files changed

- `scripts/live_db_harness/transports.py`
- `scripts/live_db_harness/docker.py`
- `tests/test_unit/live_db/test_transports.py`
- `tests/test_unit/live_db/test_docker.py`
- This report file

## Self-review findings

- All eight required SSH/transport test functions are present:
  - `test_ssh_identity_accepts_exact_label_destination_tuple_and_ancestry`
  - `test_ssh_identity_rejects_wrong_launchd_label`
  - `test_ssh_identity_rejects_wrong_destination`
  - `test_ssh_identity_rejects_wrong_remote_tuple`
  - `test_ssh_identity_rejects_wrong_ancestry`
  - `test_other_ssh_listener_on_same_port_is_rejected`
  - `test_direct_target_does_not_require_process_identity`
  - `test_external_port_equal_to_compose_port_is_rejected`
- All eleven required Docker lifecycle test functions are present.
- The launchd PID comparison is against the `autossh` ancestor, not the launchd parent process.
- No source command contains a `docker compose down` subcommand.
- No launchd mutation command (`bootstrap`, `bootout`, `enable`, or `disable`) is used.
- SSH and Compose diagnostics avoid printing command environments or credential values; cleanup exception text is passed through the configured redactor.
- Service lifecycle commands are always scoped to the selected service.
- Final focused tests and scoped lint pass.

## Issues or concerns

- No project-wide suite was run because this task's required focused command is the relevant validation; aggregate validation remains with the integration task owner.

## Commit

`bb93e38 feat(test): enforce live transport and Docker safety`

## Fix round 1

### Changes

- Fixed the Python 3.10/3.11-incompatible nested f-string in `transports.py` by extracting the ancestry chain into `ancestry_text` before interpolation. Reviewed the Task 5 implementation f-strings; no other same-quote nested f-strings remain.
- Updated `ComposeInspector.service_state()` to query service existence with `docker compose ps --all --services <service>` separately from running state with `docker compose ps --status running --services <service>`. A stopped but present service now reports `exists=True, running=False`.
- Kept Docker ownership tied to pre-start absence (`before.exists is False`), so starting a pre-existing stopped service does not make the lease remove it.
- When a successful `up` has an ambiguous or non-running post-start observation for a previously absent service, `DockerServiceLease.start()` now marks the service as owned, attempts stop and removal, and preserves cleanup failures as the raised error's cause.
- Added regression coverage for real `ComposeInspector.service_state()` command/output parsing, stopped pre-existing service preservation, and cleanup after ambiguous post-start state. Updated the existing absent-after-start expectation for the required cleanup.

### Verification

Focused tests:

```text
hatch run test:test-target-quick tests/test_unit/live_db/test_transports.py tests/test_unit/live_db/test_docker.py -q
......................                                                   [100%]
22 passed in 0.07s
```

Ruff:

```text
hatch run ruff:check
All checks passed!
```

Older-interpreter syntax parse:

```text
which python3.10 python3.11
/Users/nathanielramm/.local/bin/python3.10
/Users/nathanielramm/.local/bin/python3.11

python3.10 -c "import ast; ast.parse(open('scripts/live_db_harness/transports.py').read()); ast.parse(open('scripts/live_db_harness/docker.py').read()); print('Python 3.10 parse: OK')"
Python 3.10 parse: OK
```
