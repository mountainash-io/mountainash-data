---
title: Contributing and testing
description: Repository layout, test tiers and the live-database harness.
date: 2026-10-29
---

# Contributing and testing

The mountainash-data repository holds the package source, its tests, runnable examples, a harness for testing against real database servers, and the source of this book. Contributions go through pull requests against `develop`, and Hatch environments run the tests and checks. The branching rules, review process and coding standards are in [CONTRIBUTING.md](https://github.com/mountainash-io/mountainash-data/blob/develop/CONTRIBUTING.md). The full command reference, coverage reports and CI behaviour are in [TESTING.md](https://github.com/mountainash-io/mountainash-data/blob/develop/TESTING.md).

## Repository layout

| Path | Contents |
|---|---|
| `src/mountainash_data/core/` | The `Backend` protocol, inspection model, `Namespace`, errors, the connection factory and the settings layer (`core/settings/`, with auth adapters in `core/settings/adapters/`). |
| `src/mountainash_data/backends/ibis/` | `IbisBackend`, its operations, and the `DialectSpec` registry under `dialects/`. Modules starting with `_` are private. |
| `tests/` | Test tiers (below), shared fixtures in `tests/fixtures/`, helpers in `tests/helpers/`, and live-database configuration in `tests/config/`. |
| `examples/` | Runnable recipes, each an `example.py` that asserts its own behaviour plus a README with the expected output. |
| `scripts/` | The live-database runner (`live_db.py`) and its implementation (`live_db_harness/`). |
| `compose.yaml` | Docker Compose services for the live tier: PostgreSQL, MySQL, Oracle and SingleStoreDB. |
| `docs-site/` | This book: the MkDocs site, editorial brief, chapter plan and package profile. |
| `hatch.toml`, `pyproject.toml`, `pytest.ini` | Environments and scripts, package metadata and extras, and pytest defaults. |
| `.github/workflows/` | CI: pytest, ruff, radon, the on-demand live-database check, releases and the book deployment. |

The development, test and type-check environments install `mountainash-settings`, `mountainash` and `mountainash-auth-client` from sibling checkouts beside this repository (`../mountainash-settings` and so on). CI checks the siblings out under `temp/` instead, choosing a branch with the same name as the one under test when it exists. Changes that span packages are tested by pushing matching branch names to each repository.

<!-- concept:153 -->
## Test tiers

Tests are divided by what they need to run, not by marker. The `unit` and `integration` markers tag tests for selection, but they do not decide whether a test needs a driver or a server.

| Tier | Path | Needs | Run by default |
|---|---|---|---|
| Core | `tests/test_unit/`, `tests/test_integration/` | The package with its `sqlite`, `duckdb` and `polars` extras; SQLite and DuckDB run in memory | Yes (`pytest.ini` sets these as `testpaths`) |
| Optional backend | `tests/test_optional_backends/<backend>/` | One extra driver (`oracle`, `trino` or `bigquery`), no server | No |
| Live backend | `tests/test_live_backends/` | One backend driver and a running server, selected through the live-database harness | No |

The optional tier also has a `mountainash/` cohort, which needs the separate `mountainash` package for dtype bridging and runs only in an environment provisioned for it. In CI, the core job runs on every relevant pull request, each optional backend runs in its own `test_optional.<backend>` environment, and the live tier runs only when triggered by hand.

The Hatch scripts below are defined in `hatch.toml`:

```bash
hatch run test:test                 # core tiers with coverage (JSON, XML, HTML, terminal)
hatch run test:test-quick           # core tiers, no coverage
hatch run test:test-target-quick tests/test_unit/core/test_protocol.py
hatch run test:test-changed-quick   # tests affected by changed files (pytest-picked)
hatch run test_backend.oracle:test-target-quick tests/test_optional_backends/oracle

hatch run ruff:check                # lint src/ (ruff:fix to apply fixes)
hatch run mypy:check                # type-check src/ and tests/
hatch run radon:radon-cc            # cyclomatic complexity
```

Each `test_backend.<backend>` environment adds exactly one backend extra to the core test tools, so a driver installed for one backend never hides a missing dependency in another. The recipes under `examples/` are checked separately by running each `example.py`, as TESTING.md describes. A change that adds a dialect or a settings backend also adds tests in the core tier; [Extending mountainash-data](../14-extending-mountainash-data/index.md) lists which ones.

<!-- concept:154 -->
## The live-database harness

The live tier runs only through the harness runner, `python -m scripts.live_db`, exposed as the `live-db` Hatch script. Every command names a target with `--target`; the runner never guesses one from open ports, environment variables or running processes.

| Command | Effect |
|---|---|
| `status` | Shows the target's configured backends. Never resolves or prints a credential. |
| `check` | Connects to the selected backend to confirm it is reachable, resolving the credential without printing it. |
| `test` | Runs `tests/test_live_backends` against an already-running backend. |
| `up`, `down` | Start or stop one Compose service. |
| `run` | Starts a Compose service, runs the tests and cleans up. |

Configuration is TOML. The tracked `tests/config/live-db.toml` declares the backends the live tests know (`postgres`, `mysql`, `oracle`, `mssql`, `singlestoredb`, `exasol`, `trino`, `pyspark`), each mapped to a settings profile, and one target, `docker`, which uses the `compose` transport with connection and password settings for PostgreSQL, MySQL, Oracle and SingleStoreDB. A personal file at `~/.config/mountainash-data/live-db.toml`, when present, is merged on top and is where targets for external servers belong; `--config PATH` (repeatable) selects files explicitly. Connection values become the backend's settings profile and auth profile, the same objects described in [Connection profiles and authentication](../03-connection-profiles-and-authentication/index.md).

```bash
hatch run test:live-db status --target docker
hatch run test_backend.postgres:live-db run --target docker postgres
hatch run test_backend.postgres:live-db test --target <external-target> postgres
```

The first command runs in the driver-free `test` environment; running it against the tracked configuration prints each configured backend:

```text
TARGET docker: 4 configured
postgres       PASS
mysql          PASS
oracle         PASS
singlestoredb  PASS
```

The live tests create and drop tables, so the harness guards against pointing them at a database that matters. A target with the `direct` transport (any server the harness does not start itself) is refused unless its configuration sets `allow_destructive_tests = true`. The runner hands its selection to pytest through `MOUNTAINASH_LIVE_DB_*` environment variables and sets `MOUNTAINASH_REQUIRE_LIVE_DB=1`, so a backend that cannot be reached fails the run instead of being skipped. Invoked directly with plain `pytest` and no selection, the live fixtures skip.

Run live tests one backend at a time, each in its own `test_backend.<backend>` environment. In CI, the `python-verify-live-db.yml` workflow is started by hand for one of the four Compose backends and runs in the `test_github_live` environment. Backends without a Compose service (MSSQL, Trino, Exasol and PySpark) are verified only against external targets; TESTING.md records the details for each.
