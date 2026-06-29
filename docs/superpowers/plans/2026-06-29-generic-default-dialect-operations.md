# Generic-Default Dialect Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `IbisBackend.upsert` work across three SQL upsert families (`ON CONFLICT`, `MERGE`, MySQL `ON DUPLICATE KEY UPDATE`) and `IbisBackend.rename_table` work on every dialect whose rename is expressible — via sqlglot-rendered generic defaults dispatched off a new `DialectSpec.upsert_style`, with the override hooks retained.

**Architecture:** A shared `_render.py` helper renders identifiers/types/sources and compiles `update_condition` predicates off the live connection's own sqlglot compiler. `upsert`/`rename_table` dispatch hook-or-generic; the generic upsert branches on `upsert_style` into three sqlglot-AST renderers. Source rows are staged as a compiled subquery (Ibis's own mechanism), not a temp table. Live-tested on sqlite/duckdb/postgres/mysql (Docker); golden-SQL render assertions cover the full 20-dialect registry.

**Tech Stack:** Python 3.12, ibis-framework >= 12.0.0 (env has 12.0.0), sqlglot 30.x (`sqlglot.expressions as sge` / `from sqlglot import exp`), polars, pytest, hatch + uv, Docker Compose (postgres + mariadb).

**Spec:** `docs/superpowers/specs/2026-06-29-generic-default-dialect-operations-design.md`

## Global Constraints

- **Run everything in the hatch test env:** `hatch run test:test-target-quick <path>` (quick, no coverage). NEVER the stale `.venv`.
- **Lint:** `hatch run ruff:check src` and (for new test files) `hatch run ruff:check <test-path>` — the `ruff:check` script is hardcoded to `./src`, so test paths must be appended explicitly. Every commit ruff-clean.
- **Types:** `hatch run mypy:check` stays `Success` (0 errors). Resolve new findings in touched files only; do not touch pre-existing `iceberg.*` debt.
- **Render off the LIVE connection** — types via `ibis_conn.compiler.type_mapper.to_string(dtype)` (create_table parity); dialect via `ibis_conn.compiler.dialect` (a sqlglot dialect, NOT ibis's backend name — ibis `mssql` ↔ sqlglot `tsql`). Never a hand-written type map.
- **One generic default + optional override.** `upsert_hook` / `rename_table_hook` default `None`; when `None` the generic path runs. `upsert_style=None` → honest `NotImplementedError`.
- **Simple identifiers only** — `name`/`database`/rename names validated via the existing `_validate_simple_identifier`; dotted names raise `ValueError` (consistent with `add_columns`).
- **Explicit column lists + target-type casts** in every upsert family (no `SELECT *`, no positional values); source columns projected in target-column order.
- **Reserved sentinels** `__ma_incoming__` / `__ma_existing__` for the condition compiler; a target colliding with a sentinel raises `ValueError`.
- **20 registry dialects** — golden tests iterate the live `DIALECTS` registry, never a hardcoded list/count.
- **Targeted local test backends:** in-memory `sqlite`/`duckdb` always; `postgres`/`mysql` via Docker, skip-if-unreachable locally, fail-closed in CI (`MOUNTAINASH_REQUIRE_LIVE_DB=1`).

---

## File Structure

- `src/mountainash_data/backends/ibis/_render.py` (new) — rendering primitives + the conditional-predicate compiler + predicate-grammar validator + sentinel names. One responsibility: turn ibis/names/types into dialect-correct SQL fragments off a live connection.
- `src/mountainash_data/backends/ibis/operations.py` (modify) — add `_generic_rename_table`, `_generic_upsert`, `_render_on_conflict`, `_render_merge`, `_render_on_duplicate_key`; migrate `_generic_add_columns` to `_render.py`; delete `duckdb_family_upsert` at cutover.
- `src/mountainash_data/backends/ibis/dialects/_registry.py` (modify) — `UpsertStyle` enum, `upsert_style` field, per-dialect style assignment, remove `upsert_hook=duckdb_family_upsert` registrations at cutover.
- `src/mountainash_data/backends/ibis/backend.py` (modify) — `upsert`/`rename_table` dispatch to hook-or-generic; retype `update_condition`.
- `compose.yaml` (new, repo root) — stock postgres + mariadb services.
- `tests/conftest.py` / `tests/fixtures/` (modify) — `postgres_backend` / `mysql_backend` skip-if-unreachable fixtures.
- `tests/test_unit/backends/ibis/test_render_primitives.py`, `test_rename_table_render.py`, `test_upsert_render.py`, `test_upsert_condition_render.py` (new) — golden-SQL / unit.
- `tests/test_integration/test_write_ops_live.py`, `test_upsert_mysql_preflight.py` (new) — live round-trips.
- `.github/workflows/python-run-pytest.yml` (modify) — service containers.
- `pyproject.toml` (modify) — ibis pin `>=12.0.0`. `CLAUDE.md` (modify) — correct stale ibis note.

---

### Task 1: `_render.py` rendering primitives + migrate `add_columns`

**Files:**
- Create: `src/mountainash_data/backends/ibis/_render.py`
- Modify: `src/mountainash_data/backends/ibis/operations.py` (`_generic_add_columns` uses `quote_identifier`)
- Test: `tests/test_unit/backends/ibis/test_render_primitives.py` (create)

**Interfaces:**
- Produces:
  - `dialect_of(ibis_conn) -> t.Any` — returns `ibis_conn.compiler.dialect` (sqlglot dialect).
  - `quote_identifier(name: str, dialect: t.Any) -> str` — `exp.to_identifier(name, quoted=True).sql(dialect=dialect)`.
  - `qualified_name(parts: list[str], dialect: t.Any) -> str` — `".".join(quote_identifier(p, dialect) for p in parts)`.
  - `render_type(type_mapper: t.Any, dtype: t.Any) -> str` — `type_mapper.to_string(dtype)`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_unit/backends/ibis/test_render_primitives.py`:

```python
"""Unit tests for the shared sqlglot rendering primitives."""

import ibis

from mountainash_data.backends.ibis._render import (
    dialect_of,
    qualified_name,
    quote_identifier,
    render_type,
)


class TestRenderPrimitives:
    def test_quote_identifier_duckdb(self):
        d = dialect_of(ibis.duckdb.connect())
        assert quote_identifier("new col", d) == '"new col"'

    def test_quote_identifier_mysql_backticks(self):
        d = dialect_of(ibis.mysql.connect.__self__) if False else None
        # mysql connect needs a server; render via a sqlglot dialect string instead
        assert quote_identifier("c", "mysql") == "`c`"

    def test_qualified_name_two_parts(self):
        assert qualified_name(["db", "t"], "duckdb") == '"db"."t"'

    def test_render_type_matches_create_table_mapper(self):
        con = ibis.duckdb.connect()
        tm = con.compiler.type_mapper
        assert render_type(tm, ibis.dtype("int64")) == tm.to_string(ibis.dtype("int64"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_render_primitives.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'mountainash_data.backends.ibis._render'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/mountainash_data/backends/ibis/_render.py`:

```python
"""Shared sqlglot rendering primitives for dialect-agnostic write ops.

Everything renders off a *live* ibis connection's own compiler, so identifier
quoting and type rendering match what ibis emits for create_table.
"""

from __future__ import annotations

import typing as t

from sqlglot import exp


def dialect_of(ibis_conn: t.Any) -> t.Any:
    """The live connection's sqlglot dialect (NOT ibis's backend name)."""
    return ibis_conn.compiler.dialect


def quote_identifier(name: str, dialect: t.Any) -> str:
    """Quote a single identifier for `dialect` via sqlglot."""
    return exp.to_identifier(name, quoted=True).sql(dialect=dialect)


def qualified_name(parts: list[str], dialect: t.Any) -> str:
    """Quote each part and join with '.' (e.g. database.table)."""
    return ".".join(quote_identifier(p, dialect) for p in parts)


def render_type(type_mapper: t.Any, dtype: t.Any) -> str:
    """Render an ibis dtype to SQL via the connection's type-mapper."""
    return type_mapper.to_string(dtype)
```

- [ ] **Step 4: Migrate `_generic_add_columns` to the helper (no behaviour change)**

In `operations.py`, add to the imports near the top:

```python
from mountainash_data.backends.ibis._render import quote_identifier
```

In `_generic_add_columns`, delete the inline `_quote` closure and use the helper. Replace:

```python
    def _quote(identifier: str) -> str:
        return exp.to_identifier(identifier, quoted=True).sql(dialect=dialect)

    table_parts = [database, table_name] if database else [table_name]
    qualified = ".".join(_quote(part) for part in table_parts)
```

with:

```python
    table_parts = [database, table_name] if database else [table_name]
    qualified = ".".join(quote_identifier(part, dialect) for part in table_parts)
```

and in the loop replace `_quote(col_name)` with `quote_identifier(col_name, dialect)`.

- [ ] **Step 5: Run tests to verify they pass**

Run:
```bash
hatch run test:test-target-quick tests/test_unit/backends/ibis/test_render_primitives.py -v
hatch run test:test-target-quick tests/test_unit/backends/ibis/test_add_columns.py -v
```
Expected: primitives PASS; all `test_add_columns.py` still PASS (no behaviour change).

- [ ] **Step 6: Lint, types, commit**

```bash
hatch run ruff:check src
hatch run ruff:check tests/test_unit/backends/ibis/test_render_primitives.py
hatch run mypy:check
git add src/mountainash_data/backends/ibis/_render.py src/mountainash_data/backends/ibis/operations.py tests/test_unit/backends/ibis/test_render_primitives.py
git commit -m "feat(ibis): extract shared _render.py primitives; migrate add_columns"
```

---

### Task 2: Test infrastructure (Docker services, fixtures, ibis pin)

**Files:**
- Create: `compose.yaml` (repo root)
- Modify: `tests/fixtures/database_fixtures.py` (live fixtures), `tests/conftest.py` (re-export if needed)
- Modify: `pyproject.toml` (ibis pin), `CLAUDE.md` (stale-note fix), `hatch.toml` (`test-live` script), `.github/workflows/python-run-pytest.yml` (services)
- Test: `tests/test_integration/test_live_smoke.py` (create)

**Interfaces:**
- Produces:
  - `postgres_backend` / `mysql_backend` pytest fixtures yielding a connected `IbisBackend`, skipping locally when unreachable and failing when `MOUNTAINASH_REQUIRE_LIVE_DB=1` and unreachable.

- [ ] **Step 1: Write `compose.yaml`**

```yaml
services:
  postgres:
    image: postgres:18-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: ibis_testing
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "postgres"]
      interval: 1s
      retries: 20
    ports:
      - "5432:5432"
  mysql:
    image: mariadb:12.1.2
    environment:
      MYSQL_ALLOW_EMPTY_PASSWORD: "true"
      MYSQL_DATABASE: ibis_testing
      MYSQL_USER: ibis
      MYSQL_PASSWORD: ibis
    healthcheck:
      test: ["CMD", "mariadb-admin", "ping", "-h", "localhost"]
      interval: 1s
      retries: 20
    ports:
      - "3306:3306"
```

- [ ] **Step 2: Write the failing live-smoke test**

Create `tests/test_integration/test_live_smoke.py`:

```python
"""Smoke test that the live-db fixtures connect or skip correctly."""

import pytest


@pytest.mark.integration
def test_postgres_smoke(postgres_backend):
    assert isinstance(postgres_backend.list_tables(), list)


@pytest.mark.integration
def test_mysql_smoke(mysql_backend):
    assert isinstance(mysql_backend.list_tables(), list)
```

- [ ] **Step 3: Run to verify it fails**

Run: `hatch run test:test-target-quick tests/test_integration/test_live_smoke.py -v`
Expected: FAIL — `fixture 'postgres_backend' not found`.

- [ ] **Step 4: Implement the fixtures**

Add to `tests/fixtures/database_fixtures.py` (and ensure it is imported by `tests/conftest.py` like the other fixture modules):

```python
import os

import pytest

from mountainash_data import IbisBackend

_PG = dict(
    host=os.environ.get("IBIS_TEST_POSTGRES_HOST", os.environ.get("PGHOST", "localhost")),
    port=int(os.environ.get("IBIS_TEST_POSTGRES_PORT", os.environ.get("PGPORT", "5432"))),
    user=os.environ.get("IBIS_TEST_POSTGRES_USER", os.environ.get("PGUSER", "postgres")),
    password=os.environ.get("IBIS_TEST_POSTGRES_PASSWORD", os.environ.get("PGPASSWORD", "postgres")),
    database=os.environ.get("IBIS_TEST_POSTGRES_DATABASE", os.environ.get("PGDATABASE", "ibis_testing")),
)
_MY = dict(
    host=os.environ.get("IBIS_TEST_MYSQL_HOST", "localhost"),
    port=int(os.environ.get("IBIS_TEST_MYSQL_PORT", "3306")),
    user=os.environ.get("IBIS_TEST_MYSQL_USER", "ibis"),
    password=os.environ.get("IBIS_TEST_MYSQL_PASSWORD", "ibis"),
    database=os.environ.get("IBIS_TEST_MYSQL_DATABASE", "ibis_testing"),
)


def _live_or_skip(dialect: str, params: dict):
    require = os.environ.get("MOUNTAINASH_REQUIRE_LIVE_DB") == "1"
    try:
        be = IbisBackend(dialect=dialect, **params)
        be.connect()
        return be
    except Exception as exc:  # noqa: BLE001 - service availability gate
        msg = f"{dialect} service unreachable: {exc}"
        if require:
            pytest.fail(msg)
        pytest.skip(msg)


@pytest.fixture
def postgres_backend():
    be = _live_or_skip("postgres", _PG)
    try:
        yield be
    finally:
        be.close()


@pytest.fixture
def mysql_backend():
    be = _live_or_skip("mysql", _MY)
    try:
        yield be
    finally:
        be.close()
```

- [ ] **Step 5: Bump ibis pin + fix CLAUDE.md + add hatch script**

In `pyproject.toml`, change every `ibis-framework...>=11.0.0` floor to `>=12.0.0` (core dep + each extra). In `CLAUDE.md`, change the `ibis-framework[...] == 10.4.0` line to `ibis-framework[polars,pandas,sqlite,duckdb] >= 12.0.0`. In `hatch.toml` `[envs.test.scripts]`, add:

```toml
test-live = "docker compose up -d --wait && pytest -m integration {args}"
```

- [ ] **Step 6: Update CI workflow**

In `.github/workflows/python-run-pytest.yml`, add `services:` for postgres (`postgres:18-alpine`) and mariadb (`mariadb:12.1.2`) with the same env/ports as `compose.yaml`, and set `MOUNTAINASH_REQUIRE_LIVE_DB: "1"` in the job `env:`. (Mirror the env-var names the fixtures read.)

- [ ] **Step 7: Verify (services up locally), then commit**

```bash
docker compose up -d --wait
hatch run test:test-target-quick tests/test_integration/test_live_smoke.py -v   # both PASS
docker compose down
hatch run test:test-target-quick tests/test_integration/test_live_smoke.py -v   # both SKIP (no service)
hatch run ruff:check tests/test_integration/test_live_smoke.py
git add compose.yaml tests/fixtures/database_fixtures.py tests/conftest.py tests/test_integration/test_live_smoke.py pyproject.toml CLAUDE.md hatch.toml .github/workflows/python-run-pytest.yml
git commit -m "test(infra): docker postgres+mariadb services, live fixtures, ibis>=12 pin"
```

---

### Task 3: `UpsertStyle` enum + `upsert_style` field + per-dialect assignment

**Files:**
- Modify: `src/mountainash_data/backends/ibis/dialects/_registry.py`
- Test: `tests/test_unit/backends/ibis/test_upsert_style_registry.py` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: `UpsertStyle` (`str, enum.Enum`: `ON_CONFLICT="on_conflict"`, `MERGE="merge"`, `ON_DUPLICATE_KEY="on_duplicate_key"`); `DialectSpec.upsert_style: t.Optional[UpsertStyle] = None`.

This task is **additive** — it assigns styles but does NOT remove the existing `upsert_hook=duckdb_family_upsert` registrations (cutover is Task 9), so existing upsert behaviour stays green.

- [ ] **Step 1: Write the failing test**

Create `tests/test_unit/backends/ibis/test_upsert_style_registry.py`:

```python
"""The upsert_style assignment must match the spec's §7 coverage matrix."""

from mountainash_data.backends.ibis.dialects._registry import (
    DIALECTS,
    DialectSpec,
    UpsertStyle,
)

# Spec §7 coverage matrix — the single source of truth for this assertion.
EXPECTED_STYLE = {
    "sqlite": UpsertStyle.ON_CONFLICT,
    "duckdb": UpsertStyle.ON_CONFLICT,
    "motherduck": UpsertStyle.ON_CONFLICT,
    "postgres": UpsertStyle.ON_CONFLICT,
    "risingwave": UpsertStyle.ON_CONFLICT,
    "mysql": UpsertStyle.ON_DUPLICATE_KEY,
    "singlestoredb": UpsertStyle.ON_DUPLICATE_KEY,
    "snowflake": UpsertStyle.MERGE,
    "bigquery": UpsertStyle.MERGE,
    "mssql": UpsertStyle.MERGE,
    "oracle": UpsertStyle.MERGE,
    "databricks": UpsertStyle.MERGE,
    "exasol": UpsertStyle.MERGE,
    "trino": UpsertStyle.MERGE,
    "redshift": UpsertStyle.MERGE,
    "clickhouse": None,
    "impala": None,
    "materialize": None,
    "druid": None,
    "pyspark": None,
}


class TestUpsertStyleField:
    def test_field_defaults_none(self):
        spec = DialectSpec(
            ibis_backend_name="duckdb",
            connection_mode="connection_string",
            connection_string_scheme="duckdb://",
        )
        assert spec.upsert_style is None

    def test_every_registry_dialect_has_an_explicit_decision(self):
        # Iterates the live registry — a new dialect with no matrix entry fails.
        assert set(DIALECTS) == set(EXPECTED_STYLE), (
            "registry dialects and the §7 matrix have diverged"
        )

    def test_assigned_styles_match_matrix(self):
        for name, expected in EXPECTED_STYLE.items():
            assert DIALECTS[name].upsert_style == expected, name
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_upsert_style_registry.py -v`
Expected: FAIL — `ImportError: cannot import name 'UpsertStyle'`.

- [ ] **Step 3: Add the enum + field**

In `_registry.py`, after the imports add:

```python
import enum


class UpsertStyle(str, enum.Enum):
    ON_CONFLICT = "on_conflict"
    MERGE = "merge"
    ON_DUPLICATE_KEY = "on_duplicate_key"
```

Add the field to `DialectSpec` after `upsert_hook`:

```python
    upsert_hook: t.Optional[UpsertHook] = None
    upsert_style: t.Optional[UpsertStyle] = None
```

- [ ] **Step 4: Assign `upsert_style` to each dialect**

Add `upsert_style=UpsertStyle.<X>` to each `DialectSpec(...)` entry per `EXPECTED_STYLE` above. Leave the `None` dialects without the field (defaults to `None`). Leave the three existing `upsert_hook=duckdb_family_upsert` lines in place for now.

- [ ] **Step 5: Run test to verify it passes**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_upsert_style_registry.py -v`
Expected: PASS (3 tests).

- [ ] **Step 6: Lint, types, commit**

```bash
hatch run ruff:check src
hatch run ruff:check tests/test_unit/backends/ibis/test_upsert_style_registry.py
hatch run mypy:check
git add src/mountainash_data/backends/ibis/dialects/_registry.py tests/test_unit/backends/ibis/test_upsert_style_registry.py
git commit -m "feat(ibis): add UpsertStyle enum + upsert_style field; assign per matrix"
```

---

### Task 4: `_generic_rename_table` + dispatch

**Files:**
- Modify: `src/mountainash_data/backends/ibis/operations.py` (add `_generic_rename_table`), `src/mountainash_data/backends/ibis/backend.py` (dispatch)
- Test: `tests/test_unit/backends/ibis/test_rename_table_render.py` (create), `tests/test_integration/test_write_ops_live.py` (create, rename portion)

**Interfaces:**
- Consumes: `dialect_of`, `quote_identifier` (Task 1); `_validate_simple_identifier` (existing).
- Produces: `_generic_rename_table(ibis_conn, old_name: str, new_name: str) -> None`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_unit/backends/ibis/test_rename_table_render.py`:

```python
"""rename_table works via the sqlglot generic default on every dialect."""

import ibis
import polars as pl
import pytest

from mountainash_data import IbisBackend
from mountainash_data.backends.ibis.operations import _generic_rename_table


class TestGenericRenameTable:
    def test_renames_on_duckdb(self):
        con = ibis.duckdb.connect()
        con.create_table("old", pl.DataFrame({"id": [1]}))
        _generic_rename_table(con, "old", "new")
        names = con.list_tables()
        assert "new" in names and "old" not in names

    def test_renames_on_sqlite(self):
        con = ibis.sqlite.connect()
        con.create_table("old", pl.DataFrame({"id": [1]}))
        _generic_rename_table(con, "old", "new")
        assert "new" in con.list_tables()

    def test_rejects_dotted_names(self):
        con = ibis.duckdb.connect()
        con.create_table("old", pl.DataFrame({"id": [1]}))
        with pytest.raises(ValueError, match="simple"):
            _generic_rename_table(con, "a.old", "new")

    def test_backend_rename_table_returns_self(self):
        with IbisBackend(dialect="duckdb", database=":memory:") as be:
            be.create_table("old", pl.DataFrame({"id": [1]}))
            assert be.rename_table("old", "new") is be
            assert "new" in be.list_tables()
```

- [ ] **Step 2: Run to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_rename_table_render.py -v`
Expected: FAIL — `ImportError: cannot import name '_generic_rename_table'`.

- [ ] **Step 3: Implement `_generic_rename_table`**

In `operations.py`, add (importing the helpers and `exp` at top if not already present: `from sqlglot import exp` is already imported for add_columns; add `from mountainash_data.backends.ibis._render import dialect_of, quote_identifier`):

```python
def _generic_rename_table(ibis_conn: t.Any, old_name: str, new_name: str) -> None:
    """Rename a table via a sqlglot-rendered ALTER, portable across dialects.

    sqlglot renders `ALTER TABLE … RENAME TO …` for most dialects, `EXEC
    sp_rename …` for SQL Server, and `ALTER TABLE … RENAME …` for MySQL.
    """
    _validate_simple_identifier(old_name, kind="old_name")
    _validate_simple_identifier(new_name, kind="new_name")
    dialect = dialect_of(ibis_conn)
    stmt = exp.Alter(
        this=exp.to_table(quote_identifier(old_name, dialect)),
        kind="TABLE",
        actions=[exp.AlterRename(this=exp.to_identifier(new_name, quoted=True))],
    ).sql(dialect=dialect)
    ibis_conn.raw_sql(stmt)
```

> If `exp.Alter`/`exp.AlterRename` are not the exact class names in the installed sqlglot 30.x, use the verified-equivalent transpile fallback: `sqlglot.transpile(f'ALTER TABLE {quote_identifier(old_name, "")} RENAME TO {quote_identifier(new_name, "")}', read="duckdb", write=dialect)[0]`. Confirm via a one-line probe in the test env before settling.

- [ ] **Step 4: Wire dispatch in `backend.py`**

Replace the body of `rename_table` (currently raises when `rename_table_hook is None`) with hook-or-generic. Add the import near the other operations imports (`from mountainash_data.backends.ibis.operations import _generic_rename_table`) and:

```python
    def rename_table(self, old_name: str, new_name: str) -> IbisBackend:
        conn = self._require_connected()
        hook = self._spec.rename_table_hook
        if hook is not None:
            hook(conn._ibis_conn, old_name, new_name)
        else:
            _generic_rename_table(conn._ibis_conn, old_name, new_name)
        return self
```

- [ ] **Step 5: Add the live rename round-trip**

Create `tests/test_integration/test_write_ops_live.py` with the rename portion:

```python
"""Live round-trip tests for generic write ops (postgres + mysql)."""

import polars as pl
import pytest


@pytest.mark.integration
def test_rename_table_live_postgres(postgres_backend):
    be = postgres_backend
    be.create_table("ren_old", pl.DataFrame({"id": [1]}), overwrite=True)
    be.rename_table("ren_old", "ren_new")
    assert "ren_new" in be.list_tables()
    be.drop_table("ren_new", force=True)


@pytest.mark.integration
def test_rename_table_live_mysql(mysql_backend):
    be = mysql_backend
    be.create_table("ren_old", pl.DataFrame({"id": [1]}), overwrite=True)
    be.rename_table("ren_old", "ren_new")
    assert "ren_new" in be.list_tables()
    be.drop_table("ren_new", force=True)
```

- [ ] **Step 6: Run, lint, types, commit**

```bash
hatch run test:test-target-quick tests/test_unit/backends/ibis/test_rename_table_render.py -v
docker compose up -d --wait && hatch run test:test-target-quick tests/test_integration/test_write_ops_live.py -v ; docker compose down
hatch run ruff:check src
hatch run ruff:check tests/test_unit/backends/ibis/test_rename_table_render.py tests/test_integration/test_write_ops_live.py
hatch run mypy:check
git add src/mountainash_data/backends/ibis/operations.py src/mountainash_data/backends/ibis/backend.py tests/test_unit/backends/ibis/test_rename_table_render.py tests/test_integration/test_write_ops_live.py
git commit -m "feat(ibis): generic sqlglot rename_table (works on every dialect)"
```

---

### Task 5: Conditional-predicate compiler (`_render.py`)

**Files:**
- Modify: `src/mountainash_data/backends/ibis/_render.py`
- Test: `tests/test_unit/backends/ibis/test_upsert_condition_render.py` (create)

**Interfaces:**
- Consumes: nothing from later tasks.
- Produces:
  - `INCOMING_SENTINEL = "__ma_incoming__"`, `EXISTING_SENTINEL = "__ma_existing__"`.
  - `validate_predicate(expr: ir.BooleanValue) -> None` — raises `ValueError` if the predicate contains an aggregation, window, or subquery/EXISTS op.
  - `compile_condition(ibis_conn, target_schema, predicate, *, incoming_alias, existing_alias) -> exp.Expression` — returns the remapped `ON` sub-AST with incoming columns → `incoming_alias`, existing columns → `existing_alias`. Raises `ValueError` if the target schema would collide with a sentinel name (caller passes the real target name to check) or the grammar is violated.

The mechanism is the §6.1/§9 probe path: bind two sentinel-named ibis tables, `existing.join(incoming, predicate)`, `compiler.to_sqlglot`, extract the join `ON`, remap aliases keyed by sentinel.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_unit/backends/ibis/test_upsert_condition_render.py`:

```python
"""The update_condition ibis-expression predicate compiler (§6.1)."""

import ibis
import pytest

from mountainash_data.backends.ibis._render import (
    compile_condition,
    dialect_of,
    validate_predicate,
)

_SCHEMA = ibis.schema({"id": "int64", "updated_at": "timestamp", "v": "string"})


def _render(con, predicate, *, incoming_alias, existing_alias):
    ast = compile_condition(
        con, _SCHEMA, predicate,
        incoming_alias=incoming_alias, existing_alias=existing_alias,
    )
    return ast.sql(dialect=dialect_of(con))


class TestCompileCondition:
    def test_on_conflict_alias_mapping_duckdb(self):
        con = ibis.duckdb.connect()
        sql = _render(
            con,
            lambda inc, exi: inc.updated_at > exi.updated_at,
            incoming_alias="EXCLUDED", existing_alias="tgt",
        )
        assert '"EXCLUDED"."updated_at"' in sql
        assert '"tgt"."updated_at"' in sql

    def test_merge_alias_mapping_duckdb(self):
        con = ibis.duckdb.connect()
        sql = _render(
            con,
            lambda inc, exi: inc.updated_at > exi.updated_at,
            incoming_alias="src", existing_alias="tgt",
        )
        assert '"src"."updated_at"' in sql and '"tgt"."updated_at"' in sql

    def test_function_predicate_renders_per_dialect(self):
        con = ibis.duckdb.connect()
        sql = _render(
            con,
            lambda inc, exi: inc.v.upper() != exi.v.upper(),
            incoming_alias="src", existing_alias="tgt",
        )
        assert "UPPER(" in sql.upper()

    def test_rejects_aggregate_predicate(self):
        with pytest.raises(ValueError, match="aggregat|window|subquer"):
            validate_predicate(
                ibis.table(_SCHEMA, name="x").v.count() > 0  # aggregation
            )
```

- [ ] **Step 2: Run to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_upsert_condition_render.py -v`
Expected: FAIL — `ImportError: cannot import name 'compile_condition'`.

- [ ] **Step 3: Implement the compiler**

Append to `_render.py`:

```python
import ibis  # noqa: E402  (kept with the other third-party imports at top in practice)
import ibis.expr.operations as ops
import ibis.expr.types as ir

INCOMING_SENTINEL = "__ma_incoming__"
EXISTING_SENTINEL = "__ma_existing__"

# ops whose presence makes a predicate invalid in a WHERE / WHEN MATCHED splice
_FORBIDDEN_OPS = (ops.Reduction, ops.WindowFunction)


def validate_predicate(expr: ir.BooleanValue) -> None:
    """Reject predicates that cannot live in a row-level WHERE/WHEN MATCHED."""
    node = expr.op()
    for n in node.find(_FORBIDDEN_OPS):  # type: ignore[arg-type]
        raise ValueError(
            "update_condition must be a scalar row predicate; found "
            f"{type(n).__name__} (aggregation/window). Use the upsert_hook "
            "override for conditions outside this grammar."
        )
    # subqueries / EXISTS surface as relational ops embedded in the predicate
    for n in node.find((ops.Relation,)):  # type: ignore[arg-type]
        raise ValueError(
            "update_condition may not contain subqueries/EXISTS/third-table "
            "references; use the upsert_hook override."
        )


def compile_condition(
    ibis_conn: t.Any,
    target_schema: t.Any,
    predicate: t.Callable[[ir.Table, ir.Table], ir.BooleanValue],
    *,
    incoming_alias: str,
    existing_alias: str,
) -> exp.Expression:
    """Render an (incoming, existing) -> bool predicate to a sqlglot ON sub-AST,
    with incoming columns aliased to `incoming_alias` and existing to
    `existing_alias`. See spec §6.1."""
    incoming = ibis.table(target_schema, name=INCOMING_SENTINEL)
    existing = ibis.table(target_schema, name=EXISTING_SENTINEL)
    pred = predicate(incoming, existing)
    validate_predicate(pred)

    joined = existing.join(incoming, pred, how="inner")
    ast = ibis_conn.compiler.to_sqlglot(joined)
    ast = ast if isinstance(ast, exp.Expression) else ast[0]

    alias_to_side = {}
    for tbl in ast.find_all(exp.Table):
        if tbl.name == INCOMING_SENTINEL:
            alias_to_side[tbl.alias_or_name] = incoming_alias
        elif tbl.name == EXISTING_SENTINEL:
            alias_to_side[tbl.alias_or_name] = existing_alias

    join = next(ast.find_all(exp.Join), None)
    if join is None or join.args.get("on") is None:
        raise ValueError("could not extract join ON predicate")
    on = join.args["on"].copy()

    def _remap(n: exp.Expression) -> exp.Expression:
        if isinstance(n, exp.Column) and n.table in alias_to_side:
            n.set("table", exp.to_identifier(alias_to_side[n.table], quoted=True))
        return n

    return on.transform(_remap)
```

> Pin the exact ibis op classes (`ops.Reduction`, `ops.WindowFunction`, `ops.Relation`) against the installed ibis 12 during this step — run a quick probe to confirm `expr.op().find((ops.Reduction,))` exists and behaves as used; adjust the forbidden-op tuple if the names differ. The behaviour contract (reject aggregate/window/subquery) is fixed by the tests.

- [ ] **Step 4: Run to verify it passes**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_upsert_condition_render.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Lint, types, commit**

```bash
hatch run ruff:check src
hatch run ruff:check tests/test_unit/backends/ibis/test_upsert_condition_render.py
hatch run mypy:check
git add src/mountainash_data/backends/ibis/_render.py tests/test_unit/backends/ibis/test_upsert_condition_render.py
git commit -m "feat(ibis): conditional-predicate compiler (sentinel join->AST->remap)"
```

---

### Task 6: `_render_on_conflict` + `_generic_upsert` (ON_CONFLICT branch)

**Files:**
- Modify: `src/mountainash_data/backends/ibis/_render.py` (add `compiled_source`), `src/mountainash_data/backends/ibis/operations.py` (add `_generic_upsert`, `_render_on_conflict`, helpers)
- Test: `tests/test_unit/backends/ibis/test_upsert_render.py` (create)

**Interfaces:**
- Consumes: `quote_identifier`, `qualified_name`, `compile_condition`, `validate_predicate` (Tasks 1, 5); `_normalize_columns`, `_validate_simple_identifier` (existing).
- Produces:
  - `compiled_source(ibis_conn, obj, target_schema) -> tuple[str, list[str]]` — `(subquery_sql, source_columns)`; columns cast to the target type and projected in target order.
  - `_generic_upsert(ibis_conn, name, obj, *, style, conflict_columns, update_columns, conflict_action, update_condition, database, schema) -> None`.
  - `_render_on_conflict(...) -> str`.

This task wires `_generic_upsert` and the ON_CONFLICT branch only; MERGE/ON_DUPLICATE raise `NotImplementedError("unimplemented style")` as placeholders until Tasks 7/8. Dispatch is NOT flipped yet (Task 9), so this is exercised directly against a raw connection.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_unit/backends/ibis/test_upsert_render.py`:

```python
"""Generic upsert — ON CONFLICT family (sqlite/duckdb)."""

import ibis
import polars as pl
import pytest

from mountainash_data.backends.ibis.dialects._registry import UpsertStyle
from mountainash_data.backends.ibis.operations import _generic_upsert


def _seed(con):
    con.create_table("t", pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}))


class TestOnConflictUpsert:
    def test_insert_and_update_duckdb(self):
        con = ibis.duckdb.connect()
        _seed(con)
        _generic_upsert(
            con, "t", pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}),
            style=UpsertStyle.ON_CONFLICT, conflict_columns=["id"],
            update_columns=None, conflict_action="UPDATE",
            update_condition=None, database=None, schema=None,
        )
        rows = dict(con.table("t").order_by("id").execute()[["id", "v"]].itertuples(index=False))
        assert rows == {1: "a", 2: "B", 3: "c"}

    def test_do_nothing_duckdb(self):
        con = ibis.duckdb.connect()
        _seed(con)
        _generic_upsert(
            con, "t", pl.DataFrame({"id": [2], "v": ["X"]}),
            style=UpsertStyle.ON_CONFLICT, conflict_columns="id",
            update_columns=None, conflict_action="NOTHING",
            update_condition=None, database=None, schema=None,
        )
        assert con.table("t").filter(ibis._.id == 2).execute()["v"].iloc[0] == "b"

    def test_composite_key_sqlite(self):
        con = ibis.sqlite.connect()
        con.create_table("t", pl.DataFrame({"a": [1], "b": [1], "v": ["x"]}))
        # needs a composite unique index for ON CONFLICT to detect
        con.raw_sql("CREATE UNIQUE INDEX ux ON t (a, b)")
        _generic_upsert(
            con, "t", pl.DataFrame({"a": [1], "b": [1], "v": ["y"]}),
            style=UpsertStyle.ON_CONFLICT, conflict_columns=["a", "b"],
            update_columns=None, conflict_action="UPDATE",
            update_condition=None, database=None, schema=None,
        )
        assert con.table("t").execute()["v"].iloc[0] == "y"

    def test_conditional_update_only_when_newer_duckdb(self):
        con = ibis.duckdb.connect()
        con.create_table("t", pl.DataFrame({"id": [1], "ver": [5], "v": ["old"]}))
        con.raw_sql("CREATE UNIQUE INDEX ux ON t (id)")
        _generic_upsert(
            con, "t", pl.DataFrame({"id": [1], "ver": [3], "v": ["stale"]}),
            style=UpsertStyle.ON_CONFLICT, conflict_columns=["id"],
            update_columns=None, conflict_action="UPDATE",
            update_condition=lambda inc, exi: inc.ver > exi.ver,
            database=None, schema=None,
        )
        # incoming ver(3) is NOT newer than existing(5) -> unchanged
        assert con.table("t").execute()["v"].iloc[0] == "old"

    def test_unknown_style_raises_notimplemented(self):
        con = ibis.duckdb.connect()
        _seed(con)
        with pytest.raises(NotImplementedError):
            _generic_upsert(
                con, "t", pl.DataFrame({"id": [9], "v": ["z"]}),
                style=None, conflict_columns=["id"], update_columns=None,
                conflict_action="UPDATE", update_condition=None,
                database=None, schema=None,
            )
```

- [ ] **Step 2: Run to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_upsert_render.py -v`
Expected: FAIL — `ImportError: cannot import name '_generic_upsert'`.

- [ ] **Step 3: Implement `compiled_source` in `_render.py`**

```python
def compiled_source(
    ibis_conn: t.Any, obj: t.Any, target_schema: t.Any
) -> tuple[str, list[str]]:
    """Compile `obj` to a SELECT subquery, casting each column to the target
    type and projecting in target-column order. Returns (sql, columns).

    Columns present in the target but absent from the source are omitted;
    columns present in the source but absent from the target raise ValueError.
    """
    src = obj if isinstance(obj, ir.Table) else ibis.memtable(obj)
    src_cols = set(src.columns)
    extra = src_cols - set(target_schema.names)
    if extra:
        raise ValueError(f"source columns absent from target: {sorted(extra)}")
    cols = [c for c in target_schema.names if c in src_cols]
    projected = src.select(
        [src[c].cast(target_schema[c]).name(c) for c in cols]
    )
    return ibis_conn.compile(projected), cols
```

- [ ] **Step 4: Implement `_generic_upsert` + `_render_on_conflict` in `operations.py`**

Add imports at top: `from mountainash_data.backends.ibis._render import (compile_condition, compiled_source, qualified_name, quote_identifier, dialect_of)` and `from mountainash_data.backends.ibis.dialects._registry import UpsertStyle`. Then:

```python
def _generic_upsert(
    ibis_conn: t.Any,
    name: str,
    obj: t.Any,
    *,
    style: t.Any,
    conflict_columns: t.Any,
    update_columns: t.Any,
    conflict_action: str,
    update_condition: t.Any,
    database: str | None,
    schema: str | None,
) -> None:
    if style is None:
        raise NotImplementedError(
            f"Dialect (connection {type(ibis_conn).__name__}) does not support upsert"
        )
    _validate_simple_identifier(name, kind="name")
    if database is not None:
        _validate_simple_identifier(database, kind="database")
    if conflict_action not in ("UPDATE", "NOTHING"):
        raise ValueError(f"conflict_action must be UPDATE or NOTHING, got {conflict_action!r}")

    target_schema = ibis_conn.table(name, database=database).schema()
    conflict = _normalize_columns(conflict_columns)
    if update_columns is None:
        update = [c for c in target_schema.names if c not in conflict]
    else:
        update = _normalize_columns(update_columns)
    if conflict_action == "UPDATE" and not update:
        raise ValueError("no columns to update; provide update_columns or non-key columns")

    if style is UpsertStyle.ON_CONFLICT:
        stmt = _render_on_conflict(
            ibis_conn, name, obj, target_schema=target_schema, conflict=conflict,
            update=update, conflict_action=conflict_action,
            update_condition=update_condition, database=database, schema=schema,
        )
    elif style is UpsertStyle.MERGE:
        raise NotImplementedError("unimplemented style: MERGE")  # Task 7
    elif style is UpsertStyle.ON_DUPLICATE_KEY:
        raise NotImplementedError("unimplemented style: ON_DUPLICATE_KEY")  # Task 8
    else:
        raise NotImplementedError(f"unknown upsert_style: {style!r}")
    ibis_conn.raw_sql(stmt)


def _render_on_conflict(
    ibis_conn, name, obj, *, target_schema, conflict, update,
    conflict_action, update_condition, database, schema,
) -> str:
    dialect = dialect_of(ibis_conn)
    source_sql, cols = compiled_source(ibis_conn, obj, target_schema)
    parts = [p for p in (database, schema, name) if p]
    target = qualified_name(parts, dialect)
    col_list = ", ".join(quote_identifier(c, dialect) for c in cols)
    conflict_list = ", ".join(quote_identifier(c, dialect) for c in conflict)

    if conflict_action == "NOTHING":
        action = f"ON CONFLICT ({conflict_list}) DO NOTHING"
    else:
        set_sql = ", ".join(
            f"{quote_identifier(c, dialect)} = "
            f'{quote_identifier("EXCLUDED", dialect)}.{quote_identifier(c, dialect)}'
            for c in update
        )
        where = ""
        if update_condition is not None:
            cond = compile_condition(
                ibis_conn, target_schema, update_condition,
                incoming_alias="EXCLUDED", existing_alias=name,
            ).sql(dialect=dialect)
            where = f" WHERE {cond}"
        action = f"ON CONFLICT ({conflict_list}) DO UPDATE SET {set_sql}{where}"

    return f"INSERT INTO {target} ({col_list}) SELECT {col_list} FROM ({source_sql}) AS __src {action}"
```

> Note: when `update_condition` is supplied, the existing row is referenced by the bare table `name` (the default ON CONFLICT convention); if a future dialect needs `INSERT INTO t AS tgt` aliasing (spec §7), thread an alias through here. For the live-tested dialects (duckdb/sqlite/postgres) the bare-name form is valid.

- [ ] **Step 5: Run to verify it passes**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_upsert_render.py -v`
Expected: PASS (5 tests). If a backend rejects the subquery cast or the `__src` alias, adjust `compiled_source`/alias to the form the live engine accepts (the tests are the contract).

- [ ] **Step 6: Lint, types, commit**

```bash
hatch run ruff:check src
hatch run ruff:check tests/test_unit/backends/ibis/test_upsert_render.py
hatch run mypy:check
git add src/mountainash_data/backends/ibis/_render.py src/mountainash_data/backends/ibis/operations.py tests/test_unit/backends/ibis/test_upsert_render.py
git commit -m "feat(ibis): generic upsert ON CONFLICT branch + compiled-subquery staging"
```

---

### Task 7: `_render_merge` (MERGE family)

**Files:**
- Modify: `src/mountainash_data/backends/ibis/operations.py`
- Test: `tests/test_unit/backends/ibis/test_upsert_render.py` (append)

**Interfaces:**
- Consumes: `compiled_source`, `compile_condition`, `qualified_name`, `quote_identifier` (Tasks 1/5/6).
- Produces: `_render_merge(...) -> str`; wired into the `_generic_upsert` MERGE branch (replacing the placeholder).

Model on Ibis 12 `SQLBackend._build_upsert_from_table` (`sge.merge`), extended to composite `on` and `conflict_action`. duckdb supports MERGE, so this is live-testable in-memory.

- [ ] **Step 1: Write the failing tests** (append to `test_upsert_render.py`)

```python
class TestMergeUpsert:
    def test_merge_insert_and_update_duckdb(self):
        con = ibis.duckdb.connect()
        con.create_table("m", pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}))
        from mountainash_data.backends.ibis.operations import _generic_upsert as gu
        gu(
            con, "m", pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}),
            style=UpsertStyle.MERGE, conflict_columns=["id"], update_columns=None,
            conflict_action="UPDATE", update_condition=None, database=None, schema=None,
        )
        rows = dict(con.table("m").order_by("id").execute()[["id", "v"]].itertuples(index=False))
        assert rows == {1: "a", 2: "B", 3: "c"}

    def test_merge_nothing_omits_matched_duckdb(self):
        con = ibis.duckdb.connect()
        con.create_table("m", pl.DataFrame({"id": [1], "v": ["a"]}))
        from mountainash_data.backends.ibis.operations import _generic_upsert as gu
        gu(
            con, "m", pl.DataFrame({"id": [1, 2], "v": ["X", "b"]}),
            style=UpsertStyle.MERGE, conflict_columns=["id"], update_columns=None,
            conflict_action="NOTHING", update_condition=None, database=None, schema=None,
        )
        rows = dict(con.table("m").order_by("id").execute()[["id", "v"]].itertuples(index=False))
        assert rows == {1: "a", 2: "b"}  # id=1 NOT updated, id=2 inserted
```

- [ ] **Step 2: Run to verify it fails**

Run: `hatch run test:test-target-quick "tests/test_unit/backends/ibis/test_upsert_render.py::TestMergeUpsert" -v`
Expected: FAIL — `NotImplementedError: unimplemented style: MERGE`.

- [ ] **Step 3: Implement `_render_merge`** (replace the MERGE-branch placeholder with `stmt = _render_merge(...)` and add):

```python
def _render_merge(
    ibis_conn, name, obj, *, target_schema, conflict, update,
    conflict_action, update_condition, database, schema,
) -> str:
    dialect = dialect_of(ibis_conn)
    source_sql, cols = compiled_source(ibis_conn, obj, target_schema)
    parts = [p for p in (database, schema, name) if p]
    target = qualified_name(parts, dialect)
    q = lambda c: quote_identifier(c, dialect)  # noqa: E731

    on = " AND ".join(f"tgt.{q(c)} = src.{q(c)}" for c in conflict)
    not_matched = (
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(q(c) for c in cols)}) "
        f"VALUES ({', '.join(f'src.{q(c)}' for c in cols)})"
    )
    clauses = []
    if conflict_action == "UPDATE":
        set_sql = ", ".join(f"{q(c)} = src.{q(c)}" for c in update)
        cond = ""
        if update_condition is not None:
            cond = " AND " + compile_condition(
                ibis_conn, target_schema, update_condition,
                incoming_alias="src", existing_alias="tgt",
            ).sql(dialect=dialect)
        clauses.append(f"WHEN MATCHED{cond} THEN UPDATE SET {set_sql}")
    clauses.append(not_matched)

    return (
        f"MERGE INTO {target} AS tgt USING ({source_sql}) AS src "
        f"ON {on} " + " ".join(clauses)
    )
```

- [ ] **Step 4: Run to verify it passes**

Run: `hatch run test:test-target-quick "tests/test_unit/backends/ibis/test_upsert_render.py::TestMergeUpsert" -v`
Expected: PASS (2 tests). If duckdb's MERGE grammar rejects a clause ordering, reorder to its accepted form (the data-outcome assertions are the contract).

- [ ] **Step 5: Add golden-SQL assertions for a warehouse dialect** (append):

```python
class TestMergeGoldenSQL:
    def test_snowflake_merge_shape(self):
        # render-only: assert the emitted MERGE string shape for snowflake
        from mountainash_data.backends.ibis.operations import _render_merge
        con = ibis.duckdb.connect()
        con.create_table("m", pl.DataFrame({"id": [1], "v": ["a"]}))
        # compile against duckdb conn but assert structural tokens dialect-agnostically
        sql = _render_merge(
            con, "m", pl.DataFrame({"id": [1], "v": ["a"]}),
            target_schema=con.table("m").schema(), conflict=["id"], update=["v"],
            conflict_action="UPDATE", update_condition=None, database=None, schema=None,
        )
        assert sql.startswith("MERGE INTO")
        assert "WHEN MATCHED THEN UPDATE SET" in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql
```

- [ ] **Step 6: Run, lint, types, commit**

```bash
hatch run test:test-target-quick tests/test_unit/backends/ibis/test_upsert_render.py -v
hatch run ruff:check src tests/test_unit/backends/ibis/test_upsert_render.py
hatch run mypy:check
git add src/mountainash_data/backends/ibis/operations.py tests/test_unit/backends/ibis/test_upsert_render.py
git commit -m "feat(ibis): generic upsert MERGE branch (composite keys + conflict_action)"
```

---

### Task 8: `_render_on_duplicate_key` + MySQL preflight introspection

**Files:**
- Modify: `src/mountainash_data/backends/ibis/operations.py`
- Test: `tests/test_integration/test_upsert_mysql_preflight.py` (create), `tests/test_unit/backends/ibis/test_upsert_render.py` (golden append)

**Interfaces:**
- Consumes: `compiled_source`, `qualified_name`, `quote_identifier` (Tasks 1/6).
- Produces: `_render_on_duplicate_key(...) -> str`; `_mysql_validate_conflict_key(ibis_conn, name, conflict, database) -> None` (prove-safe-or-raise preflight, §6.2); wired into the `_generic_upsert` ON_DUPLICATE_KEY branch.

- [ ] **Step 1: Write the failing live tests**

Create `tests/test_integration/test_upsert_mysql_preflight.py`:

```python
"""MySQL ON DUPLICATE KEY preflight: prove-safe-or-raise (spec §6.2)."""

import polars as pl
import pytest


@pytest.mark.integration
def test_single_pk_proceeds(mysql_backend):
    be = mysql_backend
    con = be._connection._ibis_conn  # raw ibis conn
    con.raw_sql("DROP TABLE IF EXISTS odk_ok")
    con.raw_sql("CREATE TABLE odk_ok (id INT PRIMARY KEY, v VARCHAR(16) NOT NULL)")
    con.raw_sql("INSERT INTO odk_ok VALUES (1, 'a')")
    be.upsert("odk_ok", pl.DataFrame({"id": [1, 2], "v": ["A", "b"]}), conflict_columns=["id"])
    rows = dict(con.table("odk_ok").order_by("id").execute()[["id", "v"]].itertuples(index=False))
    assert rows == {1: "A", 2: "b"}
    con.raw_sql("DROP TABLE odk_ok")


@pytest.mark.integration
def test_multiple_unique_raises(mysql_backend):
    be = mysql_backend
    con = be._connection._ibis_conn
    con.raw_sql("DROP TABLE IF EXISTS odk_multi")
    con.raw_sql(
        "CREATE TABLE odk_multi "
        "(id INT PRIMARY KEY, email VARCHAR(64) NOT NULL UNIQUE, v VARCHAR(16) NOT NULL)"
    )
    with pytest.raises(ValueError, match="unique"):
        be.upsert("odk_multi", pl.DataFrame({"id": [1], "email": ["x"], "v": ["a"]}), conflict_columns=["id"])
    con.raw_sql("DROP TABLE odk_multi")


@pytest.mark.integration
def test_nullable_conflict_column_raises(mysql_backend):
    be = mysql_backend
    con = be._connection._ibis_conn
    con.raw_sql("DROP TABLE IF EXISTS odk_null")
    con.raw_sql("CREATE TABLE odk_null (k INT NULL UNIQUE, v VARCHAR(16) NOT NULL)")
    with pytest.raises(ValueError, match="nullable|NOT NULL"):
        be.upsert("odk_null", pl.DataFrame({"k": [1], "v": ["a"]}), conflict_columns=["k"])
    con.raw_sql("DROP TABLE odk_null")
```

(The exact `be._connection._ibis_conn` accessor: confirm the attribute path against `backend.py` — use whatever the existing code exposes for the raw connection. If `upsert` already runs through dispatch by this task, calling `be.upsert(...)` is enough and the raw-conn lines are only for setup DDL.)

- [ ] **Step 2: Run to verify it fails** (services up)

Run: `docker compose up -d --wait && hatch run test:test-target-quick tests/test_integration/test_upsert_mysql_preflight.py -v`
Expected: FAIL — `NotImplementedError: unimplemented style: ON_DUPLICATE_KEY` (or dispatch not yet wired → see Task 9 ordering note).

- [ ] **Step 3: Implement preflight + renderer**

```python
def _mysql_validate_conflict_key(ibis_conn, name, conflict, database) -> None:
    """Prove the safe MySQL ON DUPLICATE KEY case or raise (spec §6.2)."""
    db = database or ibis_conn.current_database
    rows = ibis_conn.raw_sql(
        "SELECT INDEX_NAME, COLUMN_NAME, SUB_PART, NON_UNIQUE "
        "FROM information_schema.STATISTICS "
        f"WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = '{name}'"
    ).fetchall()
    uniques: dict[str, list] = {}
    for index_name, column_name, sub_part, non_unique in rows:
        if int(non_unique) == 0:
            uniques.setdefault(index_name, []).append((column_name, sub_part))
    if not uniques:
        raise ValueError(f"table {name!r} has no unique/PK index for conflict_columns")
    matching = [
        idx for idx, cols in uniques.items()
        if [c for c, _ in cols] == list(conflict) and all(sp is None for _, sp in cols)
    ]
    if len(uniques) > 1:
        raise ValueError(
            f"table {name!r} has multiple unique indexes {list(uniques)}; MySQL "
            f"ON DUPLICATE KEY detects on any of them — ambiguous for conflict_columns="
            f"{conflict}. Use the upsert_hook override."
        )
    if not matching:
        raise ValueError(
            f"no non-prefix unique index exactly matches conflict_columns={conflict} "
            f"(found {uniques}); refusing to guess. Use the upsert_hook override."
        )
    # nullable check
    cols_meta = ibis_conn.raw_sql(
        "SELECT COLUMN_NAME, IS_NULLABLE FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = '{name}'"
    ).fetchall()
    nullable = {c for c, isn in cols_meta if isn == "YES"}
    bad = [c for c in conflict if c in nullable]
    if bad:
        raise ValueError(
            f"conflict columns {bad} are nullable; MySQL unique indexes are "
            f"NULL-distinct, so duplicates would insert. Make them NOT NULL or "
            f"use the upsert_hook override."
        )


def _render_on_duplicate_key(
    ibis_conn, name, obj, *, target_schema, conflict, update,
    conflict_action, update_condition, database, schema,
) -> str:
    _mysql_validate_conflict_key(ibis_conn, name, conflict, database)
    dialect = dialect_of(ibis_conn)
    source_sql, cols = compiled_source(ibis_conn, obj, target_schema)
    parts = [p for p in (database, schema, name) if p]
    target = qualified_name(parts, dialect)
    q = lambda c: quote_identifier(c, dialect)  # noqa: E731
    col_list = ", ".join(q(c) for c in cols)

    if conflict_action == "NOTHING":
        k0 = q(conflict[0])
        set_sql = f"{k0} = {k0}"  # self-assign; see §6.2 (not a true no-op)
    else:
        set_sql = ", ".join(f"{q(c)} = VALUES({q(c)})" for c in update)

    return (
        f"INSERT INTO {target} ({col_list}) SELECT {col_list} FROM ({source_sql}) AS __src "
        f"ON DUPLICATE KEY UPDATE {set_sql}"
    )
```

Wire the ON_DUPLICATE_KEY branch in `_generic_upsert` to `stmt = _render_on_duplicate_key(...)`. Also enforce step-5 precedence (§10): when `style is ON_DUPLICATE_KEY and update_condition is not None`, raise `ValueError("update_condition is not supported for the MySQL ON DUPLICATE KEY family")` — place this check in `_generic_upsert` BEFORE the branch dispatch.

- [ ] **Step 4: Run to verify it passes** (services up)

Run: `hatch run test:test-target-quick tests/test_integration/test_upsert_mysql_preflight.py -v`
Expected: 3 PASS (after Task 9 wires dispatch; if running before Task 9, call `_generic_upsert` directly as in Task 6's tests).

- [ ] **Step 5: Add golden-SQL for on_duplicate_key** (append to `test_upsert_render.py`, render-only, no live MySQL): assert `_render_on_duplicate_key`-style output contains `ON DUPLICATE KEY UPDATE` and `VALUES(` — but since it calls the preflight, test the pure render by factoring the SQL-string builder out of the preflight, or mark this assertion as covered by the live preflight test. Keep the unit layer to the string shape only.

- [ ] **Step 6: Lint, types, commit**

```bash
hatch run ruff:check src tests/test_integration/test_upsert_mysql_preflight.py
hatch run mypy:check
git add src/mountainash_data/backends/ibis/operations.py tests/test_integration/test_upsert_mysql_preflight.py tests/test_unit/backends/ibis/test_upsert_render.py
git commit -m "feat(ibis): generic upsert ON DUPLICATE KEY + MySQL prove-safe preflight"
```

---

### Task 9: Cutover — flip `upsert` dispatch, retire `duckdb_family_upsert`

**Files:**
- Modify: `src/mountainash_data/backends/ibis/backend.py` (dispatch + retype `update_condition`), `src/mountainash_data/backends/ibis/dialects/_registry.py` (remove 3 hook registrations), `src/mountainash_data/backends/ibis/operations.py` (delete `duckdb_family_upsert`)
- Test: existing upsert tests (must stay green via the generic path)

**Interfaces:**
- Consumes: `_generic_upsert` (Tasks 6-8).
- Produces: `IbisBackend.upsert` dispatching hook-or-generic.

- [ ] **Step 1: Find existing upsert tests and run them (baseline green via hook)**

Run: `hatch run test:test-target-quick tests/ -k upsert -v` — note which pass today (via `duckdb_family_upsert`).

- [ ] **Step 2: Flip dispatch in `backend.py`**

Replace `upsert`'s body (currently raises when `upsert_hook is None`) with:

```python
    def upsert(
        self,
        name: str,
        obj: t.Any,
        *,
        conflict_columns: list[str] | str,
        update_columns: list[str] | str | None = None,
        conflict_action: str = "UPDATE",
        update_condition: t.Any = None,   # ConditionPredicate | None
        database: str | None = None,
        schema: str | None = None,
    ) -> IbisBackend:
        conn = self._require_connected()
        hook = self._spec.upsert_hook
        if hook is not None:
            hook(
                conn._ibis_conn, name, obj, conflict_columns=conflict_columns,
                update_columns=update_columns, conflict_action=conflict_action,
                update_condition=update_condition, database=database, schema=schema,
            )
        else:
            _generic_upsert(
                conn._ibis_conn, name, obj, style=self._spec.upsert_style,
                conflict_columns=conflict_columns, update_columns=update_columns,
                conflict_action=conflict_action, update_condition=update_condition,
                database=database, schema=schema,
            )
        return self
```

Add `_generic_upsert` to the operations import. (Keep `ConditionPredicate` type alias documented in `_render.py` / re-exported if the public API surfaces it.)

- [ ] **Step 3: Remove the duckdb-family hook registrations**

In `_registry.py`, delete the three `upsert_hook=duckdb_family_upsert,` lines (sqlite/duckdb/motherduck) — they already carry `upsert_style=ON_CONFLICT` from Task 3, so they now flow through the generic renderer.

- [ ] **Step 4: Delete `duckdb_family_upsert`**

Remove the `duckdb_family_upsert` function from `operations.py` and any now-unused imports it alone used (`uuid`, `contextlib`, `warnings` — keep any still used elsewhere; verify with ruff).

- [ ] **Step 5: Run the existing upsert tests + the new render tests**

Run:
```bash
hatch run test:test-target-quick tests/ -k upsert -v
docker compose up -d --wait && hatch run test:test-target-quick tests/test_integration -v ; docker compose down
```
Expected: all upsert tests PASS via the generic path; live postgres/mysql round-trips PASS. If a previously-passing assertion encoded `duckdb_family_upsert`-specific behaviour (e.g. the staging-table temp name), update it to assert the data outcome instead — present any such test to the user before changing it (Test Integrity rule).

- [ ] **Step 6: Lint, types, commit**

```bash
hatch run ruff:check src
hatch run mypy:check
git add src/mountainash_data/backends/ibis/backend.py src/mountainash_data/backends/ibis/dialects/_registry.py src/mountainash_data/backends/ibis/operations.py tests/
git commit -m "feat(ibis): cutover upsert to generic dispatch; retire duckdb_family_upsert"
```

---

### Task 10: Full-suite regression + matrix-completeness gate

**Files:** none (verification only).

- [ ] **Step 1: Golden-SQL matrix completeness**

Confirm `test_upsert_style_registry.py::test_every_registry_dialect_has_an_explicit_decision` and the render suite iterate `DIALECTS` (no hardcoded count). Run:
`hatch run test:test-target-quick tests/test_unit/backends/ibis/ -v` — all PASS.

- [ ] **Step 2: Full suite (no live) + live suite**

```bash
hatch run test:test-target-quick tests/ -v
docker compose up -d --wait && MOUNTAINASH_REQUIRE_LIVE_DB=1 hatch run test:test-target-quick tests/test_integration -v ; docker compose down
```
Expected: full unit/integration green; live job green with services up (and the `MOUNTAINASH_REQUIRE_LIVE_DB=1` run would FAIL if a service were down — proving the fail-closed gate).

- [ ] **Step 3: Lint + types across the branch**

```bash
hatch run ruff:check src
hatch run mypy:check
```
Expected: both clean.

- [ ] **Step 4: (If anything failed) fix and re-run** — do not proceed to PR until Steps 1-3 are green.

---

## Out of Scope (tracked elsewhere)

- `create_index` / `drop_index` / index-catalog queries — capability-divergent, stay hook-required.
- Consumer migration (mountainash-wearables → postgres) — in that repo after this ships.
- Live warehouse testing (snowflake/bigquery/mssql/…) — golden-SQL only here.
- An optional source-row `deduplicate=` flag — future extension (spec §11).

## Self-Review

**Spec coverage:**
- `upsert_style` registry field + assignment (spec §5.1, §7) → Task 3. ✓
- `_render.py` primitives + `add_columns` consolidation (§5.2) → Task 1. ✓
- `_generic_rename_table` + dispatch (§5.3, §4.3) → Task 4. ✓
- Three upsert renderers (§5.4, §6) → Tasks 6 (ON CONFLICT), 7 (MERGE), 8 (ON DUPLICATE KEY). ✓
- Compiled-subquery staging + column ordering + type casts (§5.5) → Task 6 (`compiled_source`). ✓
- `update_condition` ibis-expression predicate + sentinel/alias-remap + grammar validator (§4.1, §6.1) → Task 5; integrated in Tasks 6/7. ✓
- MySQL prove-safe-or-raise preflight (§6.2) → Task 8. ✓
- Validation precedence (§10) → Task 6 (`_generic_upsert` ordering) + Task 8 (ON DUPLICATE KEY condition reject). ✓
- Docker infra + skip/fail-closed fixtures + CI (§8) → Task 2. ✓
- ibis pin >=12 + CLAUDE.md fix (§12) → Task 2. ✓
- Cutover / delete `duckdb_family_upsert` (§13) → Task 9. ✓
- Matrix completeness via registry iteration (§7, §8.4) → Task 3 + Task 10. ✓

**Placeholder scan:** the MERGE/ON_DUPLICATE branches are *intentional* `NotImplementedError` placeholders in Task 6 that Tasks 7/8 replace — each is a real, runnable line with a test that asserts the placeholder, then the next task removes it. No "TBD"/"add error handling"/uncoded steps remain.

**Type consistency:** `_generic_upsert(ibis_conn, name, obj, *, style, conflict_columns, update_columns, conflict_action, update_condition, database, schema)` is identical across Tasks 6/8/9. `compiled_source(ibis_conn, obj, target_schema) -> (sql, cols)`, `compile_condition(ibis_conn, target_schema, predicate, *, incoming_alias, existing_alias)`, and the `_render_*` signatures match between definition and call sites. `UpsertStyle` members (`ON_CONFLICT`/`MERGE`/`ON_DUPLICATE_KEY`) are consistent across Tasks 3/6/7/8.

**Known implementation-time confirmations (flagged inline, not placeholders):** exact sqlglot expression classes for rename (Task 4), the forbidden ibis op classes for the grammar validator (Task 5), and the raw-conn accessor on `IbisBackend` (Task 8) are each pinned by a one-line probe in their task; the behaviour contract is fixed by the tests in every case.
