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


class TestRenameGoldenPerDialect:
    """Registry-iterating render assertion — every dialect renders a rename."""

    @pytest.mark.parametrize("name", list(DIALECTS))
    def test_every_dialect_renders_rename(self, name):
        from mountainash_data.backends.ibis.operations import build_rename_sql
        d = _IBIS_TO_SQLGLOT.get(DIALECTS[name].ibis_backend_name,
                                 DIALECTS[name].ibis_backend_name)
        sql = build_rename_sql("old", "new", dialect=d)
        # tsql renders sp_rename; everyone else an ALTER ... RENAME
        assert ("sp_rename" in sql.lower()) or ("rename" in sql.lower())
```

Add the imports this test needs to the top of the file: `from mountainash_data.backends.ibis.dialects._registry import DIALECTS` and the shared ibis→sqlglot dialect map (define once; reuse in the upsert golden test):

```python
# ibis backend name -> sqlglot dialect name (identity unless noted).
# Pinned by probe (see below): sqlglot 30.x has no "motherduck"/"singlestoredb".
_IBIS_TO_SQLGLOT = {
    "mssql": "tsql",
    "motherduck": "duckdb",
    "singlestoredb": "singlestore",
}
```

> **Pin the full map by probe (required before the golden tests run).** For every `DIALECTS` entry, confirm `sqlglot.Dialect.get_or_raise(<mapped name>)` succeeds — a name sqlglot 30.x doesn't know (e.g. it may not register `risingwave`/`exasol`/`databricks` under those exact strings) makes the golden test error, not assert. Where sqlglot lacks a dialect, map to its closest wire-compatible base (e.g. risingwave→`postgres`) and note it in the test; this is rendering-shape verification, so the base dialect's quoting/keywords are what we assert. (Codex HIGH-2.)

- [ ] **Step 2: Run to verify it fails**

Run: `hatch run test:test-target-quick tests/test_unit/backends/ibis/test_rename_table_render.py -v`
Expected: FAIL — `ImportError: cannot import name '_generic_rename_table'`.

- [ ] **Step 3: Implement `_generic_rename_table`**

In `operations.py`, add (importing the helpers and `exp` at top if not already present: `from sqlglot import exp` is already imported for add_columns; add `from mountainash_data.backends.ibis._render import dialect_of, quote_identifier`):

```python
def build_rename_sql(old_name: str, new_name: str, *, dialect: t.Any) -> str:
    """Pure builder: render a portable rename for an explicit sqlglot dialect.

    sqlglot renders `ALTER TABLE … RENAME TO …` for most dialects, `EXEC
    sp_rename …` for SQL Server (tsql), and `ALTER TABLE … RENAME …` for MySQL.
    Taking `dialect` explicitly lets the registry-iterating golden test render
    every dialect without a live connection.
    """
    return exp.Alter(
        this=exp.to_table(quote_identifier(old_name, dialect)),
        kind="TABLE",
        actions=[exp.AlterRename(this=exp.to_identifier(new_name, quoted=True))],
    ).sql(dialect=dialect)


def _generic_rename_table(ibis_conn: t.Any, old_name: str, new_name: str) -> None:
    """Rename a table via the sqlglot generic default off the live connection."""
    _validate_simple_identifier(old_name, kind="old_name")
    _validate_simple_identifier(new_name, kind="new_name")
    ibis_conn.raw_sql(build_rename_sql(old_name, new_name, dialect=dialect_of(ibis_conn)))
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
    ConditionAliases,
    compile_condition,
    dialect_of,
    validate_predicate,
)

_SCHEMA = ibis.schema({"id": "int64", "updated_at": "timestamp", "v": "string"})

# ON CONFLICT: incoming is the unquoted `excluded` pseudo-relation; existing is `tgt`.
_ONCONFLICT = ConditionAliases(incoming="excluded", existing="tgt", incoming_quoted=False)
# MERGE: both sides are normal quoted aliases.
_MERGE = ConditionAliases(incoming="src", existing="tgt")


def _render(con, predicate, aliases, *, target_name="t"):
    ast = compile_condition(con, _SCHEMA, target_name, predicate, aliases=aliases)
    return ast.sql(dialect=dialect_of(con))


class TestCompileCondition:
    def test_on_conflict_alias_mapping_unquoted_excluded(self):
        con = ibis.duckdb.connect()
        sql = _render(con, lambda inc, exi: inc.updated_at > exi.updated_at, _ONCONFLICT)
        # EXCLUDED is the unquoted pseudo-relation; existing is quoted "tgt"
        assert "excluded." in sql.lower() and '"EXCLUDED"' not in sql
        assert '"tgt"."updated_at"' in sql

    def test_merge_alias_mapping_duckdb(self):
        con = ibis.duckdb.connect()
        sql = _render(con, lambda inc, exi: inc.updated_at > exi.updated_at, _MERGE)
        assert '"src"."updated_at"' in sql and '"tgt"."updated_at"' in sql

    def test_function_predicate_renders_per_dialect(self):
        con = ibis.duckdb.connect()
        sql = _render(con, lambda inc, exi: inc.v.upper() != exi.v.upper(), _MERGE)
        assert "UPPER(" in sql.upper()

    def test_constant_predicate_renders(self):
        con = ibis.duckdb.connect()
        sql = _render(con, lambda inc, exi: inc.id > 0, _MERGE)
        assert '"src"."id"' in sql

    def test_null_check_predicate_renders(self):
        con = ibis.duckdb.connect()
        sql = _render(con, lambda inc, exi: inc.v.notnull(), _MERGE)
        assert "NULL" in sql.upper()

    def test_rejects_target_name_colliding_with_sentinel(self):
        con = ibis.duckdb.connect()
        with pytest.raises(ValueError, match="sentinel"):
            _render(con, lambda inc, exi: inc.id > exi.id, _MERGE,
                    target_name="__ma_incoming__")

    def test_rejects_aggregate_predicate(self):
        with pytest.raises(ValueError, match="aggregat|window|scalar|subquer|row predicate"):
            validate_predicate(
                ibis.table(_SCHEMA, name="x").v.count() > 0  # aggregation
            )
```

> Add a window-function rejection test and a subquery/`EXISTS` rejection test once the exact ibis op classes are pinned by the Step-3 probe (the validator's `_SUBQUERY_OPS` tuple). Keep both as `pytest.raises(ValueError)`.

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
    # subqueries / EXISTS — detect SPECIFIC subquery op types, NOT ops.Relation
    # (ops.Relation also matches the two allowed sentinel tables and would
    # reject every valid predicate — Codex finding).
    for n in node.find(_SUBQUERY_OPS):  # type: ignore[arg-type]
        raise ValueError(
            "update_condition may not contain subqueries/EXISTS/third-table "
            "references; use the upsert_hook override."
        )


@dataclasses.dataclass(frozen=True)
class ConditionAliases:
    """How each side's columns are referenced in the rendered clause.

    `quoted=False` is used for the ON CONFLICT `EXCLUDED` pseudo-relation,
    which must NOT be a quoted identifier (Postgres exposes it as the special
    unquoted `excluded`; quoting it risks referencing the wrong object).
    """
    incoming: str           # e.g. "EXCLUDED" (on conflict) or "src" (merge)
    existing: str           # e.g. "tgt"
    incoming_quoted: bool = True
    existing_quoted: bool = True


def compile_condition(
    ibis_conn: t.Any,
    target_schema: t.Any,
    target_name: str,
    predicate: t.Callable[[ir.Table, ir.Table], ir.BooleanValue],
    *,
    aliases: ConditionAliases,
) -> exp.Expression:
    """Render an (incoming, existing) -> bool predicate to a sqlglot ON sub-AST,
    remapping incoming/existing columns to `aliases`. See spec §6.1.

    `target_name` is the real target table name — rejected if it collides with
    a reserved sentinel (spec §6.1 step 0).
    """
    if target_name in (INCOMING_SENTINEL, EXISTING_SENTINEL):
        raise ValueError(
            f"target table name {target_name!r} collides with a reserved "
            f"sentinel; rename the table or use the upsert_hook override."
        )
    incoming = ibis.table(target_schema, name=INCOMING_SENTINEL)
    existing = ibis.table(target_schema, name=EXISTING_SENTINEL)
    pred = predicate(incoming, existing)
    validate_predicate(pred)

    joined = existing.join(incoming, pred, how="inner")
    ast = ibis_conn.compiler.to_sqlglot(joined)
    ast = ast if isinstance(ast, exp.Expression) else ast[0]

    # alias -> (target_alias, quoted) keyed by the underlying SENTINEL name
    remap: dict[str, tuple[str, bool]] = {}
    for tbl in ast.find_all(exp.Table):
        if tbl.name == INCOMING_SENTINEL:
            remap[tbl.alias_or_name] = (aliases.incoming, aliases.incoming_quoted)
        elif tbl.name == EXISTING_SENTINEL:
            remap[tbl.alias_or_name] = (aliases.existing, aliases.existing_quoted)

    join = next(ast.find_all(exp.Join), None)
    if join is None or join.args.get("on") is None:
        raise ValueError("could not extract join ON predicate")
    on = join.args["on"].copy()

    def _remap(n: exp.Expression) -> exp.Expression:
        if isinstance(n, exp.Column) and n.table in remap:
            alias, quoted = remap[n.table]
            n.set("table", exp.to_identifier(alias, quoted=quoted))
        return n

    return on.transform(_remap)


def validate_condition(target_schema: t.Any, target_name: str, predicate) -> None:
    """Grammar + sentinel-collision validation only (no rendering) — for the
    unconditional §10.5 check in `_generic_upsert`."""
    if target_name in (INCOMING_SENTINEL, EXISTING_SENTINEL):
        raise ValueError(
            f"target table name {target_name!r} collides with a reserved sentinel."
        )
    incoming = ibis.table(target_schema, name=INCOMING_SENTINEL)
    existing = ibis.table(target_schema, name=EXISTING_SENTINEL)
    validate_predicate(predicate(incoming, existing))
```

`compile_condition` should call `validate_condition` at its top rather than duplicating the collision/grammar checks (keep the validation in one place).

Add `import dataclasses` and `import ibis.expr.operations as ops` to `_render.py`, plus the forbidden/subquery op tuples near the top:

```python
_FORBIDDEN_OPS = (ops.Reduction, ops.WindowFunction)
# subquery/EXISTS op classes — pinned by probe (see note); NOT ops.Relation.
_SUBQUERY_OPS = tuple(
    c for c in (
        getattr(ops, "ExistsSubquery", None),
        getattr(ops, "InSubquery", None),
        getattr(ops, "ScalarSubquery", None),
    ) if c is not None
)
```

> **Pin by probe (required before writing the validator):** in the test env, confirm (a) `ops.Reduction` / `ops.WindowFunction` exist and `expr.op().find((ops.Reduction,))` flags an aggregate predicate; (b) the exact subquery op class names in ibis 12 (`ExistsSubquery`/`InSubquery`/`ScalarSubquery` or their current equivalents) and that a non-subquery predicate yields an empty `_SUBQUERY_OPS` match. The behaviour contract (accept scalar row predicates incl. the two sentinel tables; reject aggregate/window/subquery) is fixed by the tests in Step 1.

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
  - `build_on_conflict_sql(*, dialect, target, cols, conflict, update, conflict_action, source_sql, condition_sql=None) -> str` — the pure, dialect-parameterized builder (mirrors `build_merge_sql`, Task 7). `_render_on_conflict(ibis_conn, ...)` is the thin wrapper that derives `dialect`/`source_sql`/`condition_sql` from the live conn and delegates — so the registry-iterating upsert golden test (Task 7/10) can render the ON CONFLICT family per dialect without a live connection.

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

Add imports at top: `from mountainash_data.backends.ibis._render import (ConditionAliases, compile_condition, compiled_source, qualified_name, quote_identifier, dialect_of, validate_condition)` and `from mountainash_data.backends.ibis.dialects._registry import UpsertStyle`. Keep `import warnings` (used by the NOTHING-ignores-condition warning; it predates this work). Then:

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
    # Validation precedence per spec §10: style -> target existence -> identifiers
    # -> conflict_action -> update_condition -> (odk preflight) -> updatable cols.
    if style is None:
        raise NotImplementedError(
            f"Dialect (connection {type(ibis_conn).__name__}) does not support upsert"
        )
    if not ibis_conn.table_exists(name, database=database):  # target existence (§10.2)
        raise ValueError(f"target table {name!r} does not exist")
    _validate_simple_identifier(name, kind="name")
    if database is not None:
        _validate_simple_identifier(database, kind="database")
    if conflict_action not in ("UPDATE", "NOTHING"):
        raise ValueError(f"conflict_action must be UPDATE or NOTHING, got {conflict_action!r}")

    target_schema = ibis_conn.table(name, database=database).schema()
    conflict = _normalize_columns(conflict_columns)
    # column existence (§10 MEDIUM): fail loudly, not as a backend SQL error
    missing = [c for c in conflict if c not in target_schema.names]
    if missing:
        raise ValueError(f"conflict_columns absent from target: {missing}")
    if update_columns is None:
        update = [c for c in target_schema.names if c not in conflict]
    else:
        update = _normalize_columns(update_columns)
        missing_u = [c for c in update if c not in target_schema.names]
        if missing_u:
            raise ValueError(f"update_columns absent from target: {missing_u}")

    # update_condition validated UNCONDITIONALLY, before any action-specific path
    # (§10.5) — a malformed predicate must error even under NOTHING.
    if update_condition is not None:
        if style is UpsertStyle.ON_DUPLICATE_KEY:
            raise ValueError(
                "update_condition is not supported for the MySQL ON DUPLICATE KEY family"
            )
        # validate grammar + sentinel collision now (raises on aggregate/window/
        # subquery or a target name colliding with a sentinel)
        validate_condition(target_schema, name, update_condition)
        if conflict_action == "NOTHING":
            warnings.warn("update_condition is ignored when conflict_action='NOTHING'")

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
    # Alias the target (INSERT INTO t AS tgt) so the existing row is referenced
    # by a dedicated alias in the SET/WHERE — spec §6.1/§7. EXCLUDED is the
    # UNQUOTED pseudo-relation (never a quoted identifier — Codex CRITICAL).
    excl = "EXCLUDED"  # unquoted keyword; Postgres exposes it as `excluded`

    if conflict_action == "NOTHING":
        action = f"ON CONFLICT ({conflict_list}) DO NOTHING"
    else:
        set_sql = ", ".join(
            f"{quote_identifier(c, dialect)} = {excl}.{quote_identifier(c, dialect)}"
            for c in update
        )
        where = ""
        if update_condition is not None:
            aliases = ConditionAliases(
                incoming=excl, existing="tgt", incoming_quoted=False
            )
            cond = compile_condition(
                ibis_conn, target_schema, name, update_condition, aliases=aliases,
            ).sql(dialect=dialect)
            where = f" WHERE {cond}"
        action = f"ON CONFLICT ({conflict_list}) DO UPDATE SET {set_sql}{where}"

    # `WHERE true` is REQUIRED by SQLite to disambiguate INSERT…SELECT…ON CONFLICT
    # (its parser errors near DO without it); harmless on duckdb/postgres. This
    # mirrors the original duckdb_family_upsert template. (Codex CRITICAL-2.)
    return (
        f"INSERT INTO {target} AS tgt ({col_list}) "
        f"SELECT {col_list} FROM ({source_sql}) AS __src WHERE true {action}"
    )
```

Add `ConditionAliases` to the `_render` import line at the top of `operations.py`.

> **§7 per-dialect alias gate:** `INSERT INTO t AS tgt … ON CONFLICT` is valid on the live `on_conflict` dialects (duckdb/sqlite/postgres). If a future `on_conflict` dialect rejects target aliasing *and* an `update_condition` is supplied, raise `ValueError` pointing at the `upsert_hook` (spec §7). The capability flag is set from live/golden verification; it does not affect unconditional upserts (which never reference the existing row).

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
            # NEW compile_condition signature (Task 5): target_name + aliases.
            cond = " AND " + compile_condition(
                ibis_conn, target_schema, name, update_condition,
                aliases=ConditionAliases(incoming="src", existing="tgt"),
            ).sql(dialect=dialect)
        clauses.append(f"WHEN MATCHED{cond} THEN UPDATE SET {set_sql}")
    clauses.append(not_matched)

    return (
        f"MERGE INTO {target} AS tgt USING ({source_sql}) AS src "
        f"ON {on} " + " ".join(clauses)
    )
```

Add `ConditionAliases` to the `_render` import in `operations.py` (shared with Task 6).

- [ ] **Step 4: Run to verify it passes**

Run: `hatch run test:test-target-quick "tests/test_unit/backends/ibis/test_upsert_render.py::TestMergeUpsert" -v`
Expected: PASS (2 tests). If duckdb's MERGE grammar rejects a clause ordering, reorder to its accepted form (the data-outcome assertions are the contract).

- [ ] **Step 5: Per-dialect golden SQL via a pure builder (correct dialect, no live warehouse)**

The conn-bound `_render_merge` cannot render Snowflake without a Snowflake connection (Codex: rendering "Snowflake golden SQL" through a DuckDB conn is not a Snowflake test). Split a **pure builder** that takes an explicit sqlglot `dialect` + an already-rendered `source_sql`, and make `_render_merge` (and the other two renderers) thin wrappers that compute `dialect`/`source_sql` from the live conn and delegate:

```python
def build_merge_sql(
    *, dialect, target, cols, conflict, update, conflict_action,
    source_sql, condition_sql=None,
) -> str:
    q = lambda c: quote_identifier(c, dialect)  # noqa: E731
    on = " AND ".join(f"tgt.{q(c)} = src.{q(c)}" for c in conflict)
    not_matched = (
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(q(c) for c in cols)}) "
        f"VALUES ({', '.join(f'src.{q(c)}' for c in cols)})"
    )
    clauses = []
    if conflict_action == "UPDATE":
        set_sql = ", ".join(f"{q(c)} = src.{q(c)}" for c in update)
        cond = f" AND {condition_sql}" if condition_sql else ""
        clauses.append(f"WHEN MATCHED{cond} THEN UPDATE SET {set_sql}")
    clauses.append(not_matched)
    return f"MERGE INTO {target} AS tgt USING ({source_sql}) AS src ON {on} " + " ".join(clauses)
```

`_render_merge(ibis_conn, ...)` computes `dialect = dialect_of(ibis_conn)`, `source_sql, cols = compiled_source(ibis_conn, obj, target_schema)`, renders the condition AST with `dialect` if present, builds `target = qualified_name(...)`, and returns `build_merge_sql(...)`. (Apply the same pure-builder split to `build_on_conflict_sql` and `build_on_duplicate_key_sql` in Tasks 6/8 — each takes `dialect` + `source_sql`, so golden tests render any dialect.)

Then a registry-iterating golden test renders each MERGE-family dialect with its **own** sqlglot dialect (mapping ibis backend name → sqlglot dialect; identity except `mssql`→`tsql`):

```python
import pytest
from mountainash_data.backends.ibis.dialects._registry import DIALECTS, UpsertStyle
from mountainash_data.backends.ibis.operations import build_merge_sql

# Reuse the shared, probe-pinned map (mssql->tsql, motherduck->duckdb,
# singlestoredb->singlestore, + any sqlglot-unknown dialect mapped to its base).
from tests.test_unit.backends.ibis.test_rename_table_render import _IBIS_TO_SQLGLOT


def _sqlglot_dialect(spec):
    return _IBIS_TO_SQLGLOT.get(spec.ibis_backend_name, spec.ibis_backend_name)


@pytest.mark.parametrize(
    "name", [n for n, s in DIALECTS.items() if s.upsert_style is UpsertStyle.MERGE]
)
def test_merge_golden_per_dialect(name):
    d = _sqlglot_dialect(DIALECTS[name])
    sql = build_merge_sql(
        dialect=d, target=f'"m"' if d not in {"tsql", "mysql"} else "m",
        cols=["id", "v"], conflict=["id"], update=["v"],
        conflict_action="UPDATE", source_sql="SELECT 1 AS id, 'a' AS v",
    )
    assert sql.startswith("MERGE INTO")
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    # identifiers quoted in the target dialect's style
    assert ("`" in sql) == (d in {"mysql"})
```

This renders each warehouse dialect with the correct sqlglot dialect (real per-dialect emission), not DuckDB-as-Snowflake. Pin the `_IBIS_TO_SQLGLOT` map against the installed sqlglot during implementation (most ibis names equal their sqlglot dialect; `mssql`→`tsql` is the known exception).

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
- Produces: `build_on_duplicate_key_sql(*, dialect, target, cols, conflict, update, conflict_action, source_sql) -> str` (pure builder, mirrors `build_merge_sql`); `_render_on_duplicate_key(ibis_conn, ...)` thin wrapper that runs `_mysql_validate_conflict_key` then delegates; `_mysql_validate_conflict_key(ibis_conn, name, conflict, database) -> None` (prove-safe-or-raise preflight, §6.2); wired into the `_generic_upsert` ON_DUPLICATE_KEY branch.

- [ ] **Step 1: Write the failing live tests**

Create `tests/test_integration/test_upsert_mysql_preflight.py`:

```python
"""MySQL ON DUPLICATE KEY preflight: prove-safe-or-raise (spec §6.2).

Calls `_generic_upsert(...)` DIRECTLY against the raw mariadb connection — the
`be.upsert()` dispatch is not flipped until Task 9, so testing the generic
function directly is what keeps this task self-contained (Codex finding).
"""

import polars as pl
import pytest

from mountainash_data.backends.ibis.dialects._registry import UpsertStyle
from mountainash_data.backends.ibis.operations import _generic_upsert


def _raw(be):
    # The fixture yields a connected IbisBackend; reach its raw ibis conn.
    # Confirm the exact accessor against backend.py during implementation.
    return be._require_connected()._ibis_conn


def _odk(con, name, df, conflict):
    _generic_upsert(
        con, name, df, style=UpsertStyle.ON_DUPLICATE_KEY,
        conflict_columns=conflict, update_columns=None, conflict_action="UPDATE",
        update_condition=None, database=None, schema=None,
    )


@pytest.mark.integration
def test_single_pk_proceeds(mysql_backend):
    con = _raw(mysql_backend)
    con.raw_sql("DROP TABLE IF EXISTS odk_ok")
    con.raw_sql("CREATE TABLE odk_ok (id INT PRIMARY KEY, v VARCHAR(16) NOT NULL)")
    con.raw_sql("INSERT INTO odk_ok VALUES (1, 'a')")
    _odk(con, "odk_ok", pl.DataFrame({"id": [1, 2], "v": ["A", "b"]}), ["id"])
    rows = dict(con.table("odk_ok").order_by("id").execute()[["id", "v"]].itertuples(index=False))
    assert rows == {1: "A", 2: "b"}
    con.raw_sql("DROP TABLE odk_ok")


@pytest.mark.integration
def test_multiple_unique_raises(mysql_backend):
    con = _raw(mysql_backend)
    con.raw_sql("DROP TABLE IF EXISTS odk_multi")
    con.raw_sql(
        "CREATE TABLE odk_multi "
        "(id INT PRIMARY KEY, email VARCHAR(64) NOT NULL UNIQUE, v VARCHAR(16) NOT NULL)"
    )
    with pytest.raises(ValueError, match="unique"):
        _odk(con, "odk_multi", pl.DataFrame({"id": [1], "email": ["x"], "v": ["a"]}), ["id"])
    con.raw_sql("DROP TABLE odk_multi")


@pytest.mark.integration
def test_prefix_index_raises(mysql_backend):
    con = _raw(mysql_backend)
    con.raw_sql("DROP TABLE IF EXISTS odk_prefix")
    con.raw_sql("CREATE TABLE odk_prefix (email VARCHAR(64) NOT NULL, v VARCHAR(16) NOT NULL, UNIQUE (email(10)))")
    with pytest.raises(ValueError, match="prefix|SUB_PART"):
        _odk(con, "odk_prefix", pl.DataFrame({"email": ["x"], "v": ["a"]}), ["email"])
    con.raw_sql("DROP TABLE odk_prefix")


@pytest.mark.integration
def test_nullable_conflict_column_raises(mysql_backend):
    con = _raw(mysql_backend)
    con.raw_sql("DROP TABLE IF EXISTS odk_null")
    con.raw_sql("CREATE TABLE odk_null (k INT NULL UNIQUE, v VARCHAR(16) NOT NULL)")
    with pytest.raises(ValueError, match="nullable|NOT NULL"):
        _odk(con, "odk_null", pl.DataFrame({"k": [1], "v": ["a"]}), ["k"])
    con.raw_sql("DROP TABLE odk_null")
```

- [ ] **Step 2: Run to verify it fails** (services up)

Run: `docker compose up -d --wait && hatch run test:test-target-quick tests/test_integration/test_upsert_mysql_preflight.py -v`
Expected: FAIL — `NotImplementedError: unimplemented style: ON_DUPLICATE_KEY`.

- [ ] **Step 3: Implement preflight + renderer**

```python
def _mysql_validate_conflict_key(ibis_conn, name, conflict, database) -> None:
    """Prove the safe MySQL/MariaDB ON DUPLICATE KEY case or raise (spec §6.2).

    Fails closed on: no unique index, >1 unique index, prefix index (SUB_PART),
    functional/expression index, a unique index whose ORDERED columns don't
    exactly equal conflict_columns, or any nullable conflict column.
    """
    db = database or _current_schema(ibis_conn)  # see note: pin the resolver
    # ORDER BY SEQ_IN_INDEX so composite-key column order is correct.
    # NOTE: do NOT select EXPRESSION — it exists only in MySQL 8's STATISTICS,
    # not MariaDB's (the test image is mariadb:12.1.2), so selecting it errors
    # the query (Codex CRITICAL-1). Functional/expression index PARTS have a
    # NULL COLUMN_NAME on BOTH MariaDB and MySQL 8 — detect them that way.
    # SUB_PART is non-NULL for prefix index parts (present on both engines).
    rows = ibis_conn.raw_sql(
        "SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART, NON_UNIQUE "
        "FROM information_schema.STATISTICS "
        f"WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = '{name}' "
        "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
    ).fetchall()
    uniques: dict[str, list] = {}
    for index_name, _seq, column_name, sub_part, non_unique in rows:
        if int(non_unique) == 0:
            uniques.setdefault(index_name, []).append((column_name, sub_part))
    if not uniques:
        raise ValueError(f"table {name!r} has no unique/PK index for conflict_columns")
    if len(uniques) > 1:
        raise ValueError(
            f"table {name!r} has multiple unique indexes {list(uniques)}; MySQL "
            f"ON DUPLICATE KEY detects on any of them — ambiguous for "
            f"conflict_columns={conflict}. Use the upsert_hook override."
        )
    (idx_name, parts), = uniques.items()
    if any(col is None for col, _ in parts):  # NULL COLUMN_NAME = functional part
        raise ValueError(
            f"unique index {idx_name!r} is a functional/expression index; cannot "
            f"prove it matches conflict_columns={conflict}. Use the upsert_hook override."
        )
    if any(sub is not None for _, sub in parts):
        raise ValueError(
            f"unique index {idx_name!r} has a prefix (SUB_PART); it detects on a "
            f"truncated value, not the full column. Use the upsert_hook override."
        )
    if [c for c, _ in parts] != list(conflict):
        raise ValueError(
            f"unique index {idx_name!r} columns {[c for c, _ in parts]} do not "
            f"exactly match conflict_columns={list(conflict)}; refusing to guess. "
            f"Use the upsert_hook override."
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
            f"NULL-distinct, so duplicates would insert instead of update. Make "
            f"them NOT NULL or use the upsert_hook override."
        )
```

> **Pin `_current_schema(ibis_conn)`** during implementation: ibis's current-database accessor differs by version (a `current_database`/`current_catalog` property vs a method). Probe the mysql backend in the test env and use the correct one (it must return the schema MariaDB resolves unqualified table names against). Do **not** assume `ibis_conn.current_database` is a property.

> **MariaDB `VALUES(col)` note:** the renderer's UPDATE uses `VALUES(col)`, valid on the MariaDB 12.x target. MySQL 8.0.20+ deprecates `VALUES()` in favour of a row alias; if MySQL-8 support is later required, switch to the alias form behind the dialect — out of scope for the MariaDB-tested target here.


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

Wire the ON_DUPLICATE_KEY branch in `_generic_upsert` to `stmt = _render_on_duplicate_key(...)` (replacing the `NotImplementedError` placeholder). The `update_condition`-on-ODK → `ValueError` precedence check already lives in `_generic_upsert` from Task 6 (§10.5), so no extra check is needed here.

- [ ] **Step 4: Run to verify it passes** (services up)

Run: `hatch run test:test-target-quick tests/test_integration/test_upsert_mysql_preflight.py -v`
Expected: 4 PASS (single-PK proceeds; multi-unique, prefix, nullable each raise `ValueError`). These call `_generic_upsert` directly, so they pass at this task's position regardless of the Task 9 dispatch flip.

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

- [ ] **Step 5: Add `be.upsert()` live round-trips (dispatch path, post-cutover)**

Append to `tests/test_integration/test_write_ops_live.py` — these exercise the full dispatch (Task 8's preflight tests call `_generic_upsert` directly, so the public `be.upsert()` path needs its own coverage):

```python
@pytest.mark.integration
def test_upsert_via_dispatch_postgres(postgres_backend):
    be = postgres_backend
    be.create_table("up_pg", pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}), overwrite=True)
    be._require_connected()._ibis_conn.raw_sql('ALTER TABLE up_pg ADD PRIMARY KEY (id)')
    be.upsert("up_pg", pl.DataFrame({"id": [2, 3], "v": ["B", "c"]}), conflict_columns=["id"])
    rows = dict(be.table("up_pg").order_by("id").execute()[["id", "v"]].itertuples(index=False))
    assert rows == {1: "a", 2: "B", 3: "c"}
    be.drop_table("up_pg", force=True)


@pytest.mark.integration
def test_upsert_via_dispatch_mysql(mysql_backend):
    be = mysql_backend
    con = be._require_connected()._ibis_conn
    con.raw_sql("DROP TABLE IF EXISTS up_my")
    con.raw_sql("CREATE TABLE up_my (id INT PRIMARY KEY, v VARCHAR(16) NOT NULL)")
    con.raw_sql("INSERT INTO up_my VALUES (1, 'a')")
    be.upsert("up_my", pl.DataFrame({"id": [1, 2], "v": ["A", "b"]}), conflict_columns=["id"])
    rows = dict(con.table("up_my").order_by("id").execute()[["id", "v"]].itertuples(index=False))
    assert rows == {1: "A", 2: "b"}
    con.raw_sql("DROP TABLE up_my")
```

- [ ] **Step 6: Run the existing + new upsert tests**

Run:
```bash
hatch run test:test-target-quick tests/ -k upsert -v
docker compose up -d --wait && hatch run test:test-target-quick tests/test_integration -v ; docker compose down
```
Expected: all upsert tests PASS via the generic path; live postgres/mysql round-trips PASS. If a previously-passing assertion encoded `duckdb_family_upsert`-specific behaviour (e.g. the staging-table temp name), update it to assert the data outcome instead — present any such test to the user before changing it (Test Integrity rule).

- [ ] **Step 7: Lint, types, commit**

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

**Type consistency:** `_generic_upsert(ibis_conn, name, obj, *, style, conflict_columns, update_columns, conflict_action, update_condition, database, schema)` is identical across Tasks 6/8/9. `compiled_source(ibis_conn, obj, target_schema) -> (sql, cols)`; `compile_condition(ibis_conn, target_schema, target_name, predicate, *, aliases: ConditionAliases) -> exp.Expression` (Task 5) matches its call sites in Tasks 6/7; the pure builders `build_rename_sql(old, new, *, dialect)`, `build_merge_sql(*, dialect, target, cols, conflict, update, conflict_action, source_sql, condition_sql=None)` (and `build_on_conflict_sql` / `build_on_duplicate_key_sql` per the Task 6/8 directive) take an explicit `dialect`, so golden tests render any dialect. `UpsertStyle` members are consistent across Tasks 3/6/7/8.

**Known implementation-time confirmations (flagged inline, not placeholders):** exact sqlglot expression classes for rename (Task 4), the forbidden/subquery ibis op classes for the grammar validator (Task 5), the ibis→sqlglot dialect map (`mssql`→`tsql`, Tasks 4/7), the `_current_schema` resolver (Task 8), and the raw-conn accessor on `IbisBackend` (Tasks 8/9) are each pinned by a one-line probe in their task; the behaviour contract is fixed by the tests in every case.

## Codex Plan Review — Disposition (applied)

Two-pass-reviewed spec; this plan then went through a Codex pass that found 3 CRITICAL + 4 HIGH execution-breakers, all resolved in-plan before execution:
- **C1 task ordering** — Task 8's MySQL tests now call `_generic_upsert(...)` directly (not `be.upsert()`), so they pass before the Task 9 cutover; Task 9 adds the `be.upsert()` dispatch-path round-trips.
- **C2 ON CONFLICT aliasing** — renders `INSERT INTO t AS tgt`, `existing_alias="tgt"`; §7 per-dialect alias gate noted.
- **C3 EXCLUDED quoting** — `excluded` rendered as an UNQUOTED pseudo-relation (via `ConditionAliases(incoming_quoted=False)`), never `"EXCLUDED"`.
- **H1 MySQL preflight** — orders by `SEQ_IN_INDEX`; fails closed on multi-unique, prefix (`SUB_PART`), functional/expression (`EXPRESSION`), column-set mismatch, and nullable conflict columns; live prefix test added.
- **H2 `compile_condition`** — gains `target_name` + sentinel-collision check + test.
- **H3 validator** — detects specific subquery ops (`_SUBQUERY_OPS`), not `ops.Relation` (which would match the sentinels); probe-pinned.
- **H4 golden rendering** — replaced DuckDB-as-Snowflake with registry-iterating golden tests over pure `build_*_sql(*, dialect, …)` builders rendering each dialect's correct sqlglot dialect; added the `rename_table` registry-iterating golden.
- **MEDIUM** — column-existence validation + §10 precedence ordering in `_generic_upsert`; pure-builder structure (over string-concat-on-a-conn); added predicate-shape tests. **LOW** — `VALUES()` MariaDB/MySQL-8 note; `_current_schema` resolver pinned.

A second Codex pass verified the above all RESOLVED and caught issues the revisions introduced — all fixed:
- **CRITICAL (MariaDB `EXPRESSION`)** — that column is MySQL-8-only; the preflight query would error on `mariadb:12.1.2`. Dropped it; functional indexes detected via `COLUMN_NAME IS NULL` (portable across MariaDB + MySQL 8).
- **CRITICAL (SQLite UPSERT)** — `INSERT … SELECT … ON CONFLICT` needs a discriminating `WHERE true`; added (mirrors the original `duckdb_family_upsert` template).
- **HIGH (stale call)** — Task 7's MERGE renderer still used the old `compile_condition` positional signature; updated to `target_name` + `ConditionAliases(incoming="src", existing="tgt")`.
- **HIGH (dialect map)** — expanded `_IBIS_TO_SQLGLOT` (`+motherduck→duckdb`, `+singlestoredb→singlestore`) with a probe directive to validate every name against sqlglot 30.x and map sqlglot-unknown dialects to their wire-compatible base.
- **MEDIUM** — `update_condition` grammar now validated unconditionally via `validate_condition` (errors even under NOTHING, which warns-and-ignores); `build_on_conflict_sql` / `build_on_duplicate_key_sql` made explicit pure-builder outputs.
