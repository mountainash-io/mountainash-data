# mountainash-data Audit and Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure `mountainash-data` from a class-explosion / mixin-tree architecture into a `core/` Protocol layer with `backends/ibis` and `backends/iceberg` peer implementations.

**Architecture:** Single `Backend` Protocol in `core/`. `IbisBackend` collapses 13 per-backend connection files into a data-driven `DialectSpec` registry. `IcebergBackend` finishes the half-done split between connection and operations files. Settings and factories relocate to `core/` largely unchanged. The seam to `mountainash-expressions` is `Backend.connect().to_relation(table)` returning a Relation.

**Tech Stack:** Python 3.12, ibis-framework 10.4.0, pyiceberg, pydantic (settings), hatch (build/test), pytest, ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-04-07-mountainash-data-audit-and-redesign.md`

**Phase boundaries:** Each phase ends with `hatch run test:test` green. Commit at the end of every task. Do not start a phase until the previous one is green.

---

## Pre-flight: Working environment

- [ ] **Step P1: Create a working branch off develop**

```bash
git checkout develop
git pull
git checkout -b refactor/data-package-audit
```

- [ ] **Step P2: Confirm baseline tests pass before any changes**

Run: `hatch run test:test`
Expected: PASS. If anything is red on `develop`, stop and report — do not start the refactor on a broken baseline.

- [ ] **Step P3: Capture baseline test count and coverage for later comparison**

Run: `hatch run test:test --collect-only -q | tail -5`
Record the test count somewhere (a scratch note). It should not decrease across the refactor except where tests are deliberately removed alongside their targets.

---

## Phase 0 — Cleanup (no architectural change)

**Files touched:**
- Delete: `src/mountainash_data/lineage/openlineage_helper.py`
- Delete: `src/mountainash_data/lineage/__init__.py` (if exists) and the empty `lineage/` dir
- Delete: `src/mountainash_data/databases/connections/pyiceberg/__init___old.py`
- Delete: `src/mountainash_data/databases/connections/db_connection_factory.py`
- Delete: `src/mountainash_data/databases/operations/ibis/postgres_ibis_operations.py`
- Delete: `src/mountainash_data/databases/operations/ibis/mysql_ibis_operations.py`
- Delete: `src/mountainash_data/databases/operations/ibis/oracle_ibis_operations.py`
- Delete: `src/mountainash_data/databases/operations/ibis/bigquery_ibis_operations.py`
- Delete: `src/mountainash_data/databases/operations/ibis/snowflake_ibis_operations.py`
- Delete: `src/mountainash_data/databases/operations/ibis/pyspark_ibis_operations.py`
- Delete: `src/mountainash_data/databases/operations/ibis/redshift_ibis_operations.py`
- Delete: `src/mountainash_data/databases/operations/ibis/mssql_ibis_operations.py`
- Modify: `src/mountainash_data/databases/operations/ibis/__init__.py` (drop deleted re-exports)
- Modify: `src/mountainash_data/__init__.py` (drop any references to deleted symbols if present)
- Modify: `src/mountainash_data/databases/connections/__init__.py` (drop reference to deleted `db_connection_factory`)
- Modify: `src/mountainash_data/databases/connections/pyiceberg/__init__.py` (drop `__init___old`)

### Task 0.1: Verify nothing imports the legacy db_connection_factory

- [ ] **Step 1: Grep for usages**

Run: `grep -rn "db_connection_factory\|DBConnectionFactory" src/ tests/ notebooks/ docs/`
Expected: usages only inside `databases/connections/db_connection_factory.py` itself, the modern `factories/connection_factory.py` (if it references the legacy class — unlikely), and any `__init__.py` re-exports.

If consumers exist outside the file itself, STOP and surface to user — the spec assumed this was a duplicate and safe to delete.

- [ ] **Step 2: Confirm 8 stub ops files have no concrete logic**

For each of the 8 files (postgres, mysql, oracle, bigquery, snowflake, pyspark, redshift, mssql):
Run: `wc -l src/mountainash_data/databases/operations/ibis/<name>_ibis_operations.py`
Read each file. Confirm each is ≤20 lines and contains only a class declaration with a `db_backend_name` property override.

If any file has methods beyond the property override, STOP and reclassify as `salvage` per the spec.

### Task 0.2: Delete the lineage stub

- [ ] **Step 1: Delete the file and (if present) its `__init__.py`**

```bash
rm src/mountainash_data/lineage/openlineage_helper.py
rm -f src/mountainash_data/lineage/__init__.py
rmdir src/mountainash_data/lineage
```

- [ ] **Step 2: Grep for lingering imports**

Run: `grep -rn "from mountainash_data.lineage\|mountainash_data\.lineage" src/ tests/ notebooks/`
Expected: zero matches.

- [ ] **Step 3: Run tests**

Run: `hatch run test:test`
Expected: PASS, same count as baseline.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore(data): remove empty lineage stub"
```

### Task 0.3: Delete the pyiceberg `__init___old.py` artifact

- [ ] **Step 1: Delete**

```bash
rm src/mountainash_data/databases/connections/pyiceberg/__init___old.py
```

- [ ] **Step 2: Confirm `__init__.py` doesn't reference it**

Read `src/mountainash_data/databases/connections/pyiceberg/__init__.py`. Remove any import of `__init___old` if present.

- [ ] **Step 3: Run tests**

Run: `hatch run test:test`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore(data): remove pyiceberg __init___old artifact"
```

### Task 0.4: Delete the legacy `db_connection_factory.py`

- [ ] **Step 1: Delete the file**

```bash
rm src/mountainash_data/databases/connections/db_connection_factory.py
```

- [ ] **Step 2: Update `databases/connections/__init__.py`**

Read `src/mountainash_data/databases/connections/__init__.py`. Remove any import or re-export of `db_connection_factory` / `DBConnectionFactory`.

- [ ] **Step 3: Update top-level `__init__.py` if needed**

Read `src/mountainash_data/__init__.py`. Remove any reference to the deleted symbol.

- [ ] **Step 4: Run tests**

Run: `hatch run test:test`
Expected: PASS.

If tests fail with `ImportError` referencing `db_connection_factory`, find the importer and update or remove the reference.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore(data): delete legacy db_connection_factory (duplicate of factories/connection_factory)"
```

### Task 0.5: Delete the 8 stub ibis ops files

- [ ] **Step 1: Delete the files**

```bash
cd src/mountainash_data/databases/operations/ibis
rm postgres_ibis_operations.py mysql_ibis_operations.py oracle_ibis_operations.py \
   bigquery_ibis_operations.py snowflake_ibis_operations.py pyspark_ibis_operations.py \
   redshift_ibis_operations.py mssql_ibis_operations.py
cd -
```

- [ ] **Step 2: Update `databases/operations/ibis/__init__.py`**

Read the file. Remove imports and `__all__` entries for the 8 deleted classes. The remaining valid ops modules are: `base_ibis_operations`, `_base_ibis_mixin`, `_duckdb_family_mixin`, `duckdb_ibis_operations`, `sqlite_ibis_operations`, `motherduck_ibis_operations`, `trino_ibis_operations`.

- [ ] **Step 3: Update top-level `__init__.py` and any other re-exporters**

Run: `grep -rn "PostgresIbisOperations\|MySQLIbisOperations\|OracleIbisOperations\|BigQueryIbisOperations\|SnowflakeIbisOperations\|PySparkIbisOperations\|RedshiftIbisOperations\|MSSQLIbisOperations" src/`
For each match outside the deleted files, decide: if it's a re-export, remove it; if it's actual usage in `factories/operations_factory.py`, update the strategy mapping to fall back to `BaseIbisOperations` (the parent class that contained all the actual logic).

- [ ] **Step 4: Update `factories/operations_factory.py`**

Read the file. Find the strategy mapping (likely a dict mapping backend names to class import paths). For each of the 8 deleted backends, replace the per-backend ops class with `mountainash_data.databases.operations.ibis.base_ibis_operations.BaseIbisOperations` (or whatever the canonical class name is — confirm by reading `base_ibis_operations.py`).

- [ ] **Step 5: Run tests**

Run: `hatch run test:test`
Expected: PASS.

If `test_operations_factory.py` fails because it asserts on specific class names, update the test expectations to match the new mapping. Do NOT modify tests to pass without understanding why — confirm the test was asserting "factory returns class X for backend Y" and that the new behavior ("factory returns BaseIbisOperations for backend Y") is correct.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore(data): delete 8 empty per-backend ibis ops stubs

Postgres, MySQL, Oracle, BigQuery, Snowflake, PySpark, Redshift, and
MSSQL operations classes were stubs containing only a db_backend_name
property override. The actual operations logic lives in
BaseIbisOperations (676 LOC). Factory mappings updated to point at the
base class for these backends."
```

---

## Phase 1 — Stand up `core/`

**Files touched:**
- Create: `src/mountainash_data/core/__init__.py`
- Create: `src/mountainash_data/core/protocol.py`
- Create: `src/mountainash_data/core/inspection.py`
- Create: `src/mountainash_data/core/registry.py` (placeholder, wired in Phase 4)
- Create: `src/mountainash_data/core/connection.py` (moved from `databases/connections/base_db_connection.py`)
- Create: `src/mountainash_data/core/constants.py` (moved from `databases/constants.py`)
- Create: `tests/test_unit/core/test_protocol.py`
- Create: `tests/test_unit/core/test_inspection.py`
- Modify: `src/mountainash_data/databases/connections/base_db_connection.py` (becomes a re-export shim)
- Modify: `src/mountainash_data/databases/constants.py` (becomes a re-export shim)

### Task 1.1: Define the inspection model dataclasses

- [ ] **Step 1: Write the failing test**

Create `tests/test_unit/core/__init__.py` (empty) and `tests/test_unit/core/test_inspection.py`:

```python
"""Tests for core.inspection — the shared physical metadata model."""

from mountainash_data.core.inspection import (
    CatalogInfo,
    ColumnInfo,
    NamespaceInfo,
    TableInfo,
)


class TestColumnInfo:
    def test_minimal_column(self):
        col = ColumnInfo(name="id", type_name="int64", nullable=False)
        assert col.name == "id"
        assert col.type_name == "int64"
        assert col.nullable is False

    def test_column_with_metadata(self):
        col = ColumnInfo(
            name="created_at",
            type_name="timestamp",
            nullable=True,
            description="row creation time",
        )
        assert col.description == "row creation time"


class TestTableInfo:
    def test_table_with_columns(self):
        cols = [
            ColumnInfo(name="id", type_name="int64", nullable=False),
            ColumnInfo(name="name", type_name="string", nullable=True),
        ]
        table = TableInfo(name="users", columns=cols)
        assert table.name == "users"
        assert len(table.columns) == 2
        assert table.column_names == ["id", "name"]

    def test_table_qualified_name(self):
        table = TableInfo(
            name="users",
            columns=[],
            namespace="public",
            catalog="main",
        )
        assert table.qualified_name == "main.public.users"

    def test_table_qualified_name_no_catalog(self):
        table = TableInfo(name="users", columns=[], namespace="public")
        assert table.qualified_name == "public.users"

    def test_table_qualified_name_bare(self):
        table = TableInfo(name="users", columns=[])
        assert table.qualified_name == "users"


class TestNamespaceInfo:
    def test_namespace_with_tables(self):
        ns = NamespaceInfo(name="public", tables=["users", "orders"])
        assert ns.name == "public"
        assert ns.tables == ["users", "orders"]


class TestCatalogInfo:
    def test_catalog_with_namespaces(self):
        cat = CatalogInfo(
            name="main",
            namespaces=[NamespaceInfo(name="public", tables=["users"])],
        )
        assert cat.name == "main"
        assert len(cat.namespaces) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-quick tests/test_unit/core/test_inspection.py -v`
Expected: FAIL with `ModuleNotFoundError: mountainash_data.core`

- [ ] **Step 3: Create the core package and inspection module**

Create `src/mountainash_data/core/__init__.py` (empty for now).

Create `src/mountainash_data/core/inspection.py`:

```python
"""Shared physical-layer metadata model.

Both ibis and iceberg backends populate these dataclasses from their
native introspection APIs, giving consumers a uniform shape regardless
of which backend produced them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import typing as t


@dataclass(frozen=True)
class ColumnInfo:
    """Physical metadata for a single column."""

    name: str
    type_name: str
    nullable: bool
    description: t.Optional[str] = None
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TableInfo:
    """Physical metadata for a single table or view."""

    name: str
    columns: t.Sequence[ColumnInfo]
    namespace: t.Optional[str] = None
    catalog: t.Optional[str] = None
    description: t.Optional[str] = None
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def qualified_name(self) -> str:
        parts = [p for p in (self.catalog, self.namespace, self.name) if p]
        return ".".join(parts)


@dataclass(frozen=True)
class NamespaceInfo:
    """Physical metadata for a namespace (schema/database/dataset)."""

    name: str
    tables: t.Sequence[str]
    catalog: t.Optional[str] = None
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CatalogInfo:
    """Physical metadata for a top-level catalog or backend instance."""

    name: str
    namespaces: t.Sequence[NamespaceInfo]
    metadata: t.Mapping[str, t.Any] = field(default_factory=dict)
```

- [ ] **Step 4: Run tests**

Run: `hatch run test:test-quick tests/test_unit/core/test_inspection.py -v`
Expected: PASS, all 7 test functions green.

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/ tests/test_unit/core/
git commit -m "feat(data/core): add shared physical inspection model

CatalogInfo / NamespaceInfo / TableInfo / ColumnInfo dataclasses
populated by both ibis and iceberg backends. The Medium shared layer
between paradigms — see spec section 'Shared inspection model'."
```

### Task 1.2: Define the Backend Protocol

- [ ] **Step 1: Write the failing test**

Create `tests/test_unit/core/test_protocol.py`:

```python
"""Tests for core.protocol — structural Protocol definitions.

These tests verify that the Protocols are well-formed and that a minimal
fake implementation type-checks at runtime via isinstance() with
runtime_checkable Protocols.
"""

from __future__ import annotations

import typing as t

from mountainash_data.core.inspection import (
    CatalogInfo,
    NamespaceInfo,
    TableInfo,
)
from mountainash_data.core.protocol import Backend, Connection


class _FakeConnection:
    """Minimal in-memory Connection implementation for protocol verification."""

    def __init__(self):
        self.closed = False

    def list_namespaces(self) -> list[str]:
        return ["public"]

    def list_tables(self, namespace: str | None = None) -> list[str]:
        return ["users"]

    def inspect_table(self, name: str, namespace: str | None = None) -> TableInfo:
        return TableInfo(name=name, columns=[], namespace=namespace)

    def inspect_namespace(self, name: str) -> NamespaceInfo:
        return NamespaceInfo(name=name, tables=["users"])

    def inspect_catalog(self) -> CatalogInfo:
        return CatalogInfo(name="fake", namespaces=[])

    def close(self) -> None:
        self.closed = True


class _FakeBackend:
    """Minimal Backend implementation."""

    name = "fake"

    def connect(self) -> _FakeConnection:
        return _FakeConnection()


def test_fake_backend_satisfies_protocol():
    backend: Backend = _FakeBackend()
    assert backend.name == "fake"


def test_fake_connection_satisfies_protocol():
    conn: Connection = _FakeConnection()
    assert conn.list_namespaces() == ["public"]


def test_connection_inspect_returns_table_info():
    conn = _FakeConnection()
    info = conn.inspect_table("users", namespace="public")
    assert isinstance(info, TableInfo)
    assert info.name == "users"
    assert info.namespace == "public"


def test_connection_close_idempotent_marker():
    conn = _FakeConnection()
    assert conn.closed is False
    conn.close()
    assert conn.closed is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-quick tests/test_unit/core/test_protocol.py -v`
Expected: FAIL with `ImportError: cannot import name 'Backend' from 'mountainash_data.core.protocol'`

- [ ] **Step 3: Implement the Protocol module**

Create `src/mountainash_data/core/protocol.py`:

```python
"""Backend and Connection protocols.

This is the structural contract every backend implementation must
satisfy. Implementations are plain classes — there is no inheritance.
"""

from __future__ import annotations

import typing as t

from mountainash_data.core.inspection import (
    CatalogInfo,
    NamespaceInfo,
    TableInfo,
)


@t.runtime_checkable
class Connection(t.Protocol):
    """A live, owned connection to a backend.

    Connections are obtained by calling Backend.connect(). They expose
    physical introspection and lifecycle methods. Logical query
    construction is the job of mountainash-expressions, reached via
    to_relation() on backends that support it.
    """

    def list_namespaces(self) -> list[str]:
        """Return the names of all namespaces (schemas) visible to this connection."""
        ...

    def list_tables(self, namespace: str | None = None) -> list[str]:
        """Return the names of tables in the given namespace."""
        ...

    def inspect_table(
        self, name: str, namespace: str | None = None
    ) -> TableInfo:
        """Return shared-model metadata for one table."""
        ...

    def inspect_namespace(self, name: str) -> NamespaceInfo:
        """Return shared-model metadata for one namespace."""
        ...

    def inspect_catalog(self) -> CatalogInfo:
        """Return shared-model metadata for the connection's catalog."""
        ...

    def close(self) -> None:
        """Release the connection. Idempotent."""
        ...


@t.runtime_checkable
class Backend(t.Protocol):
    """A factory for Connections to a particular backend service.

    Backends are constructed with config and are stateless from the
    consumer's perspective. State lives on the Connection returned by
    connect().
    """

    name: str

    def connect(self) -> Connection:
        """Open a connection. Caller is responsible for closing it."""
        ...
```

- [ ] **Step 4: Run tests**

Run: `hatch run test:test-quick tests/test_unit/core/test_protocol.py -v`
Expected: PASS, all 4 test functions green.

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/protocol.py tests/test_unit/core/test_protocol.py
git commit -m "feat(data/core): add Backend and Connection protocols

Structural typing — implementations satisfy the protocol by shape, no
inheritance required. Replaces the BaseDBConnection inheritance tree."
```

### Task 1.3: Add an empty registry placeholder

- [ ] **Step 1: Create the placeholder module**

Create `src/mountainash_data/core/registry.py`:

```python
"""Backend registry — populated in Phase 4 once IbisBackend and
IcebergBackend exist. This module is intentionally a placeholder for
now so that imports from core.registry don't break across phases."""

from __future__ import annotations

import typing as t

from mountainash_data.core.protocol import Backend

_REGISTRY: dict[str, t.Callable[..., Backend]] = {}


def register(name: str, factory: t.Callable[..., Backend]) -> None:
    """Register a backend factory under a name."""
    _REGISTRY[name] = factory


def get(name: str, **config: t.Any) -> Backend:
    """Look up and instantiate a backend by name."""
    if name not in _REGISTRY:
        raise KeyError(
            f"No backend registered as {name!r}. "
            f"Available: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name](**config)


def names() -> list[str]:
    return sorted(_REGISTRY)
```

- [ ] **Step 2: Run tests**

Run: `hatch run test:test`
Expected: PASS (no new tests yet — registry has no consumers until Phase 4).

- [ ] **Step 3: Commit**

```bash
git add src/mountainash_data/core/registry.py
git commit -m "feat(data/core): add backend registry placeholder

Wired up in Phase 4 once IbisBackend and IcebergBackend exist."
```

### Task 1.4: Move `databases/constants.py` to `core/constants.py` with shim

- [ ] **Step 1: Copy file to new location**

```bash
cp src/mountainash_data/databases/constants.py src/mountainash_data/core/constants.py
```

- [ ] **Step 2: Replace old file with a re-export shim**

Overwrite `src/mountainash_data/databases/constants.py`:

```python
"""DEPRECATED: import from mountainash_data.core.constants instead.

This shim exists during the Phase 1–6 refactor and will be removed in
Phase 6.
"""

from mountainash_data.core.constants import *  # noqa: F401,F403
from mountainash_data.core.constants import (  # noqa: F401  # explicit re-exports
    # Re-export every public symbol the old file exported. Read
    # core/constants.py and add each enum/class name here so that
    # `from mountainash_data.databases.constants import X` continues to work.
)
```

After writing the shim above, read `src/mountainash_data/core/constants.py` and list every top-level enum/class/constant. Add each name to the explicit re-exports list inside the parentheses.

- [ ] **Step 3: Run tests**

Run: `hatch run test:test`
Expected: PASS, baseline test count unchanged.

If anything fails with `ImportError`, the explicit re-export list is missing a symbol — add it and re-run.

- [ ] **Step 4: Commit**

```bash
git add src/mountainash_data/core/constants.py src/mountainash_data/databases/constants.py
git commit -m "refactor(data): move constants to core/, leave shim at databases/"
```

### Task 1.5: Move `databases/connections/base_db_connection.py` to `core/connection.py` with shim

Note: this file (231 LOC) defines `BaseDBConnection`, the abstract base of the old inheritance tree. In the new architecture, `core/protocol.py` (the Backend Protocol) is the authoritative contract. `BaseDBConnection` is moved verbatim for now so the old inheritance tree keeps working through Phase 5; it gets reduced or removed in Phase 6 when shims come down.

- [ ] **Step 1: Copy file**

```bash
cp src/mountainash_data/databases/connections/base_db_connection.py \
   src/mountainash_data/core/connection.py
```

- [ ] **Step 2: Update internal imports in the new `core/connection.py`**

Read the new `src/mountainash_data/core/connection.py`. Any imports from `mountainash_data.databases.constants` should be rewritten to `mountainash_data.core.constants`. Any imports from `mountainash_data.databases.connections.base_db_connection` (self-references) stay relative or are unaffected.

Also: any `from mountainash_data.databases.settings...` imports should be left as-is for now (settings move in Phase 2). They'll work via shims later.

- [ ] **Step 3: Replace old file with shim**

Overwrite `src/mountainash_data/databases/connections/base_db_connection.py`:

```python
"""DEPRECATED: import from mountainash_data.core.connection instead.

This shim exists during the Phase 1–6 refactor.
"""

from mountainash_data.core.connection import *  # noqa: F401,F403
from mountainash_data.core.connection import BaseDBConnection  # noqa: F401
```

If `core/connection.py` defines additional public symbols (read it and confirm), add them to the explicit re-export list above.

- [ ] **Step 4: Run tests**

Run: `hatch run test:test`
Expected: PASS, baseline test count unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/core/connection.py src/mountainash_data/databases/connections/base_db_connection.py
git commit -m "refactor(data): move BaseDBConnection to core/connection.py with shim

The Backend Protocol in core/protocol.py is the new authoritative
contract. BaseDBConnection is preserved during the migration so the
existing ibis/pyiceberg subclasses keep working through Phase 5."
```

### Task 1.6: Phase 1 sanity check

- [ ] **Step 1: Run full test suite**

Run: `hatch run test:test`
Expected: PASS, count matches baseline.

- [ ] **Step 2: Run lint and type-check**

Run: `hatch run ruff:check`
Expected: PASS (or only pre-existing issues unrelated to new files).

Run: `hatch run mypy:check`
Expected: PASS or only pre-existing issues.

If new files have ruff/mypy issues, fix them before proceeding.

---

## Phase 2 — Move settings to `core/settings/`

**Files touched:**
- Create: `src/mountainash_data/core/settings/__init__.py` and one file per existing settings module (16 files)
- Modify: `src/mountainash_data/databases/settings/*.py` (each becomes a one-line shim)

**Strategy:** Bulk move with shims. There is no logic change in this phase. The 16 files in `databases/settings/` are well-structured Pydantic settings classes per the audit.

### Task 2.1: Move all settings files

- [ ] **Step 1: Create the new directory and copy all files**

```bash
mkdir -p src/mountainash_data/core/settings
cp src/mountainash_data/databases/settings/*.py src/mountainash_data/core/settings/
```

- [ ] **Step 2: Update internal imports inside `core/settings/`**

Run: `grep -rn "from mountainash_data.databases.settings\|import mountainash_data.databases.settings" src/mountainash_data/core/settings/`

For every match, rewrite the import to use `mountainash_data.core.settings` instead. Also rewrite any `from mountainash_data.databases.constants` → `from mountainash_data.core.constants`.

Do NOT touch imports from `mountainash_settings` (the external sister package) — only the in-package imports.

- [ ] **Step 3: Replace each old settings file with a shim**

For each of the 16 files in `src/mountainash_data/databases/settings/` (excluding `__init__.py` which gets special handling below), overwrite the contents with:

```python
"""DEPRECATED: import from mountainash_data.core.settings.<modulename> instead."""

from mountainash_data.core.settings.<modulename> import *  # noqa: F401,F403
```

Where `<modulename>` is the file's name without `.py`. For each shim, after writing it, also add explicit re-exports of every public class/symbol the original file exported (read the corresponding `core/settings/<file>.py` to enumerate them).

The 16 files are:
- `__init__.py`
- `base.py`
- `exceptions.py`
- `templates.py`
- `bigquery.py`
- `duckdb.py`
- `motherduck.py`
- `mssql.py`
- `mysql.py`
- `postgresql.py`
- `pyiceberg_rest.py`
- `pyspark.py`
- `redshift.py`
- `snowflake.py`
- `sqlite.py`
- `trino.py`

For `databases/settings/__init__.py` specifically, the shim should re-export everything `core/settings/__init__.py` exports:

```python
"""DEPRECATED: import from mountainash_data.core.settings instead."""

from mountainash_data.core.settings import *  # noqa: F401,F403
```

Then read `core/settings/__init__.py` and add explicit re-exports for every class name in its `__all__` (or every class it imports).

- [ ] **Step 4: Run tests**

Run: `hatch run test:test`
Expected: PASS, count unchanged.

If `test_settings_parametrized.py` fails with import errors, the shim is missing a symbol. Add it.

- [ ] **Step 5: Run lint**

Run: `hatch run ruff:check src/mountainash_data/core/settings/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/mountainash_data/core/settings/ src/mountainash_data/databases/settings/
git commit -m "refactor(data): move settings to core/settings/ with shims

16 files moved verbatim. databases/settings/ retains shims that
re-export from the new location, removed in Phase 6."
```

---

## Phase 3 — Iceberg backend (D1.b: split, deduplicate)

**Files touched:**
- Create: `src/mountainash_data/backends/__init__.py`
- Create: `src/mountainash_data/backends/iceberg/__init__.py`
- Create: `src/mountainash_data/backends/iceberg/connection.py`
- Create: `src/mountainash_data/backends/iceberg/operations.py`
- Create: `src/mountainash_data/backends/iceberg/_types.py`
- Create: `src/mountainash_data/backends/iceberg/inspect.py`
- Create: `src/mountainash_data/backends/iceberg/backend.py`
- Create: `src/mountainash_data/backends/iceberg/catalogs/__init__.py`
- Create: `src/mountainash_data/backends/iceberg/catalogs/rest.py`
- Create: `tests/test_unit/backends/iceberg/test_backend.py`
- Create: `tests/test_unit/backends/iceberg/test_inspect.py`
- Modify: `src/mountainash_data/databases/connections/pyiceberg/base_pyiceberg_connection.py` (becomes shim)
- Modify: `src/mountainash_data/databases/connections/pyiceberg/pyiceberg_rest_connection.py` (becomes shim)
- Modify: `src/mountainash_data/databases/operations/pyiceberg/base_pyiceberg_operations.py` (becomes shim)
- Modify: `src/mountainash_data/databases/operations/pyiceberg/pyiceberg_rest_operations.py` (becomes shim)

### Task 3.1: Audit existing iceberg test coverage

- [ ] **Step 1: Inventory existing iceberg tests**

Run: `find tests -path '*pyiceberg*' -o -path '*iceberg*' | head`
Expected: likely zero results (the audit found no iceberg-specific test files).

- [ ] **Step 2: Document the gap**

Create `tests/test_unit/backends/iceberg/__init__.py` (empty) and `tests/test_unit/backends/iceberg/COVERAGE_GAP.md`:

```markdown
# Iceberg test coverage gap

Before Phase 3 (writing-plans audit), no tests existed for any of:

- `databases/connections/pyiceberg/base_pyiceberg_connection.py` (884 LOC)
- `databases/connections/pyiceberg/pyiceberg_rest_connection.py` (205 LOC)
- `databases/operations/pyiceberg/base_pyiceberg_operations.py` (868 LOC)
- `databases/operations/pyiceberg/pyiceberg_rest_operations.py` (207 LOC)

The Phase 3 refactor proceeds *without* a regression net for iceberg.
This is acceptable because the user (sole consumer) confirmed iceberg
is in prototype use only. Tests added during this phase target the new
shape (IcebergBackend protocol, inspection model conversion) rather
than reproducing legacy behavior.

If iceberg moves to production use later, a separate hardening pass
should add tests for the salvaged operations.
```

- [ ] **Step 3: Commit the gap doc**

```bash
git add tests/test_unit/backends/iceberg/__init__.py tests/test_unit/backends/iceberg/COVERAGE_GAP.md
git commit -m "docs(test): document iceberg test coverage gap before Phase 3 refactor"
```

### Task 3.2: Initial verbatim copy of iceberg files into new layout

This task is a **bulk move** to get all the source code into `backends/iceberg/`. Deduplication and splitting happen in Task 3.3.

- [ ] **Step 1: Create the new directory structure**

```bash
mkdir -p src/mountainash_data/backends/iceberg/catalogs
touch src/mountainash_data/backends/__init__.py
touch src/mountainash_data/backends/iceberg/__init__.py
touch src/mountainash_data/backends/iceberg/catalogs/__init__.py
```

- [ ] **Step 2: Copy the four source files into a staging location inside the new dir**

```bash
cp src/mountainash_data/databases/connections/pyiceberg/base_pyiceberg_connection.py \
   src/mountainash_data/backends/iceberg/_legacy_connection.py
cp src/mountainash_data/databases/operations/pyiceberg/base_pyiceberg_operations.py \
   src/mountainash_data/backends/iceberg/_legacy_operations.py
cp src/mountainash_data/databases/connections/pyiceberg/pyiceberg_rest_connection.py \
   src/mountainash_data/backends/iceberg/_legacy_rest_connection.py
cp src/mountainash_data/databases/operations/pyiceberg/pyiceberg_rest_operations.py \
   src/mountainash_data/backends/iceberg/_legacy_rest_operations.py
```

The `_legacy_*` files exist for the duration of Task 3.3 only. They are deleted at the end of Task 3.3.

- [ ] **Step 3: Update internal imports in the legacy copies**

Run: `grep -n "from mountainash_data" src/mountainash_data/backends/iceberg/_legacy_*.py`

For every match: rewrite `mountainash_data.databases.connections.base_db_connection` → `mountainash_data.core.connection`, `mountainash_data.databases.constants` → `mountainash_data.core.constants`, `mountainash_data.databases.settings` → `mountainash_data.core.settings`. Leave any cross-`_legacy_*` imports as relative imports within the new directory (e.g., `from mountainash_data.backends.iceberg._legacy_connection import ...`).

- [ ] **Step 4: Smoke-import the staging files**

Create a temporary smoke test `tests/test_unit/backends/iceberg/test_smoke_import.py`:

```python
"""Smoke test: verify the legacy iceberg files can be imported in their new location."""

def test_legacy_imports():
    from mountainash_data.backends.iceberg import (
        _legacy_connection,
        _legacy_operations,
        _legacy_rest_connection,
        _legacy_rest_operations,
    )
    assert _legacy_connection is not None
    assert _legacy_operations is not None
    assert _legacy_rest_connection is not None
    assert _legacy_rest_operations is not None
```

Run: `hatch run test:test-quick tests/test_unit/backends/iceberg/test_smoke_import.py -v`
Expected: PASS.

If it fails with `ImportError`, fix the imports inside the legacy files until it passes. Do NOT proceed to Task 3.3 until the smoke test is green.

- [ ] **Step 5: Commit the staging copies**

```bash
git add src/mountainash_data/backends/ tests/test_unit/backends/iceberg/test_smoke_import.py
git commit -m "refactor(data/iceberg): stage legacy iceberg files in new backends/ location

Verbatim copies as _legacy_*.py. Deduplication and split into
connection/operations/types happens in the next task."
```

### Task 3.3: Deduplicate and split (the substantial refactor)

This is the hardest task in the plan. The two legacy files share methods (the spec confirmed the previous split was half done). This task identifies duplicates, picks canonical versions, and reorganizes by responsibility.

- [ ] **Step 1: Build a method inventory of both legacy files**

Read `src/mountainash_data/backends/iceberg/_legacy_connection.py` and `_legacy_operations.py`. Produce a markdown table in scratch (do not commit) with columns: `method_name`, `in_legacy_connection`, `in_legacy_operations`, `responsibility` where responsibility is one of: `lifecycle` (connect/disconnect/catalog navigation), `mutation` (create/drop/insert/upsert/truncate/views), `inspection` (list/describe), `types` (Iceberg→PyArrow conversion), `helper`.

- [ ] **Step 2: Identify duplicates and pick canonical versions**

For every method that appears in both files, read both implementations. Pick the more complete / more recently modified version as canonical. Note the choice in the scratch table. If the implementations diverge in non-trivial ways (different error handling, different retry logic), STOP and surface the diff to the user — do not silently choose.

- [ ] **Step 3: Create `backends/iceberg/_types.py`**

Move all Iceberg→PyArrow type conversion helpers (the ~15 type variants the audit identified) from `_legacy_connection.py` and `_legacy_operations.py` (whichever has the canonical version) into a new file:

Create `src/mountainash_data/backends/iceberg/_types.py`:

```python
"""Iceberg ↔ PyArrow type conversion helpers.

Extracted from the legacy base_pyiceberg_connection.py / base_pyiceberg_operations.py
during the Phase 3 deduplication. These functions take Iceberg type
objects and return their PyArrow equivalents (and vice versa where
needed).
"""

from __future__ import annotations

# Move the canonical type-conversion functions here.
# Function names to preserve (confirm by reading the legacy files):
#   - _iceberg_to_pyarrow_schema
#   - _iceberg_to_pyarrow_type
#   - _pyarrow_to_iceberg_type
#   - any helpers for nested struct/map/list conversion
#
# DO NOT change function signatures or behavior. This is a pure move.
```

Replace the placeholder block with the actual canonical functions copied from the legacy files. Preserve docstrings.

- [ ] **Step 4: Create `backends/iceberg/connection.py`**

Create `src/mountainash_data/backends/iceberg/connection.py` and move into it everything classified as `lifecycle` or `inspection` from the canonical legacy versions:

```python
"""Iceberg connection: catalog/namespace lifecycle and read-side inspection.

Created in Phase 3 by deduplicating and splitting the legacy
base_pyiceberg_connection.py and base_pyiceberg_operations.py.
"""

from __future__ import annotations

import typing as t

from mountainash_data.backends.iceberg._types import (
    # import the type helpers needed by inspection
)
from mountainash_data.core.inspection import (
    CatalogInfo,
    NamespaceInfo,
    TableInfo,
)

# Move into this module:
#   - the connection lifecycle (open, close, catalog handle accessor)
#   - list_namespaces, list_tables
#   - load_table / get_table (the read accessor used by inspection and ops)
#   - any caching infrastructure that supports the above (schema cache, etc.)
#   - inspection helpers that build TableInfo from a loaded iceberg Table
#
# Class shape: keep an `IcebergConnectionBase` (or whatever the canonical
# legacy name was) — this is the class IcebergRestConnection subclasses
# in catalogs/rest.py.
```

Replace the placeholder with the actual extracted code. Update internal references: methods that previously called `self._upsert(...)` (which is now in operations.py) need to be left in place — operations.py will keep them on the same class instance via composition or mixin (see Step 6 below).

- [ ] **Step 5: Create `backends/iceberg/operations.py`**

Create `src/mountainash_data/backends/iceberg/operations.py` and move into it everything classified as `mutation`:

```python
"""Iceberg table mutations: create, drop, insert, upsert, truncate, view ops.

Created in Phase 3 by deduplicating and splitting the legacy
base_pyiceberg_connection.py and base_pyiceberg_operations.py.

These functions take an active connection (or table handle) and perform
mutations. They are NOT class methods — they are module-level functions
that the connection class composes when consumers call e.g.
connection.insert(...).
"""

from __future__ import annotations

import typing as t

from mountainash_data.backends.iceberg._types import (
    # import type helpers needed by mutations
)

# Move into this module:
#   - create_table / drop_table
#   - insert / upsert / truncate (with their snapshot retry logic)
#   - create_view / drop_view
#   - any helpers specific to mutations (natural key validation, etc.)
#
# Pattern: convert each from a method on the legacy class into a
# function that takes the connection as its first arg:
#
#   def insert(connection, table_name: str, df, namespace: str | None = None) -> None:
#       table = connection.load_table(table_name, namespace)
#       ...
#
# Then in connection.py, the IcebergConnectionBase class exposes
# thin wrappers:
#
#   def insert(self, table_name, df, namespace=None):
#       from mountainash_data.backends.iceberg import operations
#       return operations.insert(self, table_name, df, namespace)
```

Replace the placeholder with the actual extracted functions. The conversion pattern (method → function-taking-self-equivalent) is described in the docstring above.

- [ ] **Step 6: Wire connection.py to call operations.py**

Read `backends/iceberg/connection.py`. For every mutation method that used to live there but now lives in `operations.py`, add a thin wrapper method on `IcebergConnectionBase` that delegates:

```python
def insert(self, table_name: str, df, namespace: str | None = None) -> None:
    from mountainash_data.backends.iceberg import operations
    return operations.insert(self, table_name, df, namespace)
```

Use a local import inside each wrapper to avoid circular import issues between `connection.py` and `operations.py` (operations.py needs to type-hint `Connection` but doesn't need to import the class).

Do this for every mutation: `insert`, `upsert`, `truncate`, `create_table`, `drop_table`, `create_view`, `drop_view`. Confirm by reading both files that no mutation is orphaned.

- [ ] **Step 7: Move REST-specific code into `catalogs/rest.py`**

Create `src/mountainash_data/backends/iceberg/catalogs/rest.py`:

```python
"""REST catalog implementation.

Merges the legacy pyiceberg_rest_connection.py and pyiceberg_rest_operations.py
into a single concrete connection class for the REST catalog.
"""

from __future__ import annotations

from mountainash_data.backends.iceberg.connection import IcebergConnectionBase
# Add other imports as needed.


class IcebergRestConnection(IcebergConnectionBase):
    # Move the concrete REST initialization, any REST-specific overrides
    # (the audit identified _list_tables and _upsert overrides in
    # pyiceberg_rest_operations.py — those go here as method overrides
    # OR as registry entries depending on whether they're worth keeping
    # as overrides).
    pass
```

Replace the placeholder with the actual code from `_legacy_rest_connection.py` and `_legacy_rest_operations.py`. If the legacy `_upsert` override in `_legacy_rest_operations.py` (with natural key validation per the audit) is meaningfully different from the canonical `upsert` in `operations.py`, preserve it as an override on `IcebergRestConnection`.

- [ ] **Step 8: Create `backends/iceberg/inspect.py`**

Create `src/mountainash_data/backends/iceberg/inspect.py`:

```python
"""Iceberg → core.inspection conversion.

Helpers that take pyiceberg Table objects and produce TableInfo /
NamespaceInfo / CatalogInfo dataclasses from core.inspection.
"""

from __future__ import annotations

import typing as t

from mountainash_data.core.inspection import (
    CatalogInfo,
    ColumnInfo,
    NamespaceInfo,
    TableInfo,
)


def table_to_info(
    iceberg_table,
    *,
    name: str,
    namespace: str | None = None,
    catalog: str | None = None,
) -> TableInfo:
    """Convert a pyiceberg Table object into a TableInfo."""
    columns = [
        ColumnInfo(
            name=field.name,
            type_name=str(field.field_type),
            nullable=not field.required,
        )
        for field in iceberg_table.schema().fields
    ]
    return TableInfo(
        name=name,
        columns=columns,
        namespace=namespace,
        catalog=catalog,
    )
```

(Confirm field/method names by reading the legacy `_legacy_connection.py` — it already builds schemas from iceberg Tables, so the existing logic is the source of truth.)

- [ ] **Step 9: Create `backends/iceberg/backend.py` — the IcebergBackend Protocol implementation**

Create `src/mountainash_data/backends/iceberg/backend.py`:

```python
"""IcebergBackend — implements core.protocol.Backend for iceberg catalogs."""

from __future__ import annotations

import typing as t

from mountainash_data.backends.iceberg.catalogs.rest import IcebergRestConnection
from mountainash_data.backends.iceberg.connection import IcebergConnectionBase
from mountainash_data.core.protocol import Backend, Connection


_CATALOG_REGISTRY: dict[str, type[IcebergConnectionBase]] = {
    "rest": IcebergRestConnection,
}


class IcebergBackend:
    """Iceberg backend factory.

    Construction takes a catalog type (e.g. 'rest') and config. connect()
    returns a live IcebergConnection that implements core.protocol.Connection.
    """

    name = "iceberg"

    def __init__(self, catalog: str, **config: t.Any):
        if catalog not in _CATALOG_REGISTRY:
            raise KeyError(
                f"Unknown iceberg catalog type {catalog!r}. "
                f"Available: {sorted(_CATALOG_REGISTRY)}"
            )
        self._catalog_cls = _CATALOG_REGISTRY[catalog]
        self._config = config

    def connect(self) -> Connection:
        return self._catalog_cls(**self._config)
```

- [ ] **Step 10: Write a test for IcebergBackend instantiation and protocol satisfaction**

Create `tests/test_unit/backends/iceberg/test_backend.py`:

```python
"""Tests for IcebergBackend factory."""

import pytest

from mountainash_data.backends.iceberg.backend import IcebergBackend
from mountainash_data.core.protocol import Backend


def test_iceberg_backend_satisfies_protocol():
    backend = IcebergBackend(catalog="rest", uri="http://localhost:8181")
    assert isinstance(backend, Backend)
    assert backend.name == "iceberg"


def test_unknown_catalog_raises():
    with pytest.raises(KeyError, match="Unknown iceberg catalog"):
        IcebergBackend(catalog="bogus")


def test_iceberg_backend_carries_config():
    backend = IcebergBackend(catalog="rest", uri="http://localhost:8181", token="abc")
    assert backend._config == {"uri": "http://localhost:8181", "token": "abc"}
```

- [ ] **Step 11: Run the new tests**

Run: `hatch run test:test-quick tests/test_unit/backends/iceberg/test_backend.py -v`
Expected: PASS, 3 tests green.

If `test_iceberg_backend_satisfies_protocol` fails because the structural check finds missing methods, the issue is that `IcebergConnectionBase` (returned by `connect()`) doesn't yet expose all six Connection protocol methods (`list_namespaces`, `list_tables`, `inspect_table`, `inspect_namespace`, `inspect_catalog`, `close`). Add the missing ones — they likely wrap existing legacy methods or use `inspect.py` helpers. Read the failure carefully and add only what's missing.

Note: this test does NOT actually call `.connect()` — it just verifies the type and that construction validates. Actual end-to-end connection tests against a real iceberg catalog are out of scope for this refactor (the iceberg coverage gap is documented in Task 3.1).

- [ ] **Step 12: Delete the `_legacy_*.py` staging files**

Once `connection.py`, `operations.py`, `_types.py`, `inspect.py`, `backend.py`, and `catalogs/rest.py` collectively contain everything from the four legacy files, delete the staging copies:

```bash
rm src/mountainash_data/backends/iceberg/_legacy_connection.py
rm src/mountainash_data/backends/iceberg/_legacy_operations.py
rm src/mountainash_data/backends/iceberg/_legacy_rest_connection.py
rm src/mountainash_data/backends/iceberg/_legacy_rest_operations.py
rm tests/test_unit/backends/iceberg/test_smoke_import.py
```

- [ ] **Step 13: Replace the original four files with shims**

Overwrite `src/mountainash_data/databases/connections/pyiceberg/base_pyiceberg_connection.py`:

```python
"""DEPRECATED: import from mountainash_data.backends.iceberg.connection instead."""

from mountainash_data.backends.iceberg.connection import *  # noqa: F401,F403
from mountainash_data.backends.iceberg.connection import IcebergConnectionBase  # noqa: F401
# Add any other public symbols the original file exported.
```

Read the original file's public surface (class names, function names) and add explicit re-exports for each.

Repeat for:
- `databases/connections/pyiceberg/pyiceberg_rest_connection.py` → re-export from `backends/iceberg/catalogs/rest`
- `databases/operations/pyiceberg/base_pyiceberg_operations.py` → re-export from `backends/iceberg/operations` (note: operations are now functions, not class methods — if any consumer was importing a class, this shim will not satisfy them and you'll see test failures pinpointing the affected importer)
- `databases/operations/pyiceberg/pyiceberg_rest_operations.py` → re-export from `backends/iceberg/catalogs/rest`

- [ ] **Step 14: Run the full test suite**

Run: `hatch run test:test`
Expected: PASS, baseline count.

If anything fails:
- Import errors → a shim is missing a symbol; add it.
- Class-not-found errors on operations → an old test or factory was importing `BasePyIcebergOperations` as a class. The simplest fix is to keep a thin compatibility class in `databases/operations/pyiceberg/base_pyiceberg_operations.py` (the shim) that wraps the new module-level functions. Add it only if needed.
- Behavior failures (assertions on returned values) → the deduplication picked the wrong canonical version of a method. Stop and re-read both legacy versions in the git history to compare.

- [ ] **Step 15: Run lint**

Run: `hatch run ruff:check src/mountainash_data/backends/`
Expected: PASS.

- [ ] **Step 16: Commit**

```bash
git add -A
git commit -m "refactor(data/iceberg): split and deduplicate iceberg backend (D1.b)

Connection lifecycle in backends/iceberg/connection.py.
Table mutations in backends/iceberg/operations.py (now module
functions, called via thin wrappers on the connection class).
Iceberg→PyArrow types in _types.py.
REST catalog in catalogs/rest.py.
Inspection model conversion in inspect.py.
IcebergBackend Protocol implementation in backend.py.

Closes the half-done split that left duplicate methods between the
legacy connection (884 LOC) and operations (868 LOC) files. Original
locations retained as shims through Phase 6.

to_relation() not implemented — gap documented in spec; requires
mountainash-expressions to gain an iceberg adapter."
```

---

## Phase 4 — Ibis backend (the largest phase)

**Files touched:**
- Create: `src/mountainash_data/backends/ibis/__init__.py`
- Create: `src/mountainash_data/backends/ibis/connection.py` (from `base_ibis_connection.py`)
- Create: `src/mountainash_data/backends/ibis/operations.py` (merges base + mixins)
- Create: `src/mountainash_data/backends/ibis/inspect.py`
- Create: `src/mountainash_data/backends/ibis/backend.py`
- Create: `src/mountainash_data/backends/ibis/dialects/__init__.py`
- Create: `src/mountainash_data/backends/ibis/dialects/_registry.py`
- Modify: 13 ibis connection shims, 4 ibis ops shims (duckdb/sqlite/motherduck/trino)
- Modify: `factories/operations_factory.py` (point at new IbisBackend)

### Task 4.1: Audit ibis test coverage and resolve D3 (HYBRID mode)

- [ ] **Step 1: Inventory ibis tests**

Run: `find tests -path '*ibis*' -name '*.py'`
Expected (based on the audit): a handful of files including `test_ibis_backends.py`, `test_connection_lifecycle.py`, `test_base_ibis_operations.py`, `test_upsert_and_indexes.py`.

- [ ] **Step 2: Run the existing ibis tests in isolation**

Run: `hatch run test:test-quick tests/test_unit/databases/connections/ibis/ tests/test_unit/databases/operations/ -v`
Expected: PASS, record the test count.

This is the regression net Phase 4 must keep green.

- [ ] **Step 3: Resolve D3 — grep for `_ibis_connection_mode` usage**

Run: `grep -rn "_ibis_connection_mode\|ibis_connection_mode\|HYBRID" src/`

Read every match. Determine: is `HYBRID` set anywhere except in `trino_ibis_operations.py`? If yes, it's a general capability and goes in the registry as a per-dialect field. If no, it's trino-specific and goes in trino's `DialectSpec` entry only.

Record the answer in a scratch note for Task 4.4.

### Task 4.2: Define the DialectSpec dataclass

- [ ] **Step 1: Write the failing test**

Create `tests/test_unit/backends/ibis/__init__.py` (empty) and `tests/test_unit/backends/ibis/test_dialect_spec.py`:

```python
"""Tests for the DialectSpec dataclass — the data-driven replacement
for the per-backend connection class explosion."""

from mountainash_data.backends.ibis.dialects._registry import (
    DialectSpec,
    DIALECTS,
)


def test_dialect_spec_minimal():
    spec = DialectSpec(
        ibis_backend_name="sqlite",
        connection_mode="DIRECT",
        connection_string_scheme="sqlite",
    )
    assert spec.ibis_backend_name == "sqlite"
    assert spec.connection_mode == "DIRECT"
    assert spec.get_index_exists_sql is None
    assert spec.get_list_indexes_sql is None


def test_dialect_spec_with_capability_hooks():
    def fake_index_sql(table_name, index_name):
        return f"SELECT 1 FROM {table_name}"

    spec = DialectSpec(
        ibis_backend_name="duckdb",
        connection_mode="DIRECT",
        connection_string_scheme="duckdb",
        get_index_exists_sql=fake_index_sql,
    )
    assert spec.get_index_exists_sql is not None
    assert spec.get_index_exists_sql("users", "idx_users_id") == "SELECT 1 FROM users"


def test_registry_contains_all_12_backends():
    expected = {
        "sqlite", "duckdb", "motherduck", "postgres", "mysql", "mssql",
        "oracle", "snowflake", "bigquery", "redshift", "trino", "pyspark",
    }
    assert set(DIALECTS.keys()) == expected


def test_registry_entries_are_dialect_specs():
    for name, spec in DIALECTS.items():
        assert isinstance(spec, DialectSpec), f"{name} entry is not a DialectSpec"
        assert spec.ibis_backend_name, f"{name} missing ibis_backend_name"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run test:test-quick tests/test_unit/backends/ibis/test_dialect_spec.py -v`
Expected: FAIL with `ModuleNotFoundError: mountainash_data.backends.ibis.dialects._registry`

- [ ] **Step 3: Create the registry module with DialectSpec and stub entries**

```bash
mkdir -p src/mountainash_data/backends/ibis/dialects
touch src/mountainash_data/backends/ibis/__init__.py
touch src/mountainash_data/backends/ibis/dialects/__init__.py
```

Create `src/mountainash_data/backends/ibis/dialects/_registry.py`:

```python
"""Data-driven dialect registry. Replaces the 13 per-backend connection
classes from databases/connections/ibis/.

Each entry is a DialectSpec containing the connection-builder callable,
ibis backend name, connection mode, and any backend-specific capability
hooks (e.g. dialect-specific index introspection SQL).
"""

from __future__ import annotations

from dataclasses import dataclass, field
import typing as t


# Capability hook signatures
GetIndexExistsSql = t.Callable[[str, str], str]  # (table_name, index_name) -> SQL
GetListIndexesSql = t.Callable[[str], str]  # (table_name) -> SQL
ConnectionBuilder = t.Callable[..., t.Any]  # (**config) -> ibis backend connection


@dataclass(frozen=True)
class DialectSpec:
    """Per-dialect configuration and capability hooks."""

    ibis_backend_name: str
    connection_mode: str
    connection_string_scheme: str
    connection_builder: t.Optional[ConnectionBuilder] = None
    get_index_exists_sql: t.Optional[GetIndexExistsSql] = None
    get_list_indexes_sql: t.Optional[GetListIndexesSql] = None
    extras: t.Mapping[str, t.Any] = field(default_factory=dict)


# Stub entries — populated with real connection_builder callables in
# Task 4.4 by salvaging logic from the per-backend connection files.
DIALECTS: dict[str, DialectSpec] = {
    "sqlite": DialectSpec(
        ibis_backend_name="sqlite",
        connection_mode="DIRECT",
        connection_string_scheme="sqlite",
    ),
    "duckdb": DialectSpec(
        ibis_backend_name="duckdb",
        connection_mode="DIRECT",
        connection_string_scheme="duckdb",
    ),
    "motherduck": DialectSpec(
        ibis_backend_name="duckdb",
        connection_mode="DIRECT",
        connection_string_scheme="md",
    ),
    "postgres": DialectSpec(
        ibis_backend_name="postgres",
        connection_mode="DIRECT",
        connection_string_scheme="postgresql",
    ),
    "mysql": DialectSpec(
        ibis_backend_name="mysql",
        connection_mode="DIRECT",
        connection_string_scheme="mysql",
    ),
    "mssql": DialectSpec(
        ibis_backend_name="mssql",
        connection_mode="DIRECT",
        connection_string_scheme="mssql",
    ),
    "oracle": DialectSpec(
        ibis_backend_name="oracle",
        connection_mode="DIRECT",
        connection_string_scheme="oracle",
    ),
    "snowflake": DialectSpec(
        ibis_backend_name="snowflake",
        connection_mode="DIRECT",
        connection_string_scheme="snowflake",
    ),
    "bigquery": DialectSpec(
        ibis_backend_name="bigquery",
        connection_mode="DIRECT",
        connection_string_scheme="bigquery",
    ),
    "redshift": DialectSpec(
        ibis_backend_name="postgres",  # Redshift uses postgres protocol
        connection_mode="DIRECT",
        connection_string_scheme="redshift",
    ),
    "trino": DialectSpec(
        ibis_backend_name="trino",
        connection_mode="HYBRID",  # confirmed by Task 4.1 D3 grep
        connection_string_scheme="trino",
    ),
    "pyspark": DialectSpec(
        ibis_backend_name="pyspark",
        connection_mode="DIRECT",
        connection_string_scheme="spark",
    ),
}
```

If Task 4.1 step 3 found that HYBRID is general (not trino-only), update the relevant entries; otherwise keep it on trino only as shown.

Note on `connection_string_scheme`: confirm each scheme by reading the corresponding `databases/connections/ibis/<backend>_ibis_connection.py` file and copying the actual `connection_string_scheme` constant. The values above are educated guesses — replace any that disagree with the source.

- [ ] **Step 4: Run tests**

Run: `hatch run test:test-quick tests/test_unit/backends/ibis/test_dialect_spec.py -v`
Expected: PASS, 4 tests green.

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/backends/ibis/ tests/test_unit/backends/ibis/
git commit -m "feat(data/ibis): add DialectSpec and stub registry for 12 backends

DialectSpec is the data-driven replacement for the 13 per-backend
connection classes. Connection builders and capability hooks are
populated in Task 4.4."
```

### Task 4.3: Move `base_ibis_connection.py` to `backends/ibis/connection.py`

The legacy `BaseIbisConnection` class is the shared connection logic. Move it verbatim, then refactor in Task 4.4 to consume DialectSpec.

- [ ] **Step 1: Copy and update imports**

```bash
cp src/mountainash_data/databases/connections/ibis/base_ibis_connection.py \
   src/mountainash_data/backends/ibis/connection.py
```

Read the new `src/mountainash_data/backends/ibis/connection.py`. Rewrite imports:
- `mountainash_data.databases.connections.base_db_connection` → `mountainash_data.core.connection`
- `mountainash_data.databases.constants` → `mountainash_data.core.constants`
- `mountainash_data.databases.settings` → `mountainash_data.core.settings`

Leave the class name (`BaseIbisConnection` or whatever it is) unchanged for now.

- [ ] **Step 2: Replace original with shim**

Overwrite `src/mountainash_data/databases/connections/ibis/base_ibis_connection.py`:

```python
"""DEPRECATED: import from mountainash_data.backends.ibis.connection instead."""

from mountainash_data.backends.ibis.connection import *  # noqa: F401,F403
from mountainash_data.backends.ibis.connection import BaseIbisConnection  # noqa: F401
```

Add explicit re-exports for any other public symbols (read the file).

- [ ] **Step 3: Run tests**

Run: `hatch run test:test`
Expected: PASS, baseline count.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(data/ibis): move BaseIbisConnection to backends/ibis/connection.py

Moved verbatim, original location retained as shim. Refactored to
consume DialectSpec in the next task."
```

### Task 4.4: Salvage per-backend connection logic into DialectSpec entries

This task reads each of the 13 per-backend connection files and extracts the connection-building logic into the registry. After this task, the per-backend files become shims pointing at `IbisBackend(dialect=...)`.

For brevity, this task uses **postgres** as the worked example. Repeat the same pattern for the other 12 backends (sqlite, duckdb, motherduck, mysql, mssql, oracle, snowflake, bigquery, redshift, trino, pyspark, and any redshift quirks).

- [ ] **Step 1: Read postgres_ibis_connection.py and identify the connection-building logic**

Read `src/mountainash_data/databases/connections/ibis/postgres_ibis_connection.py`. The audit identified this as 94 LOC of concrete connection logic. Find the method (likely `_connect` or `connect`) that takes settings and returns an ibis backend connection. Note any per-session option setters (the audit mentioned this for postgres).

- [ ] **Step 2: Add a connection_builder function for postgres in the registry**

Edit `src/mountainash_data/backends/ibis/dialects/_registry.py`. Above the `DIALECTS` dict, add:

```python
def _build_postgres_connection(**config: t.Any) -> t.Any:
    """Build a postgres ibis connection. Salvaged from
    databases/connections/ibis/postgres_ibis_connection.py."""
    import ibis
    # Extract the actual connection-building logic from the legacy file.
    # Typical shape:
    #   conn = ibis.postgres.connect(
    #       host=config["host"],
    #       port=config.get("port", 5432),
    #       user=config["user"],
    #       password=config["password"],
    #       database=config["database"],
    #   )
    #   # Apply any per-session options the legacy file applied.
    #   return conn
    raise NotImplementedError("Replace with actual logic from legacy file")
```

Replace the `raise NotImplementedError` with the actual code copied (and adapted for the new function signature) from `postgres_ibis_connection.py`. If the legacy file uses a settings object, accept it as a kwarg or as `**config` and call its accessors.

Update the postgres entry in `DIALECTS`:

```python
"postgres": DialectSpec(
    ibis_backend_name="postgres",
    connection_mode="DIRECT",
    connection_string_scheme="postgresql",
    connection_builder=_build_postgres_connection,
),
```

- [ ] **Step 3: Repeat Step 1–2 for the other 12 dialects**

For each of: `sqlite`, `duckdb`, `motherduck`, `mysql`, `mssql`, `oracle`, `snowflake`, `bigquery`, `redshift`, `trino`, `pyspark`:

1. Read `databases/connections/ibis/<backend>_ibis_connection.py`
2. Add a `_build_<backend>_connection(**config)` function above `DIALECTS`
3. Wire it into the matching `DialectSpec` entry

Pay special attention to:
- **motherduck** (202 LOC) — has retries, the audit warned it's verbose. Preserve the retry logic.
- **redshift** — uses `ibis.postgres.connect` under the hood per the audit. The connection_builder may share code with postgres.
- **bigquery** (99 LOC) — auth method matters (service account vs ADC).

- [ ] **Step 4: Run the existing connection lifecycle test**

Run: `hatch run test:test-quick tests/test_unit/databases/connections/ibis/test_connection_lifecycle.py -v`
Expected: PASS — the tests still drive the legacy classes via shims, which still work because we haven't deleted the per-backend connection files yet. This confirms the registry additions didn't break anything.

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/backends/ibis/dialects/_registry.py
git commit -m "feat(data/ibis): salvage per-backend connection-builder logic into DialectSpec

Each of the 12 dialects now has a connection_builder function in the
registry, copied (and adapted for the new function signature) from the
corresponding databases/connections/ibis/<backend>_ibis_connection.py.

The per-backend files still exist; they become shims in Task 4.6."
```

### Task 4.5: Move and merge ibis operations into `backends/ibis/operations.py`

- [ ] **Step 1: Copy the core operations file and the two mixins**

```bash
cp src/mountainash_data/databases/operations/ibis/base_ibis_operations.py \
   src/mountainash_data/backends/ibis/operations.py
```

- [ ] **Step 2: Update imports in `backends/ibis/operations.py`**

Same import-rewriting as Task 4.3 step 1: `databases.constants` → `core.constants`, `databases.settings` → `core.settings`, `databases.connections.base_db_connection` → `core.connection`.

- [ ] **Step 3: Read `_base_ibis_mixin.py` and fold its helpers into `operations.py`**

Read `src/mountainash_data/databases/operations/ibis/_base_ibis_mixin.py` (98 LOC). It contains helper methods like `_generate_index_name`, `_format_qualified_table`, `_normalize_columns`.

Convert each helper from a mixin method (`def _generate_index_name(self, ...)`) into a module-level function in `operations.py` (`def _generate_index_name(...)`). Where the helper used `self`, accept the needed values as parameters instead.

If the canonical `BaseIbisOperations` class in `operations.py` was inheriting from `_BaseIbisMixin`, remove that base class — the helpers are now functions called directly.

- [ ] **Step 4: Read `_duckdb_family_mixin.py` and add its functions to `operations.py` as capability hooks**

Read `src/mountainash_data/databases/operations/ibis/_duckdb_family_mixin.py` (314 LOC). It contains DuckDB-family-specific index queries and helpers, used by duckdb, motherduck, and sqlite.

Convert each method into a module-level function in `operations.py`:

```python
def duckdb_get_index_exists_sql(table_name: str, index_name: str) -> str:
    """Salvaged from _duckdb_family_mixin.py.
    DuckDB stores indexes in the duckdb_indexes() function.
    """
    return f"SELECT * FROM duckdb_indexes() WHERE table_name = '{table_name}' AND index_name = '{index_name}'"
```

Reproduce each of the family mixin's methods this way. Then read the three concrete files (`duckdb_ibis_operations.py`, `sqlite_ibis_operations.py`, `motherduck_ibis_operations.py`, `trino_ibis_operations.py`) and confirm what each adds on top of the mixin:

- **duckdb** (66 LOC): the audit said it has dialect-specific index catalog SQL. If the SQL differs from `_duckdb_family_mixin`'s default, add a `duckdb_get_index_exists_sql` function with the duckdb-specific version.
- **sqlite** (64 LOC): adds sqlite-specific index queries against `sqlite_master`. Add `sqlite_get_index_exists_sql` and `sqlite_get_list_indexes_sql`.
- **motherduck** (80 LOC): adds `_list_tables` override and motherduck-specific queries. Add `motherduck_*` functions.
- **trino** (34 LOC): only sets connection mode (already handled in DialectSpec via Task 4.1). The file has no operations logic to salvage.

- [ ] **Step 5: Wire the capability hooks into the dialect registry**

Edit `src/mountainash_data/backends/ibis/dialects/_registry.py`. Import the capability functions:

```python
from mountainash_data.backends.ibis.operations import (
    duckdb_get_index_exists_sql,
    duckdb_get_list_indexes_sql,
    sqlite_get_index_exists_sql,
    sqlite_get_list_indexes_sql,
    motherduck_get_index_exists_sql,
    motherduck_get_list_indexes_sql,
)
```

Update the dialect entries to attach the hooks:

```python
"duckdb": DialectSpec(
    ibis_backend_name="duckdb",
    connection_mode="DIRECT",
    connection_string_scheme="duckdb",
    connection_builder=_build_duckdb_connection,
    get_index_exists_sql=duckdb_get_index_exists_sql,
    get_list_indexes_sql=duckdb_get_list_indexes_sql,
),
"sqlite": DialectSpec(
    ibis_backend_name="sqlite",
    connection_mode="DIRECT",
    connection_string_scheme="sqlite",
    connection_builder=_build_sqlite_connection,
    get_index_exists_sql=sqlite_get_index_exists_sql,
    get_list_indexes_sql=sqlite_get_list_indexes_sql,
),
"motherduck": DialectSpec(
    ibis_backend_name="duckdb",
    connection_mode="DIRECT",
    connection_string_scheme="md",
    connection_builder=_build_motherduck_connection,
    get_index_exists_sql=motherduck_get_index_exists_sql,
    get_list_indexes_sql=motherduck_get_list_indexes_sql,
),
```

- [ ] **Step 6: Replace `_base_ibis_mixin.py` and `_duckdb_family_mixin.py` with shims**

Overwrite `src/mountainash_data/databases/operations/ibis/_base_ibis_mixin.py`:

```python
"""DEPRECATED: helpers are now module-level functions in
mountainash_data.backends.ibis.operations."""

from mountainash_data.backends.ibis.operations import (  # noqa: F401
    # explicitly re-export every salvaged helper, e.g.:
    # _generate_index_name,
    # _format_qualified_table,
    # _normalize_columns,
)
```

Read `backends/ibis/operations.py` and add each helper name to the import list above.

If any consumer was using `_BaseIbisMixin` as a class (e.g., as a base class for a custom subclass), the shim won't be sufficient. Add a fallback class definition:

```python
class _BaseIbisMixin:
    """DEPRECATED compatibility shim — methods now live as module-level
    functions in mountainash_data.backends.ibis.operations."""
    pass
```

Repeat the same shim treatment for `_duckdb_family_mixin.py`.

- [ ] **Step 7: Replace `base_ibis_operations.py` with a shim**

Overwrite `src/mountainash_data/databases/operations/ibis/base_ibis_operations.py`:

```python
"""DEPRECATED: import from mountainash_data.backends.ibis.operations instead."""

from mountainash_data.backends.ibis.operations import *  # noqa: F401,F403
from mountainash_data.backends.ibis.operations import BaseIbisOperations  # noqa: F401
```

Add explicit re-exports for any other public symbols (read the file).

- [ ] **Step 8: Replace the 4 concrete ops files with shims**

For `duckdb_ibis_operations.py`, `sqlite_ibis_operations.py`, `motherduck_ibis_operations.py`, `trino_ibis_operations.py`:

Each becomes a shim. The class names they exported (e.g., `DuckDBIbisOperations`) need to remain importable for the factories. Since the per-backend ops classes added little beyond inheritance + a property override, the shim can re-export `BaseIbisOperations` under the old name:

```python
"""DEPRECATED: per-backend ops classes are gone; the registry's
capability hooks attach backend-specific behavior to BaseIbisOperations."""

from mountainash_data.backends.ibis.operations import BaseIbisOperations as DuckDBIbisOperations  # noqa: F401
```

Repeat for sqlite/motherduck/trino with their respective class names. Read each original file to confirm the exact class name being aliased.

- [ ] **Step 9: Update `databases/operations/ibis/__init__.py`**

Read the file. Make sure all the class names it exports still resolve. Most should, via the shims above.

- [ ] **Step 10: Run the existing operations tests**

Run: `hatch run test:test-quick tests/test_unit/databases/operations/ -v`
Expected: PASS, baseline count for this directory.

If `test_upsert_and_indexes.py` fails because it expected `_DuckDBFamilyOperationsMixin` to be a class with methods, the shim from Step 6 needs the fallback class definition (use the pattern shown there). Add stub methods that delegate to the new module-level functions if the test is calling methods on the class.

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "refactor(data/ibis): merge operations into backends/ibis/operations.py

base_ibis_operations.py (676 LOC of actual implementation),
_base_ibis_mixin.py (98 LOC), and _duckdb_family_mixin.py (314 LOC)
are now a single backends/ibis/operations.py module with helpers as
functions. Backend-specific operations (duckdb/sqlite/motherduck index
SQL) are attached to dialect entries via capability hooks. Per-backend
operations files retained as shims through Phase 6."
```

### Task 4.6: Replace the 13 per-backend connection files with shims

- [ ] **Step 1: For each of the 13 files, write a shim**

For each of `sqlite`, `duckdb`, `motherduck`, `postgres`, `mysql`, `mssql`, `oracle`, `snowflake`, `bigquery`, `redshift`, `trino`, `pyspark` (and the base file `base_ibis_connection.py` already handled in Task 4.3):

Read the original `databases/connections/ibis/<backend>_ibis_connection.py` and identify the class name(s) it exported (e.g., `Postgres_IbisConnection`).

Overwrite the file with:

```python
"""DEPRECATED: use IbisBackend(dialect="<backend>") from
mountainash_data.backends.ibis.backend instead."""

from mountainash_data.backends.ibis.connection import BaseIbisConnection as Postgres_IbisConnection  # noqa: F401
```

The alias means existing imports of the old class name still work — they just get the base class, which is fine because all the per-backend logic is now in the registry rather than in subclasses.

If a per-backend class had ANY method beyond `__init__` / property overrides, that logic was supposed to be salvaged in Task 4.4. Re-check by reading the file. If you find a method that wasn't salvaged, STOP and surface it.

- [ ] **Step 2: Run tests**

Run: `hatch run test:test`
Expected: PASS, baseline count.

If `test_connection_lifecycle.py` fails, the most likely cause is that a salvaged `_build_<backend>_connection` function in the registry doesn't match the legacy `_connect` method's behavior. Re-read the legacy file and the registry function side by side, find the discrepancy, fix.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor(data/ibis): replace 13 per-backend connection files with shims

Connection-building logic now lives in dialects/_registry.py as
DialectSpec.connection_builder callables. The 13 per-backend files
re-export BaseIbisConnection under the old class names so existing
imports keep working through Phase 6."
```

### Task 4.7: Create `backends/ibis/inspect.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_unit/backends/ibis/test_inspect.py`:

```python
"""Tests for ibis → core.inspection conversion."""

import ibis

from mountainash_data.backends.ibis.inspect import table_to_info
from mountainash_data.core.inspection import TableInfo


def test_table_to_info_from_ibis_table():
    # Build an in-memory sqlite ibis backend with one table
    conn = ibis.sqlite.connect()
    conn.create_table(
        "users",
        schema=ibis.schema({"id": "int64", "name": "string"}),
    )
    table = conn.table("users")
    info = table_to_info(table, name="users", namespace="main")
    assert isinstance(info, TableInfo)
    assert info.name == "users"
    assert info.namespace == "main"
    assert info.column_names == ["id", "name"]
    assert info.columns[0].nullable is True
    assert info.columns[0].type_name == "int64"
```

- [ ] **Step 2: Run the test**

Run: `hatch run test:test-quick tests/test_unit/backends/ibis/test_inspect.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `inspect.py`**

Create `src/mountainash_data/backends/ibis/inspect.py`:

```python
"""Ibis → core.inspection conversion helpers."""

from __future__ import annotations

import typing as t

from mountainash_data.core.inspection import (
    CatalogInfo,
    ColumnInfo,
    NamespaceInfo,
    TableInfo,
)


def table_to_info(
    ibis_table,
    *,
    name: str,
    namespace: str | None = None,
    catalog: str | None = None,
) -> TableInfo:
    """Convert an ibis Table object into a TableInfo."""
    schema = ibis_table.schema()
    columns = [
        ColumnInfo(
            name=col_name,
            type_name=str(col_type),
            nullable=col_type.nullable,
        )
        for col_name, col_type in zip(schema.names, schema.types)
    ]
    return TableInfo(
        name=name,
        columns=columns,
        namespace=namespace,
        catalog=catalog,
    )
```

- [ ] **Step 4: Run the test**

Run: `hatch run test:test-quick tests/test_unit/backends/ibis/test_inspect.py -v`
Expected: PASS.

If the assertion on `nullable` fails with `True != False`, ibis may default to non-nullable. Adjust the test to match ibis's actual default rather than changing production behavior; pick whichever is correct by reading ibis's documentation for `Schema`.

- [ ] **Step 5: Commit**

```bash
git add src/mountainash_data/backends/ibis/inspect.py tests/test_unit/backends/ibis/test_inspect.py
git commit -m "feat(data/ibis): add ibis → core.inspection conversion"
```

### Task 4.8: Create `backends/ibis/backend.py` — IbisBackend Protocol implementation

- [ ] **Step 1: Write the failing test**

Create `tests/test_unit/backends/ibis/test_backend.py`:

```python
"""Tests for IbisBackend factory."""

import pytest

from mountainash_data.backends.ibis.backend import IbisBackend
from mountainash_data.backends.ibis.dialects._registry import DIALECTS
from mountainash_data.core.protocol import Backend


def test_ibis_backend_satisfies_protocol():
    backend = IbisBackend(dialect="sqlite")
    assert isinstance(backend, Backend)
    assert backend.name == "ibis"


def test_unknown_dialect_raises():
    with pytest.raises(KeyError, match="Unknown ibis dialect"):
        IbisBackend(dialect="bogus")


def test_all_registered_dialects_construct():
    for dialect_name in DIALECTS:
        backend = IbisBackend(dialect=dialect_name)
        assert backend.dialect == dialect_name


def test_in_memory_sqlite_connect_and_inspect():
    """End-to-end test with the only dialect that needs no external service."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    conn = backend.connect()
    try:
        # Should expose protocol methods even if no tables exist
        assert conn.list_tables() == []
    finally:
        conn.close()
```

- [ ] **Step 2: Run the test**

Run: `hatch run test:test-quick tests/test_unit/backends/ibis/test_backend.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `backend.py`**

Create `src/mountainash_data/backends/ibis/backend.py`:

```python
"""IbisBackend — implements core.protocol.Backend for ibis-supported backends."""

from __future__ import annotations

import typing as t

from mountainash_data.backends.ibis.connection import BaseIbisConnection
from mountainash_data.backends.ibis.dialects._registry import DIALECTS, DialectSpec
from mountainash_data.core.protocol import Backend, Connection


class IbisBackend:
    """Ibis backend factory.

    Construction takes a dialect name (e.g. 'postgres') and config.
    connect() returns a live connection that satisfies
    core.protocol.Connection.
    """

    name = "ibis"

    def __init__(self, dialect: str, **config: t.Any):
        if dialect not in DIALECTS:
            raise KeyError(
                f"Unknown ibis dialect {dialect!r}. "
                f"Available: {sorted(DIALECTS)}"
            )
        self.dialect = dialect
        self._spec: DialectSpec = DIALECTS[dialect]
        self._config = config

    def connect(self) -> Connection:
        if self._spec.connection_builder is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} has no connection_builder configured"
            )
        ibis_conn = self._spec.connection_builder(**self._config)
        return BaseIbisConnection(ibis_conn, dialect_spec=self._spec)
```

Note: this introduces new constructor args on `BaseIbisConnection`. Read the existing `BaseIbisConnection` `__init__` in `backends/ibis/connection.py` and reconcile. Likely you'll need to add a constructor that takes a pre-built ibis connection plus a `DialectSpec`, while keeping the legacy constructor working through the shim path.

The simplest reconciliation: add a class method or alternate constructor:

```python
@classmethod
def from_dialect(cls, ibis_conn, dialect_spec: DialectSpec) -> "BaseIbisConnection":
    self = cls.__new__(cls)
    self._ibis_conn = ibis_conn
    self._dialect_spec = dialect_spec
    # ... initialize whatever else BaseIbisConnection needs
    return self
```

And have `IbisBackend.connect()` use it:

```python
return BaseIbisConnection.from_dialect(ibis_conn, self._spec)
```

Read the existing BaseIbisConnection to make this concrete; the goal is "construct a connection from a pre-built ibis backend and a DialectSpec without going through the legacy settings-class path."

Also: `BaseIbisConnection` must satisfy the `Connection` Protocol — `list_namespaces`, `list_tables`, `inspect_table`, `inspect_namespace`, `inspect_catalog`, `close`. Add any missing methods (delegating to the wrapped ibis connection where possible, and to `inspect.table_to_info` for the inspect_* methods).

- [ ] **Step 4: Run the test**

Run: `hatch run test:test-quick tests/test_unit/backends/ibis/test_backend.py -v`
Expected: PASS.

The end-to-end sqlite test (`test_in_memory_sqlite_connect_and_inspect`) is the most likely to fail. Common failure modes:
- `_build_sqlite_connection` doesn't accept `database=":memory:"` — adjust the function to pass through kwargs to `ibis.sqlite.connect`.
- `BaseIbisConnection.list_tables()` doesn't exist or has a different signature — add it as a wrapper around `self._ibis_conn.list_tables()`.
- `BaseIbisConnection.close()` doesn't exist — add it.

Each failure points at one missing piece. Add only what's needed to pass.

- [ ] **Step 5: Run the full test suite**

Run: `hatch run test:test`
Expected: PASS, baseline count.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(data/ibis): add IbisBackend Protocol implementation

IbisBackend(dialect=...) constructs a connection by looking up the
DialectSpec in the registry. BaseIbisConnection extended with a
from_dialect classmethod and the methods required by core.protocol.Connection."
```

### Task 4.9: Implement `to_relation()` on the ibis connection

Per the spec, `Backend.connect().to_relation(table_name)` is the seam to `mountainash-expressions`. This task adds it for ibis.

- [ ] **Step 1: Confirm the mountainash-expressions Relation API**

Run: `grep -rn "class Relation\|from_ibis\|^def from_ibis" /home/nathanielramm/git/mountainash-io/mountainash/mountainash-expressions/src/mountainash/ --include='*.py' | head -20`

Identify how `mountainash-expressions` constructs a Relation from an ibis Table. Likely there's something like `Relation.from_ibis(table)` or an `ibis_to_relation(table)` function.

If the API doesn't exist, STOP — `to_relation()` for ibis cannot be implemented without it. Document the gap in the same way iceberg's gap is documented (in `tests/test_unit/backends/ibis/COVERAGE_GAP.md`) and skip steps 2–5.

- [ ] **Step 2: Write the failing test**

Append to `tests/test_unit/backends/ibis/test_backend.py`:

```python
def test_to_relation_returns_expressions_relation():
    from mountainash.expressions import Relation  # adjust import to actual API

    backend = IbisBackend(dialect="sqlite", database=":memory:")
    conn = backend.connect()
    try:
        conn._ibis_conn.create_table(
            "users",
            schema=__import__("ibis").schema({"id": "int64"}),
        )
        rel = conn.to_relation("users")
        assert isinstance(rel, Relation)
    finally:
        conn.close()
```

Adjust the import statement to match the actual `mountainash-expressions` public API confirmed in Step 1.

- [ ] **Step 3: Run the test**

Run: `hatch run test:test-quick tests/test_unit/backends/ibis/test_backend.py::test_to_relation_returns_expressions_relation -v`
Expected: FAIL.

- [ ] **Step 4: Implement `to_relation` on `BaseIbisConnection`**

Edit `src/mountainash_data/backends/ibis/connection.py`. Add:

```python
def to_relation(self, table_name: str, namespace: str | None = None):
    """Hand off to mountainash-expressions: return a Relation backed by
    this connection's ibis table.
    """
    from mountainash.expressions import Relation  # adjust to actual API
    ibis_table = self._ibis_conn.table(table_name)
    return Relation.from_ibis(ibis_table)  # adjust to actual API
```

- [ ] **Step 5: Run the test**

Run: `hatch run test:test-quick tests/test_unit/backends/ibis/test_backend.py::test_to_relation_returns_expressions_relation -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(data/ibis): add to_relation() seam to mountainash-expressions"
```

### Task 4.10: Phase 4 sanity check

- [ ] **Step 1: Run full test suite**

Run: `hatch run test:test`
Expected: PASS, baseline count.

- [ ] **Step 2: Run lint and type-check**

Run: `hatch run ruff:check src/mountainash_data/`
Expected: PASS.

Run: `hatch run mypy:check`
Expected: PASS.

Fix any issues introduced by Phase 4 before continuing.

---

## Phase 5 — Wire factories to the registry (D2.a)

**Files touched:**
- Create: `src/mountainash_data/core/factories/__init__.py` and 5 file copies
- Create: `src/mountainash_data/core/utils.py` (from `database_utils.py`)
- Modify: `src/mountainash_data/factories/*.py` (each becomes a shim)
- Modify: `src/mountainash_data/database_utils.py` (becomes a shim)

### Task 5.1: Move factories verbatim with shims

- [ ] **Step 1: Copy factories to `core/factories/`**

```bash
mkdir -p src/mountainash_data/core/factories
cp src/mountainash_data/factories/*.py src/mountainash_data/core/factories/
```

- [ ] **Step 2: Update internal imports inside `core/factories/`**

Run: `grep -rn "from mountainash_data" src/mountainash_data/core/factories/`

Rewrite:
- `mountainash_data.databases.connections.base_db_connection` → `mountainash_data.core.connection`
- `mountainash_data.databases.constants` → `mountainash_data.core.constants`
- `mountainash_data.databases.settings` → `mountainash_data.core.settings`
- `mountainash_data.factories.<x>` → `mountainash_data.core.factories.<x>` (cross-factory imports)

Leave imports of per-backend connection classes (e.g., `databases.connections.ibis.postgres_ibis_connection`) alone for now — they still resolve via the shims and Task 5.2 updates them.

- [ ] **Step 3: Replace each `factories/*.py` file with a shim**

For each of `__init__.py`, `base_strategy_factory.py`, `settings_type_factory_mixin.py`, `connection_factory.py`, `operations_factory.py`, `settings_factory.py`:

```python
"""DEPRECATED: import from mountainash_data.core.factories.<modulename> instead."""

from mountainash_data.core.factories.<modulename> import *  # noqa: F401,F403
```

Add explicit re-exports for the public symbols (read each `core/factories/<file>.py` to enumerate).

- [ ] **Step 4: Run tests**

Run: `hatch run test:test`
Expected: PASS, baseline count.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(data): move factories to core/factories/ with shims"
```

### Task 5.2: Update factory strategy mappings to point at IbisBackend / IcebergBackend

- [ ] **Step 1: Read `core/factories/connection_factory.py` and identify the strategy mapping**

The audit said this file lazily loads per-backend connection classes (13 ibis + pyiceberg). Find the mapping (likely a dict mapping backend names to dotted import paths).

- [ ] **Step 2: Update the mapping**

Replace each per-backend class import path with a factory function that returns an `IbisBackend(dialect=...)` instance. For example, if the old mapping was:

```python
_STRATEGIES = {
    "postgres": "mountainash_data.databases.connections.ibis.postgres_ibis_connection.Postgres_IbisConnection",
    "sqlite": "mountainash_data.databases.connections.ibis.sqlite_ibis_connection.SQLite_IbisConnection",
    ...
}
```

Replace with a function-based mapping or add a wrapper:

```python
def _make_ibis_factory(dialect: str):
    def factory(**config):
        from mountainash_data.backends.ibis.backend import IbisBackend
        return IbisBackend(dialect=dialect, **config)
    return factory


def _make_iceberg_factory(catalog: str):
    def factory(**config):
        from mountainash_data.backends.iceberg.backend import IcebergBackend
        return IcebergBackend(catalog=catalog, **config)
    return factory


_STRATEGIES = {
    "postgres": _make_ibis_factory("postgres"),
    "sqlite": _make_ibis_factory("sqlite"),
    "duckdb": _make_ibis_factory("duckdb"),
    "motherduck": _make_ibis_factory("motherduck"),
    "mysql": _make_ibis_factory("mysql"),
    "mssql": _make_ibis_factory("mssql"),
    "oracle": _make_ibis_factory("oracle"),
    "snowflake": _make_ibis_factory("snowflake"),
    "bigquery": _make_ibis_factory("bigquery"),
    "redshift": _make_ibis_factory("redshift"),
    "trino": _make_ibis_factory("trino"),
    "pyspark": _make_ibis_factory("pyspark"),
    "pyiceberg_rest": _make_iceberg_factory("rest"),
}
```

If `BaseStrategyFactory` uses lazy loading via importlib (per the audit), the function-based mapping above is incompatible — adjust to whatever shape the lazy loader expects. Read `base_strategy_factory.py` to confirm. The simplest backward-compatible approach: keep the dotted-string lazy-loading path, and create a thin wrapper module that exposes the new backends under the old class names. But function-based is cleaner if the framework allows it.

- [ ] **Step 3: Apply the same treatment to `operations_factory.py`**

Read it. The strategy mapping previously pointed at per-backend ops classes. Now there's only `BaseIbisOperations` and the iceberg operations module. Update the mapping to return `BaseIbisOperations` for all ibis backends (since per-backend operations classes no longer exist) and the iceberg operations module/class for pyiceberg.

- [ ] **Step 4: `settings_factory.py` — leave the URL detection logic alone**

Read `core/factories/settings_factory.py`. Per the audit, the SCHEME_MAP and URL detection are the "real feature" worth keeping. Do not change them. Confirm internal imports are updated per Task 5.1 step 2.

- [ ] **Step 5: Run the factory tests**

Run: `hatch run test:test-quick tests/test_unit/factories/ -v`
Expected: PASS.

If `test_connection_factory.py` fails with assertions like "expected class X, got IbisBackend", update the test expectations — the new behavior (factory returns `IbisBackend(dialect="postgres")`) is correct per the spec. Document the test expectation change in the commit message.

If `test_operations_factory.py` fails similarly, same treatment.

If a test fails because it tried to instantiate the returned class with legacy positional args, the test was depending on the old shape. Update it to use the new construction style: `factory.get_connection("postgres", **config)` returning an `IbisBackend`.

- [ ] **Step 6: Run the full test suite**

Run: `hatch run test:test`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor(data/factories): point connection/operations factories at new backends

ConnectionFactory now returns IbisBackend(dialect=...) or
IcebergBackend(catalog=...) instead of per-backend subclasses.
OperationsFactory returns BaseIbisOperations for all ibis dialects
(per-backend ops classes are gone). settings_factory unchanged."
```

### Task 5.3: Move `database_utils.py` → `core/utils.py`

- [ ] **Step 1: Copy and update imports**

```bash
cp src/mountainash_data/database_utils.py src/mountainash_data/core/utils.py
```

Read `src/mountainash_data/core/utils.py`. Rewrite imports:
- `mountainash_data.factories.*` → `mountainash_data.core.factories.*`
- `mountainash_data.databases.constants` → `mountainash_data.core.constants`
- `mountainash_data.databases.settings` → `mountainash_data.core.settings`

- [ ] **Step 2: Update the facade to consume new factories**

The audit identified six methods on `DatabaseUtils`: `create_connection`, `create_operations`, `create_backend`, `create_settings_from_url`, `detect_backend_from_url`, `create_from_url`.

Read each method. They likely already call into `ConnectionFactory` / `OperationsFactory` / `SettingsFactory`. The factory APIs haven't changed (still `get_connection(backend_type, **config)`), so the methods should keep working without changes. Only the import paths needed updating in Step 1.

If any method directly imports a per-backend connection class (e.g., `from mountainash_data.databases.connections.ibis.postgres_ibis_connection import Postgres_IbisConnection`), replace that with `from mountainash_data.backends.ibis.backend import IbisBackend` and use the dialect arg.

- [ ] **Step 3: Replace the original `database_utils.py` with a shim**

Overwrite `src/mountainash_data/database_utils.py`:

```python
"""DEPRECATED: import from mountainash_data.core.utils instead."""

from mountainash_data.core.utils import *  # noqa: F401,F403
from mountainash_data.core.utils import DatabaseUtils  # noqa: F401
```

Add explicit re-exports for any other public symbols.

- [ ] **Step 4: Run tests**

Run: `hatch run test:test`
Expected: PASS, baseline count.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(data): move database_utils to core/utils.py with shim"
```

---

## Phase 6 — Remove shims and finalize

### Task 6.1: Confirm no production code references shimmed paths

- [ ] **Step 1: Grep for legacy paths**

Run: `grep -rn "mountainash_data\.databases\." src/ tests/ notebooks/`

Expected: matches inside `src/mountainash_data/databases/` itself (the shims) and inside the test files that still test the legacy paths.

For every match in `notebooks/` or in non-shim `src/` files, update to the new path before proceeding. Tests that test the shim layer specifically (if any) can be deleted — the shims are about to be removed.

- [ ] **Step 2: Update top-level `__init__.py`**

Read `src/mountainash_data/__init__.py`. Replace its current re-exports with the new public surface:

```python
"""mountainash-data: physical access to backend data services.

Public API:
    Backend, Connection — protocols (core.protocol)
    IbisBackend — ibis-style relational backends (backends.ibis.backend)
    IcebergBackend — iceberg-style table-format catalogs (backends.iceberg.backend)
    CatalogInfo, NamespaceInfo, TableInfo, ColumnInfo — inspection model
    DatabaseUtils — high-level facade
    ConnectionFactory, OperationsFactory, SettingsFactory — factories
    *Settings classes — see mountainash_data.core.settings
"""

from mountainash_data.__version__ import __version__
from mountainash_data.core.protocol import Backend, Connection
from mountainash_data.core.inspection import (
    CatalogInfo,
    ColumnInfo,
    NamespaceInfo,
    TableInfo,
)
from mountainash_data.core.utils import DatabaseUtils
from mountainash_data.core.factories import (
    ConnectionFactory,
    OperationsFactory,
    SettingsFactory,
)
from mountainash_data.backends.ibis.backend import IbisBackend
from mountainash_data.backends.iceberg.backend import IcebergBackend

__all__ = [
    "__version__",
    "Backend",
    "Connection",
    "CatalogInfo",
    "ColumnInfo",
    "NamespaceInfo",
    "TableInfo",
    "DatabaseUtils",
    "ConnectionFactory",
    "OperationsFactory",
    "SettingsFactory",
    "IbisBackend",
    "IcebergBackend",
]
```

- [ ] **Step 3: Run tests**

Run: `hatch run test:test`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(data): update top-level __init__.py to new public surface"
```

### Task 6.2: Delete all shims and the old `databases/` and `factories/` directories

- [ ] **Step 1: Delete the shimmed directories**

```bash
rm -rf src/mountainash_data/databases
rm -rf src/mountainash_data/factories
rm src/mountainash_data/database_utils.py
```

- [ ] **Step 2: Run tests**

Run: `hatch run test:test`
Expected: PASS — if any test still imports from the deleted paths, it should be updated or removed.

If a test fails with `ModuleNotFoundError: mountainash_data.databases...`, that test is testing the shim layer. Two options:
1. Update the test to import from the new location and assert the same behavior.
2. Delete the test if its only purpose was to verify the shim.

Pick option 1 unless the test is purely about the legacy structure and has no equivalent value in the new structure.

- [ ] **Step 3: Run lint and type-check**

Run: `hatch run ruff:check`
Expected: PASS.

Run: `hatch run mypy:check`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(data): remove all Phase 1–5 shims and legacy directories

databases/, factories/, and database_utils.py are gone. The new public
surface lives in core/ and backends/. Refactor complete."
```

### Task 6.3: Update notebooks, README, and CLAUDE.md

- [ ] **Step 1: Update notebook imports**

Run: `grep -rn "mountainash_data" notebooks/ --include='*.ipynb' --include='*.py'`

For each notebook that imports from the package, update the import paths to the new public surface (`from mountainash_data import IbisBackend` etc.).

If notebooks aren't actively used, this can be deferred — note any deferral in the commit message.

- [ ] **Step 2: Update README.md**

Read `README.md`. Update the architecture overview and usage examples to reflect the new shape. Replace any code samples that show `from mountainash_data.databases.connections.ibis.postgres_ibis_connection import Postgres_IbisConnection` with `from mountainash_data import IbisBackend`.

- [ ] **Step 3: Update CLAUDE.md**

Read `CLAUDE.md`. Update the Architecture, Package Structure, and Usage Patterns sections to match the new layout. Replace the old usage patterns block with:

```python
from mountainash_data import IbisBackend, IcebergBackend
from mountainash_data.core.settings.postgresql import PostgreSQLSettings

# Ibis backend
backend = IbisBackend(dialect="postgres", host="localhost", database="mydb", user="me")
conn = backend.connect()
try:
    tables = conn.list_tables()
    info = conn.inspect_table("users")
    relation = conn.to_relation("users")  # → mountainash-expressions Relation
finally:
    conn.close()

# Iceberg backend
ice = IcebergBackend(catalog="rest", uri="http://localhost:8181")
ice_conn = ice.connect()
try:
    namespaces = ice_conn.list_namespaces()
finally:
    ice_conn.close()
```

- [ ] **Step 4: Run tests one final time**

Run: `hatch run test:test`
Expected: PASS, final count recorded for comparison with baseline.

Run: `hatch run ruff:check && hatch run mypy:check`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs(data): update README and CLAUDE.md to reflect new architecture"
```

### Task 6.4: Create the PR

- [ ] **Step 1: Push the branch and open a PR**

```bash
git push -u origin refactor/data-package-audit
```

```bash
gh pr create --title "Refactor: collapse data package onto core+backends architecture" --body "$(cat <<'EOF'
## Summary

- Collapse 13 ibis per-backend connection classes into a data-driven `DialectSpec` registry under `backends/ibis/dialects/`
- Finish the half-done split between iceberg connection and operations files; deduplicate methods
- Add `core/protocol.py` (Backend, Connection protocols) and `core/inspection.py` (shared physical metadata model)
- Move settings, factories, and the high-level facade (`database_utils` → `core/utils.py`) into `core/`
- Add `to_relation()` seam between physical (`mountainash-data`) and logical (`mountainash-expressions`) layers
- Delete 10 dead/duplicate files (legacy db_connection_factory, 8 stub ibis ops files, lineage stub, `__init___old.py`)
- ~9.5k LOC reorganized; behavior preserved (existing test suite green throughout)

See `docs/superpowers/specs/2026-04-07-mountainash-data-audit-and-redesign.md` for the full design.

## Test plan

- [x] `hatch run test:test` green at every phase boundary
- [x] `hatch run ruff:check` clean
- [x] `hatch run mypy:check` clean
- [ ] Manual smoke: open a sqlite IbisBackend, list tables, inspect, to_relation
- [ ] Manual smoke: open a pyiceberg-rest IcebergBackend, list namespaces, inspect

## Known gaps

- `IcebergBackend.to_relation()` is unimplemented — requires `mountainash-expressions` to add an iceberg relation adapter; tracked separately
- No iceberg-specific tests added (the legacy code had none either; documented in `tests/test_unit/backends/iceberg/COVERAGE_GAP.md`)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-review

After writing the plan, the following spec sections were checked for coverage:

- ✅ Defects 1–4 (class explosion, mixins, no expressions seam, stateful connections) → addressed by Phases 4 (registry collapse), 4 (mixin → functions), 4.9 (`to_relation`), 1 (Backend Protocol stateless model)
- ✅ Target architecture sections (`core/`, `backends/`, `DialectSpec`, capability hooks, inspection model, expressions seam) → Phases 1, 3, 4
- ✅ Audit findings 1–8 → handled across Phases 0 (deletes), 3 (iceberg dedup), 4 (ibis salvage)
- ✅ D1.b (iceberg split + dedup) → Task 3.3
- ✅ D2.a (factories survive, registry is data) → Task 5.2
- ✅ D3.b (HYBRID grep at migration time) → Task 4.1 step 3
- ✅ Migration phases 0–6 → Phases 0–6
- ✅ Test strategy (per-phase tests, coverage gap audit) → Tasks 3.1, 4.1, plus per-task test runs
- ✅ Known gaps (`to_relation` for iceberg) → documented in Task 3.1 and PR body
