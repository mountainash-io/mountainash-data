# mountainash-data — Audit and Redesign

**Date:** 2026-04-07
**Status:** Design approved, awaiting written-spec review
**Scope:** Full audit and architectural redesign of the `mountainash-data` package

## Context

`mountainash-data` began as an abstraction over Ibis but has drifted. Today it contains:

- Ibis connection management for 13 backends
- A mixin-based ibis "operations" layer that overlaps awkwardly with the sister package `mountainash-expressions`
- A pyiceberg connection factory that has grown to ~1.7k LOC of mixed connection + operations logic
- A factories layer, settings subtree, and a 3-line lineage stub

The user is the only consumer (greenfield freedom; breaking changes are fine). The brainstorming session reframed the data/expressions relationship: they are **complementary, not duplicative** — `mountainash-data` is the *physical* layer (connect, inspect, manage backend services), `mountainash-expressions` is the *logical* layer (relations, expressions, query construction). The real defect is that `mountainash-data`'s architecture is antiquated and incompatible with the protocol-based, composable shape that `mountainash-expressions` uses.

This document captures the audit findings and the target architecture, then defines a phased migration sequence.

## Defects identified

The user confirmed four primary defects in the current architecture:

1. **Class explosion** — one connection class + one operations class per backend (26+ files), most of which are stubs or near-stubs
2. **Operations-as-mixins** — `_base_ibis_mixin` / `_duckdb_family_mixin` inheritance is rigid where composable functions would serve better
3. **No clean seam to expressions** — operations return raw ibis tables, not the relation/expression types `mountainash-expressions` speaks; consumers bridge manually
4. **Stateful connection objects** — `BaseDBConnection` carries state that should be a thin factory / context manager

Settings coupling to `mountainash-settings` is **not** a defect and stays.

## Target architecture

### Package identity (one sentence)

> *`mountainash-data` provides physical access to backend data services — connecting, inspecting, and managing them — through a single `Backend` protocol with peer implementations for ibis-style relational backends and iceberg-style table-format catalogs.*

### Top-level layout

```
src/mountainash_data/
├── __init__.py
├── __version__.py
├── core/                         # protocol layer (abstract, no impls)
│   ├── __init__.py
│   ├── protocol.py               # Backend, Connection, Catalog, Namespace, Table, Column protocols
│   ├── inspection.py             # CatalogInfo / NamespaceInfo / TableInfo / ColumnInfo dataclasses
│   ├── registry.py               # backend registration / lookup by name
│   ├── connection.py             # base connection types (was databases/connections/base_db_connection.py)
│   ├── constants.py              # was databases/constants.py
│   ├── utils.py                  # high-level facade (was database_utils.py)
│   ├── factories/                # lazy-loading strategy factories
│   │   ├── __init__.py
│   │   ├── base_strategy_factory.py
│   │   ├── settings_type_factory_mixin.py
│   │   ├── connection_factory.py
│   │   ├── operations_factory.py
│   │   └── settings_factory.py
│   └── settings/                 # Pydantic settings classes per backend
│       ├── __init__.py
│       ├── base.py
│       ├── exceptions.py
│       ├── templates.py
│       └── {bigquery,duckdb,motherduck,mssql,mysql,postgresql,
│            pyiceberg_rest,pyspark,redshift,snowflake,sqlite,trino}.py
└── backends/
    ├── ibis/                     # ibis implementation (peer)
    │   ├── __init__.py
    │   ├── backend.py            # IbisBackend implements core.Backend
    │   ├── connection.py         # was base_ibis_connection.py, refactored to consume DialectSpec
    │   ├── operations.py         # base_ibis_operations + _base_ibis_mixin + _duckdb_family functions
    │   ├── inspect.py            # ibis → core inspection model
    │   └── dialects/
    │       ├── __init__.py
    │       └── _registry.py      # DialectSpec entries for all 12 ibis backends
    └── iceberg/                  # iceberg implementation (peer)
        ├── __init__.py
        ├── backend.py            # IcebergBackend implements core.Backend
        ├── connection.py         # catalog/namespace lifecycle (deduplicated)
        ├── operations.py         # table mutations (deduplicated)
        ├── _types.py             # Iceberg → PyArrow conversion helpers
        ├── inspect.py            # pyiceberg → core inspection model
        └── catalogs/
            ├── __init__.py
            └── rest.py           # REST connection + operations merged
```

### Key architectural moves

1. **`Backend` is a Protocol, not a base class.** No inheritance. Implementations are plain classes that satisfy the protocol. Kills the mixin tree.

2. **`Backend` is stateless from the consumer's perspective.** Construction takes config; `connect()` returns a `Connection` context manager that owns the live handle. No long-lived stateful connection objects.

3. **The 13 ibis backend files collapse into a data-driven dialect registry.** Each backend (sqlite, duckdb, motherduck, postgres, mysql, mssql, oracle, snowflake, bigquery, redshift, trino, pyspark) becomes a `DialectSpec` entry containing: connection-builder callable, ibis backend name, connection mode, capability hooks for any backend-specific operations. One `IbisBackend` class drives all of them.

4. **Operations stop being mixins.** Concrete shared logic from `_base_ibis_mixin.py` (98 LOC), `_duckdb_family_mixin.py` (314 LOC), and `base_ibis_operations.py` (676 LOC, the actual implementation — see audit finding #2 below) folds into `backends/ibis/operations.py` as plain functions. Backend-specific concrete logic from the duckdb/sqlite/motherduck ops files is salvaged and attached to dialect entries via capability hooks.

5. **The seam to `mountainash-expressions`:** `Backend.connect()` exposes a `to_relation(table_name)` method that hands consumers a `mountainash-expressions` Relation. That is the entire bridge — physical layer hands off to logical layer at the relation boundary. No expression logic lives in `mountainash-data`.

6. **Shared inspection model (the "Medium" shared layer):** dataclasses for `CatalogInfo → NamespaceInfo → TableInfo → ColumnInfo`. Both `IbisBackend.inspect()` and `IcebergBackend.inspect()` populate the same shapes. This is the *one* concrete thing both paradigms share, and it is where consumers get the most value.

### Capability hooks pattern

Replaces mixin inheritance for backend-specific operations:

```python
DIALECTS = {
    "duckdb": DialectSpec(
        connect=_build_duckdb_conn,
        inspect=ibis_default_inspect,
        get_index_exists_sql=duckdb_get_index_exists_sql,    # salvaged
        get_list_indexes_sql=duckdb_get_list_indexes_sql,    # salvaged
        ...
    ),
    "sqlite": DialectSpec(
        connect=_build_sqlite_conn,
        get_index_exists_sql=sqlite_get_index_exists_sql,
        ...
    ),
    "postgres": DialectSpec(
        connect=_build_postgres_conn,
        inspect=ibis_default_inspect,
        # no extras
    ),
}
```

Backend-specific operations become **data on the dialect**, not subclass methods. Consumers reach them via `backend.capability("get_index_exists_sql")(args)` or — if usage warrants — first-class methods on a protocol extension.

### Sanity check: protocol holds for both paradigms

- **sqlite via ibis:** `IbisBackend(dialect="sqlite", config=...).connect()` → context manager yielding a connection with `list_tables()`, `inspect(table)` → `TableInfo`, `to_relation(table)` → ibis-backed Relation. ✅
- **pyiceberg-rest:** `IcebergBackend(catalog="rest", config=...).connect()` → context manager yielding a connection with `list_namespaces()`, `list_tables(ns)`, `inspect(table)` → `TableInfo`, `to_relation(table)` → pyiceberg-backed Relation **if** `mountainash-expressions` has an iceberg adapter; otherwise this method is unimplemented for now and the gap is documented. The protocol holds either way — `to_relation` is a capability that not every backend must implement.

## Audit methodology

Every `.py` file under `src/mountainash_data/` was classified into:

| Classification | Meaning |
|---|---|
| `keep-as-is` | fits the new layout unchanged |
| `adapt` | concept survives, needs rewriting against the new protocol |
| `salvage` | concrete logic worth extracting; surrounding class/file structure discarded |
| `delete` | dead code, stub, boilerplate, or fully superseded |
| `move-to-expressions` | logic that is logical/relational and belongs in `mountainash-expressions` |
| `ambiguous` | flagged for explicit user decision |

Out of scope for the audit: `tests/` (rewritten alongside their targets in migration), `notebooks/`, `docs/`, `pyproject.toml`, `hatch.toml`.

## Audit findings

**67 files audited, ~9.5k LOC total.** Highlights:

1. **`base_pyiceberg_connection.py` (884 lines) is not really a connection class.** It mixes catalog lifecycle with table operations (create, insert, upsert, truncate, view ops) and ~15 Iceberg→PyArrow type conversions. It is effectively the entire iceberg backend, with operations smuggled inside what is named a connection class. The previous attempt to split connection from operations was only half done and left duplicate methods between this file and `base_pyiceberg_operations.py` (868 lines).

2. **`base_ibis_operations.py` (676 lines) is not abstract.** It contains the full concrete implementations (run_sql, run_expr, table, create_table, drop_table, insert, upsert, truncate, view ops) with try/catch wrappers. The 11 per-backend "subclasses" are mostly stubs (12–19 LOC, just property overrides).

3. **8 of 11 per-backend ibis ops files are pure stubs** → straight delete: postgres, mysql, oracle, bigquery, snowflake, pyspark, redshift, mssql. Only `duckdb`, `sqlite`, `motherduck` have real logic (delegating to `_duckdb_family_mixin.py`); `trino` has a 34-line init whose necessity is unclear (see D3).

4. **`db_connection_factory.py` (213 lines) is a legacy duplicate** of the modern `factories/connection_factory.py`. Safe delete.

5. **`database_utils.py` (224 lines) is a useful high-level facade** (`create_connection`, `create_from_url`, `detect_backend_from_url`), not a junk drawer. Worth keeping as the public entry point. Becomes `core/utils.py`.

6. **The settings subtree (~2.7k LOC across 16 files) is exactly what it claims** — Pydantic settings classes per backend. Confirmed `keep-as-is`, relocated to `core/settings/`.

7. **The factories layer is well-built** (`BaseStrategyFactory` + lazy-loading mixin + 3 concrete factories, ~770 LOC). Not overengineered. The URL→backend detection in `settings_factory.py` is a real feature worth preserving.

8. **No file in the package was classified `move-to-expressions`.** The audit confirms data and expressions are genuinely complementary — there is no logical/relational logic hiding in `mountainash-data` that needs to migrate out.

### Tally

| Classification | Files |
|---|---|
| `keep-as-is` | 19 |
| `adapt` | 9 |
| `salvage` | 29 |
| `delete` | 10 |
| `move-to-expressions` | 0 |

## Decisions

### D1 — pyiceberg connection/ops split

**Decision: D1.b — keep the split, deduplicate.** The previous split was only half done; duplicate methods exist between `base_pyiceberg_connection.py` and `base_pyiceberg_operations.py`. Migration finishes the split: catalog/namespace lifecycle stays in `backends/iceberg/connection.py`, table-mutation methods go to `backends/iceberg/operations.py`, type-conversion helpers go to `backends/iceberg/_types.py`. During Phase 3, every duplicate method must be identified and a canonical version chosen.

### D2 — Factories vs. dialect registry

**Decision: D2.a — keep factories, point them at the registry.** The `factories/` layer moves to `core/factories/` largely unchanged. `connection_factory.py` and `operations_factory.py` strategy mappings are updated to point at `IbisBackend` (with dialect arg) and `IcebergBackend` instead of the old per-backend classes. `settings_factory.py`'s URL detection logic stays as-is — it is the real feature worth keeping. The dialect registry becomes the *data* the factories iterate over, not a replacement for them.

### D3 — Trino's HYBRID connection mode

**Decision: D3.b — defer.** `trino_ibis_operations.py`'s 34-line init sets `_ibis_connection_mode = HYBRID`. Whether HYBRID is trino-specific or a general capability cannot be determined from one file. During Phase 4 migration, grep `_ibis_connection_mode` across the codebase and place the flag accordingly: as a general capability in the registry, or as a trino-specific dialect entry field.

## Migration sequence

Phases are sized so each ends with a working package and tests passing. No phase leaves the tree in a half-rewritten state for more than a single PR.

### Phase 0 — Cleanup (no architectural change)

- Delete `lineage/openlineage_helper.py` (3-line stub) and the empty `lineage/` dir
- Delete `databases/connections/pyiceberg/__init___old.py`
- Delete `databases/connections/db_connection_factory.py` (legacy duplicate)
- Delete the 8 stub ibis ops files: postgres, mysql, oracle, bigquery, snowflake, pyspark, redshift, mssql
- Update `databases/operations/ibis/__init__.py` to drop the deleted re-exports
- Run tests

### Phase 1 — Stand up `core/`

- Create `core/` with `protocol.py`, `inspection.py`, `registry.py` (placeholder, wired in Phase 4)
- Move `databases/constants.py` → `core/constants.py`
- Move `databases/connections/base_db_connection.py` → `core/connection.py` (the new `Backend` Protocol references it; exact form determined when writing)
- Add re-export shims at the old paths
- Run tests

### Phase 2 — Move settings to `core/settings/`

- Move all 16 files from `databases/settings/` → `core/settings/` verbatim
- Re-export shims at old paths
- Run tests

### Phase 3 — Iceberg backend (D1.b: split, deduplicate)

- Create `backends/iceberg/`
- Move `base_pyiceberg_connection.py` (884) and `base_pyiceberg_operations.py` (868) into the new dir as a starting point
- **Deduplicate**: identify methods present in both files (the half-done split) and pick the canonical version. Connection-lifecycle and catalog/namespace methods → `backends/iceberg/connection.py`. Table-mutation methods → `backends/iceberg/operations.py`. Type-conversion helpers → `backends/iceberg/_types.py`.
- Move REST-specific files into `backends/iceberg/catalogs/rest.py` (merging connection + operations REST files)
- Implement the `core.Backend` Protocol on `IcebergBackend`
- Implement `inspect()` returning shared inspection model dataclasses
- `to_relation()` left unimplemented (or omitted) — flagged as a gap to fix once `mountainash-expressions` has an iceberg adapter
- Update factory mappings
- Re-export shims at old paths
- Run tests

### Phase 4 — Ibis backend

- Create `backends/ibis/`
- Create `backends/ibis/dialects/_registry.py` with one `DialectSpec` entry per backend, populated by reading the 13 existing connection files and extracting: connection-builder callable, ibis backend name, connection mode, per-dialect quirks
- Move `base_ibis_connection.py` → `backends/ibis/connection.py`, refactored to consume `DialectSpec` instead of being subclassed
- Move `base_ibis_operations.py` (676 lines, the actual implementation) → `backends/ibis/operations.py`
- Fold `_base_ibis_mixin.py` (98) → into `backends/ibis/operations.py` as helper functions (no longer a mixin)
- Fold `_duckdb_family_mixin.py` (314) → into `backends/ibis/operations.py` as functions, attached to duckdb/motherduck/sqlite dialect entries via `DialectSpec` capability hooks
- Salvage concrete duckdb/sqlite/motherduck ops files: extract dialect-specific SQL into corresponding `DialectSpec` entries, delete the source files
- **Trino ops file (D3.b):** grep for `_ibis_connection_mode`, decide whether HYBRID is general or trino-specific, place the flag accordingly, delete the file
- Implement `core.Backend` Protocol on `IbisBackend(dialect: str)`
- Implement `inspect()` returning shared inspection model dataclasses
- Implement `to_relation(table_name)` handing back a `mountainash-expressions` Relation
- Delete the now-empty `databases/connections/ibis/` and `databases/operations/ibis/` directories
- Re-export shims at old paths (removed in Phase 6)
- Run tests

### Phase 5 — Wire factories to the registry (D2.a)

- Move `factories/` → `core/factories/` verbatim
- Update `connection_factory.py` and `operations_factory.py` strategy mappings to point at `IbisBackend` (with dialect arg) and `IcebergBackend`
- `settings_factory.py`'s URL detection logic stays as-is
- Move `database_utils.py` → `core/utils.py`, update to consume the new factories
- Run tests

### Phase 6 — Remove shims and finalize

- Delete all re-export shims at `databases/...` paths
- Delete the now-empty `databases/` directory
- Update top-level `src/mountainash_data/__init__.py` to export the new public surface: `Backend` protocol, `IbisBackend`, `IcebergBackend`, inspection model dataclasses, factories, `database_utils` facade, settings classes
- Update `notebooks/` and `docs/` examples to use the new imports
- Update `README.md` and `CLAUDE.md` to reflect the new architecture
- Run tests, run ruff, run mypy

## Test strategy

Each phase ends with `hatch run test:test` green. Phases 3 and 4 are the risky ones. The audit did not inspect `tests/`; the writing-plans phase will need a pass over the test tree to identify coverage gaps in iceberg and ibis operations *before* those phases begin. If coverage is thin, add tests first, refactor second.

## Known gaps and follow-ups

- **`IcebergBackend.to_relation()`** is unimplemented at the end of migration. Resolving it requires `mountainash-expressions` to gain an iceberg relation adapter. Tracked separately.
- **HYBRID connection mode (D3)** is resolved during Phase 4 by grep, not pre-decided here.
- **Test coverage audit** for `tests/iceberg/` and `tests/ibis/operations/` happens in writing-plans, not here.
