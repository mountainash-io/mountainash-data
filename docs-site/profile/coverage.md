# Coverage Report — mountainash-data

**Source hash:** 2d0782b507141baecd5a120ac0547c7e8af11662 (`docs/textbook-rebuild`)
**Previous hash:** 17d9ed3ae77f05bc1eea90f14294f732fb26ea5e
**Behavior:** incremental (no source changes)

## Incremental refresh (2026-10-29)

No file under `src/mountainash_data` changed between `17d9ed3` and `2d0782b`; the intervening commits touched only `CLAUDE.md` and this profile. All 30 module records stay as written at `17d9ed3` (their `source_hash`), and their facts still hold at `2d0782b`. Nothing is stale or orphaned.

The two low-confidence review flags below (`core.registry`, `core.constants`) are **resolved for documentation purposes**. The owner chose to fix them in code, tracked as DEBT-25 in `mountainash-central/04.planning/mountainash-data/a.backlog/unwired-core-registry-and-constants.md`. Until then, docs treat both modules as internal/unwired.

## Full refresh at 17d9ed3

## Why full refresh

- 76 source files changed since the previous profile (20 added, 15 deleted, 41 modified); changed modules exceed the 1/3 threshold.
- Module boundaries were added and removed: the Iceberg backend was retired and new private Ibis modules appeared (`_transaction`, `_index`, `_render`, `_raw`, `_adoption`, `_protected`, compat shims).
- Every prior module record failed the current `module-profile.schema.json` (object-shaped `public_api` / `key_classes` entries).
- `docs-site/profile/README.md` (a stale vision document advertising Iceberg) was deleted on user instruction; `facets/broader-hype.json` was previously unmapped and is now in the manifest.

## Summary

| Metric | Count |
|--------|-------|
| Modules discovered | 36 |
| Modules profiled | 30 |
| Paths ignored (package markers / version string) | 6 |
| Missing profiles | 0 |
| Stale profiles | 0 |
| Orphan profiles removed | 7 |
| Facets written | 6 |

## Orphaned records removed

Source no longer exists at the target hash; records deleted so the manifest map stays exact.

| Former module | Reason |
|---|---|
| `mountainash_data.core.connection` | `core/connection.py` deleted; `Backend` protocol is the single contract |
| `mountainash_data.core.settings.auth` | Auth types now re-exported from `mountainash_auth_client` via `core.settings` |
| `mountainash_data.backends.iceberg.backend` / `.catalogs` / `.connection` / `.inspect` / `.operations` | Iceberg backend retired |

## Test coverage gaps

| Module | Priority | Gap |
|---|---|---|
| `mountainash_data.core.registry` | useful | No test and no importer; behaviour unverified |
| `mountainash_data.backends.ibis._protected` | internal-note | Covered only indirectly via transaction-scope tests |
| `mountainash_data.core._redact` | internal-note | Covered only indirectly via credential-URL tests |
| `mountainash_data.backends.ibis.dialects` | essential | `upsert_hook`, `add_columns_hook`, `rename_table_hook`, `create_index_hook`, `drop_index_hook` seams have no in-tree consumer |
| `mountainash_data.backends.ibis.backend` | essential | `from_raw_connection` verified only for duckdb/motherduck |
| `mountainash_data.backends.ibis.operations` | useful | MySQL upsert tested on MariaDB only (`VALUES()` deprecated on MySQL 8.0.20+) |

## Low-confidence classifications

| Module | Confidence | Open question |
|---|---|---|
| `mountainash_data.core.registry` | medium | Resolved → DEBT-25: retained per DEBT-6; tests and public-reach statement to follow. Docs: internal/unwired. |
| `mountainash_data.core.constants` | medium | Resolved → DEBT-25: remove-vs-adopt per enum pending. Docs: only `CONST_DB_PROVIDER_TYPE` is load-bearing. |

## Documentation drift (source of truth disagrees with docs)

- **CLAUDE.md** (fixed in #148) described `BackendDescriptor`/`ProfileDescriptor`, `ConnectionProfile.to_driver_kwargs()`/`to_connection_string()`, `AuthSpec` subclasses, `*AuthSettings(..., auth=NoAuth())`, `OperationsFactory`, `SettingsFactory`, `DatabaseUtils` and per-dialect connection subclasses. Code: `BackendSpec`, `BackendProfile`, `*BackendProfile`, `*AuthProfile` (from `mountainash_auth_client`), `ConnectionFactory` only, and a single `IbisBackend`. Evidence: `core.settings`, `core.settings.descriptor`, `core.settings.profile`, `core.settings.backends`, `core.factories` records.
- **`src/mountainash_data/__init__.py`** docstring omits `IndexInfo`, `Namespace`, `NamespaceLike` from `__all__`.
- **`IbisConnection` docstrings** reference `core.protocol.Connection`, which no longer exists.
- **`core/constants.py`** header names a non-existent `databases/constants.py` path.
- **`backends/ibis/operations.py`** error messages recommend an `upsert_hook` override no dialect provides.

## Notable risks (maintainer facet)

- Dead or unread code: `motherduck_list_tables`, `_format_qualified_table`, `DialectSpec.connection_mode` / `extras`, `raw_fetch_scalar` hook argument.
- Oracle compat patch is applied in the connection builder, so connections adopted via `from_ibis_connection` are unpatched.
- `operations` ↔ `dialects` circular import held together by a bottom-of-module import.
- `IbisConnection.list_*` print errors to stdout and return `[]` outside a transaction scope.
- Ibis private-API coupling in `_exasol_compat`, `_oracle_compat`, `_protected`, `_render`.
- `_sqlite_compat` registers a process-global sqlite3 adapter.
- `snowflake.oauth2` adapter ignores client id/secret/scope.
- Importing `core.settings` eagerly imports all 20 backend modules.

## Audience coverage

| Audience | Featured | Concepts | Notes |
|---|---|---|---|
| users | 7 | 7 | Transaction exceptions not exported from the package root |
| maintainers | 6 | 8 | — |
| contributors | 4 | 4 | Depends on CLAUDE.md being corrected |
| backend-architecture | 5 | 6 | — |
| extension-authors | 4 | 4 | `core.registry` unwired |
| broader-hype | 4 | 4 | Lakehouse/Iceberg positioning retired; proof points limited to `examples/` and live-backend tests |
