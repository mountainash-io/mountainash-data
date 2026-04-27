# Settings-Aware Backends + to_relation()

> **Date:** 2026-04-26 (updated 2026-04-27)
> **Status:** ABANDONED -- superseded by `2026-04-27-settings-aware-ibis-backend-design.md`. to_relation() descoped per revised principle.
> **Backlog refs:**
> - `mountainash-central/01.principles/mountainash-data/f.backlog/to-relation-gap.md`
> - `mountainash-central/01.principles/mountainash-data/f.backlog/settings-aware-backends.md` (Phase 1)

## Context

mountainash-data has two parallel connection paths that produce the same
result with different indirection:

1. **New-style:** `IbisBackend(dialect="sqlite", **config).connect()` →
   `IbisConnection` (protocol-compliant)
2. **Settings-driven:** `ConnectionFactory.get_connection(settings_params)` →
   `BaseIbisConnection` subclass (old hierarchy)

The gap: `IbisBackend` only accepts direct config. Consumers with a
`SettingsParameters` must use the old factory path or manually resolve
settings. This keeps the factory alive despite adding no value post-refactor.

Separately, `to_relation()` is defined on the `Connection` protocol concept
but not yet implemented. The `mountainash` package (formerly
mountainash-expressions) provides `relation()` which accepts ibis tables
and wraps them in a `Relation` AST node.

This spec covers:
- Making `IbisBackend` settings-aware (Phase 1 of settings-aware-backends backlog)
- Wiring `to_relation()` on both connection paths

## Design

### 1. Settings-aware IbisBackend

`IbisBackend.__init__` accepts either a dialect string + kwargs (existing)
or a `SettingsParameters` object (new):

```python
class IbisBackend:
    name = "ibis"

    def __init__(self, dialect_or_settings: str | SettingsParameters, **config: Any):
        if isinstance(dialect_or_settings, SettingsParameters):
            settings_params = dialect_or_settings
            settings = settings_params.settings_class.get_settings(settings_params)
            descriptor = settings.__descriptor__
            ibis_dialect = descriptor.ibis_dialect
            if ibis_dialect not in DIALECTS:
                raise KeyError(
                    f"Unknown ibis dialect {ibis_dialect!r}. "
                    f"Available: {sorted(DIALECTS)}"
                )
            self.dialect = ibis_dialect
            self._spec = DIALECTS[ibis_dialect]
            self._config = settings.to_driver_kwargs()
        else:
            dialect = dialect_or_settings
            if dialect not in DIALECTS:
                raise KeyError(
                    f"Unknown ibis dialect {dialect!r}. "
                    f"Available: {sorted(DIALECTS)}"
                )
            self.dialect = dialect
            self._spec = DIALECTS[dialect]
            self._config = config
```

Usage:

```python
from mountainash_data import IbisBackend

# Direct
backend = IbisBackend(dialect="sqlite", database=":memory:")

# Settings-driven
backend = IbisBackend(settings_params)

# Both produce the same IbisConnection
conn = backend.connect()
```

### 2. Protocol — to_relation()

Add `to_relation()` to the `Connection` protocol in `core/protocol.py`:

```python
def to_relation(self, name: str, namespace: str | None = None) -> "Relation":
    """Return a mountainash Relation for the named table.

    Requires the mountainash package. Raises ImportError if not installed.
    """
    ...
```

Signature matches `inspect_table(name, namespace)` for consistency. Return
type uses a string forward reference to avoid importing mountainash at module
level.

### 3. Ibis implementation — new-style path (IbisConnection)

In `backends/ibis/backend.py`, add to `IbisConnection`:

```python
def to_relation(self, name: str, namespace: str | None = None) -> "Relation":
    try:
        from mountainash.relations import relation
    except ImportError:
        raise ImportError(
            "mountainash package is required for to_relation(). "
            "Install it with: pip install mountainash"
        )
    ibis_table = self._ibis_conn.table(name, database=namespace)
    return relation(ibis_table)
```

### 4. Ibis implementation — settings/factory path (BaseIbisConnection)

In `backends/ibis/connection.py`, add to `BaseIbisConnection`:

```python
def to_relation(self, name: str, namespace: str | None = None) -> "Relation":
    try:
        from mountainash.relations import relation
    except ImportError:
        raise ImportError(
            "mountainash package is required for to_relation(). "
            "Install it with: pip install mountainash"
        )
    self.connect()
    ibis_table = self.ibis_backend.table(name, database=namespace)
    return relation(ibis_table)
```

This covers connections obtained via `ConnectionFactory` and `DatabaseUtils`.
The factory path gets `to_relation()` now; it will be deprecated in Phase 3
of the settings-aware-backends backlog.

### 5. Iceberg stub

In `backends/iceberg/connection.py`, add to `IcebergConnectionBase`:

```python
def to_relation(self, name: str, namespace: str | None = None) -> "Relation":
    raise NotImplementedError(
        "to_relation() is not yet supported for Iceberg connections. "
        "Use table() to get the native pyiceberg Table object."
    )
```

### 6. Dependency

Add `mountainash` as an optional extra in `pyproject.toml`:

```toml
[project.optional-dependencies]
relations = ["mountainash"]
```

The core package does NOT depend on mountainash — the import is guarded at
call time in `to_relation()`.

### 7. Extensibility pattern

Future backends (DataFusion, etc.) follow the same two-step pattern:

1. Make the backend class settings-aware (`__init__` accepts `SettingsParameters`)
2. Add `to_relation()` — get native table handle, pass to `relation()`

The only prerequisite is that `identify_backend()` in mountainash recognises
the native table type and a corresponding relation system backend exists.

## Testing

### Settings-aware IbisBackend
- Construct `IbisBackend(settings_params)` with SQLite/DuckDB settings,
  verify `.connect()` returns working `IbisConnection`
- Verify `IbisBackend(settings_params).dialect` matches expected dialect
- Verify invalid settings raise `KeyError`
- Verify existing direct path still works unchanged

### to_relation()
- `IbisConnection.to_relation()` (new-style) with in-memory DuckDB: create
  table, call `to_relation()`, verify returns `Relation` instance
- `BaseIbisConnection.to_relation()` (factory path) via
  `ConnectionFactory.get_connection()`: same verification
- Round-trip test: `to_relation()` → `.collect()` returns expected data
- `ImportError` path: mock the import to verify clear error message
- `IcebergConnectionBase.to_relation()` raises `NotImplementedError`

## Commit strategy

Three commits, single branch, single PR targeting `develop`:

1. `feat(backend): make IbisBackend settings-aware`
2. `feat(protocol): add to_relation() to Connection protocol and Ibis implementations`
3. `feat(deps): add mountainash as optional relations extra`

## Follow-up (not in this PR)

- **Phase 2:** Migrate `DatabaseUtils` consumers to use `IbisBackend(settings_params)` directly
- **Phase 3:** Deprecate `ConnectionFactory`, `OperationsFactory`, `DatabaseUtils.create_connection()`/`create_operations()`, and the 12 concrete `BaseIbisConnection` subclasses

Tracked in: `mountainash-central/01.principles/mountainash-data/f.backlog/settings-aware-backends.md`

## Backlog updates after merge

- `to-relation-gap.md` → **RESOLVED** (Ibis wired; Iceberg stub)
- `settings-aware-backends.md` → Phase 1 **RESOLVED**
