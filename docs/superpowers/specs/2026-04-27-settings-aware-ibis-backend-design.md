# Settings-Aware IbisBackend

> **Date:** 2026-04-27
> **Status:** Approved
> **Principle:** `mountainash-central/01.principles/mountainash-data/b.connection-management/connection-consolidation.md` (Phase 1)
> **Backlog ref:** `mountainash-central/01.principles/mountainash-data/f.backlog/settings-aware-backends.md`
> **Supersedes:** `docs/superpowers/specs/2026-04-26-to-relation-design.md` (ABANDONED)

## Goal

Extend `IbisBackend` to accept `SettingsParameters` and connection URLs
alongside the existing dialect keyword. All three input forms resolve to
the same internal state and produce `IbisConnection` via `connect()`.
This is Phase 1 of connection consolidation.

## Constructor Signature

```python
def __init__(
    self,
    settings_or_connection_string: str | SettingsParameters | None = None,
    /,
    *,
    dialect: str | None = None,
    **config: t.Any,
):
```

### Input Forms

```python
from mountainash_data import IbisBackend
from mountainash_settings import SettingsParameters
from mountainash_data.core.settings import DuckDBAuthSettings, NoAuth

# Form 1: Settings object (deployment, env-driven config)
settings_params = SettingsParameters.create(
    settings_class=DuckDBAuthSettings,
    DATABASE=":memory:",
    auth=NoAuth(),
)
backend = IbisBackend(settings_params)

# Form 2: Connection URL (universal connection strings)
backend = IbisBackend("postgresql://user:pass@host:5432/db")

# Form 3: Dialect keyword + kwargs (tests, scripts)
backend = IbisBackend(dialect="sqlite", database=":memory:")

# All produce the same IbisConnection
conn = backend.connect()
```

## Dispatch Logic

The constructor resolves all input forms to `(self.dialect, self._spec,
self._config)` so that `connect()` requires no changes beyond empty-list
normalization.

1. **Both positional and `dialect=` provided** -> `ValueError`.
2. **Positional is `SettingsParameters`** -> settings path:
   - `settings_class.get_settings(settings_parameters)` to resolve.
   - `descriptor.ibis_dialect` to get dialect name.
   - `to_driver_kwargs()` for config dict.
   - `**config` kwargs merged on top (caller overrides).
3. **Positional is `str` containing `://`** -> URL path:
   - Detect dialect from URL scheme using a reverse lookup on
     `DialectSpec.connection_string_scheme`.
   - Store the raw URL as `connection_string` in config — the dialect
     builder passes it straight to `ibis.connect(url)`.
   - `**config` kwargs merged on top (caller overrides).
4. **Positional is a plain `str`** -> treat as dialect name (same as
   `dialect=` keyword).
5. **`dialect` keyword provided** -> existing dialect path (unchanged).
6. **Neither provided** -> `ValueError`.

### Settings Resolution Detail

```python
# Inside __init__, settings path:
from mountainash_settings import SettingsParameters

obj_settings = settings_or_connection_string.settings_class.get_settings(
    settings_parameters=settings_or_connection_string
)
descriptor = getattr(obj_settings, "__descriptor__", None)
if descriptor is None or descriptor.ibis_dialect is None:
    raise ValueError(
        f"Settings class {type(obj_settings).__name__} has no ibis_dialect on its descriptor"
    )
resolved_dialect = descriptor.ibis_dialect
driver_kwargs = obj_settings.to_driver_kwargs()
driver_kwargs.update(config)  # caller overrides
```

### URL Resolution Detail

The URL path bypasses settings entirely. The raw URL is passed to the
dialect builder as `connection_string`, which forwards it to
`ibis.connect(url)`. This preserves all URL components (host, port,
credentials, database, query params) without lossy round-tripping
through settings fields.

Dialect detection uses a reverse lookup built from the `DIALECTS`
registry — each `DialectSpec` already carries `connection_string_scheme`.

```python
# Inside __init__, URL path:
from urllib.parse import urlparse

# Build reverse scheme -> dialect map from registry
# e.g. {"sqlite": "sqlite", "duckdb": "duckdb", "postgres": "postgres", ...}
scheme = urlparse(settings_or_connection_string).scheme.lower()

# Special cases: "postgresql" -> "postgres", "md" -> "motherduck"
# MotherDuck also detected by "duckdb://md:" prefix
resolved_dialect = _SCHEME_TO_DIALECT.get(scheme)
if resolved_dialect is None:
    raise ValueError(
        f"Cannot detect ibis dialect from URL scheme: {scheme!r}"
    )

# Store raw URL as connection_string — builders pass it to ibis.connect()
driver_kwargs = {"connection_string": settings_or_connection_string}
driver_kwargs.update(config)  # caller overrides
```

The `_SCHEME_TO_DIALECT` map is built once at module level from the
`DIALECTS` registry, with additional aliases for common scheme variants
(e.g. `postgresql` -> `postgres`).

## Empty-List Normalization

Happens in `connect()`, not `__init__`. Some ibis drivers reject empty
sequences (e.g. `ibis.duckdb.connect(extensions=[])` fails).

```python
def connect(self) -> IbisConnection:
    cleaned_config = {
        k: v for k, v in self._config.items()
        if not (isinstance(v, (list, tuple)) and len(v) == 0)
    }
    ibis_conn = self._spec.connection_builder(**cleaned_config)
    return IbisConnection(ibis_conn, self._spec)
```

## Files Changed

| File | Change |
|------|--------|
| `src/mountainash_data/backends/ibis/backend.py` | New constructor signature, dispatch logic, empty-list normalization in `connect()`, `_SCHEME_TO_DIALECT` map |
| `tests/test_unit/backends/ibis/test_backend.py` | New tests for settings, URL, and error paths |

## Files NOT Changed

- `IbisConnection` -- untouched
- `DialectSpec` registry / builders -- untouched (URL path uses existing `connection_string` kwarg)
- `ConnectionFactory`, `DatabaseUtils`, `BaseIbisConnection` -- no deprecation yet (Phase 2)
- `core/protocol.py` -- untouched
- No new files

## Testing

All tests use SQLite and DuckDB (in-memory, no external deps).

### 1. Dialect Path (existing, unchanged)

```python
def test_dialect_path():
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    conn = backend.connect()
    assert isinstance(conn, IbisConnection)
    conn.close()
```

### 2. Settings Path

```python
def test_settings_path_sqlite():
    from mountainash_settings import SettingsParameters
    from mountainash_data.core.settings import SQLiteAuthSettings, NoAuth

    params = SettingsParameters.create(
        settings_class=SQLiteAuthSettings,
        DATABASE=":memory:",
        auth=NoAuth(),
    )
    backend = IbisBackend(params)
    conn = backend.connect()
    assert isinstance(conn, IbisConnection)
    tables = conn.list_tables()
    assert isinstance(tables, list)
    conn.close()

def test_settings_path_duckdb_empty_extensions():
    """DuckDB settings with default EXTENSIONS=[] must not reach ibis."""
    from mountainash_settings import SettingsParameters
    from mountainash_data.core.settings import DuckDBAuthSettings, NoAuth

    params = SettingsParameters.create(
        settings_class=DuckDBAuthSettings,
        DATABASE=":memory:",
        auth=NoAuth(),
    )
    backend = IbisBackend(params)
    conn = backend.connect()  # Must not raise
    assert isinstance(conn, IbisConnection)
    conn.close()
```

### 3. URL Path

```python
def test_url_path_sqlite():
    backend = IbisBackend("sqlite://")
    conn = backend.connect()
    assert isinstance(conn, IbisConnection)
    conn.close()

def test_url_path_duckdb():
    backend = IbisBackend("duckdb://")
    conn = backend.connect()
    assert isinstance(conn, IbisConnection)
    conn.close()

def test_url_path_preserves_database(tmp_path):
    """URL database component must reach the driver, not be discarded."""
    db_file = tmp_path / "test.db"
    backend = IbisBackend(f"sqlite:///{db_file}")
    conn = backend.connect()
    assert isinstance(conn, IbisConnection)
    conn.close()
    assert db_file.exists()

def test_url_path_unknown_scheme_raises():
    with pytest.raises(ValueError, match="Cannot detect ibis dialect"):
        IbisBackend("nosuch://localhost/db")
```

### 4. Error Cases

```python
def test_both_positional_and_dialect_raises():
    with pytest.raises(ValueError):
        IbisBackend("sqlite://", dialect="sqlite")

def test_neither_provided_raises():
    with pytest.raises(ValueError):
        IbisBackend()

def test_unknown_dialect_raises():
    with pytest.raises(KeyError):
        IbisBackend(dialect="nosuch")
```

## Commit Strategy

Single branch (`feature/settings-aware-ibis-backend`) targeting `develop`.
Two commits:

1. `feat(backend): make IbisBackend settings-aware` -- constructor + tests
2. `chore(specs): abandon old to-relation spec and plan` -- mark old docs
