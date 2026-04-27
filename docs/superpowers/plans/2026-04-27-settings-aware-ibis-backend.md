# Settings-Aware IbisBackend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `IbisBackend` to accept `SettingsParameters` and connection URLs alongside the existing dialect keyword, producing `IbisConnection` from all three input forms.

**Architecture:** The `IbisBackend.__init__` constructor gains a positional-only first parameter that accepts `SettingsParameters` or a URL string. A `dialect=` keyword preserves the existing path. All forms resolve to `(self.dialect, self._spec, self._config)` so `connect()` stays simple. A module-level `_SCHEME_TO_DIALECT` map (built from the `DIALECTS` registry) handles URL scheme detection.

**Tech Stack:** ibis-framework, mountainash-settings (`SettingsParameters`), mountainash-data settings (`ConnectionProfile`, `BackendDescriptor`)

**Spec:** `docs/superpowers/specs/2026-04-27-settings-aware-ibis-backend-design.md`

---

## File Map

| File | Responsibility | Change |
|------|---------------|--------|
| `src/mountainash_data/backends/ibis/backend.py` | `IbisBackend` + `IbisConnection` | New constructor, `_SCHEME_TO_DIALECT` map, empty-list filter in `connect()` |
| `tests/test_unit/backends/ibis/test_backend.py` | Unit tests for `IbisBackend` | New tests for settings, URL, error paths |

No new files. `IbisConnection`, `DialectSpec` registry, and builders are untouched.

---

### Task 1: Error-case tests and `_SCHEME_TO_DIALECT` map

Establishes the constructor signature, dispatch validation, and the scheme lookup — all tested before the happy paths.

**Files:**
- Modify: `src/mountainash_data/backends/ibis/backend.py:106-142`
- Modify: `tests/test_unit/backends/ibis/test_backend.py`

- [ ] **Step 1: Write failing tests for constructor validation**

Add to `tests/test_unit/backends/ibis/test_backend.py`:

```python
import pytest
from mountainash_data.backends.ibis.backend import IbisBackend


def test_neither_positional_nor_dialect_raises():
    """Constructor with no arguments must raise ValueError."""
    with pytest.raises(ValueError, match="Either.*or.*dialect"):
        IbisBackend()


def test_both_positional_and_dialect_raises():
    """Cannot supply both a positional arg and dialect= keyword."""
    with pytest.raises(ValueError, match="Cannot specify both"):
        IbisBackend("sqlite://", dialect="sqlite")


def test_unknown_url_scheme_raises():
    """URL with unrecognised scheme must raise ValueError."""
    with pytest.raises(ValueError, match="Cannot detect ibis dialect"):
        IbisBackend("nosuch://localhost/db")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend.py::test_neither_positional_nor_dialect_raises tests/test_unit/backends/ibis/test_backend.py::test_both_positional_and_dialect_raises tests/test_unit/backends/ibis/test_backend.py::test_unknown_url_scheme_raises -v`

Expected: FAIL — the current constructor signature `__init__(self, dialect: str, **config)` doesn't match these call forms.

- [ ] **Step 3: Implement `_SCHEME_TO_DIALECT` map and new constructor**

Replace the `IbisBackend` class in `src/mountainash_data/backends/ibis/backend.py` (lines 106–142) with:

```python
# ---------------------------------------------------------------------------
# Scheme → dialect reverse lookup (built once from the DIALECTS registry)
# ---------------------------------------------------------------------------
def _build_scheme_to_dialect() -> dict[str, str]:
    """Build a map from URL scheme (e.g. 'sqlite', 'postgres') to dialect name."""
    result: dict[str, str] = {}
    for dialect_name, spec in DIALECTS.items():
        # connection_string_scheme is e.g. "postgres://", "duckdb://md:"
        scheme = spec.connection_string_scheme.split("://")[0].lower()
        # First dialect wins — e.g. "postgres" maps to "postgres", not "redshift"
        if scheme not in result:
            result[scheme] = dialect_name
    # Common aliases
    result.setdefault("postgresql", result.get("postgres", "postgres"))
    return result


_SCHEME_TO_DIALECT: dict[str, str] = _build_scheme_to_dialect()


class IbisBackend:
    """Ibis backend — single entry point for all Ibis connections.

    Three input forms, all producing IbisConnection via connect():

        # Settings object (deployment, env-driven config)
        backend = IbisBackend(settings_params)

        # Connection URL (universal connection strings)
        backend = IbisBackend("postgresql://user:pass@host:5432/db")

        # Dialect keyword + kwargs (tests, scripts)
        backend = IbisBackend(dialect="sqlite", database=":memory:")
    """

    name = "ibis"

    def __init__(
        self,
        settings_or_connection_string: str | t.Any | None = None,
        /,
        *,
        dialect: str | None = None,
        **config: t.Any,
    ):
        if settings_or_connection_string is not None and dialect is not None:
            raise ValueError(
                "Cannot specify both a positional settings/URL argument "
                "and dialect= keyword"
            )

        if settings_or_connection_string is not None:
            self._init_from_positional(settings_or_connection_string, config)
        elif dialect is not None:
            self._init_from_dialect(dialect, config)
        else:
            raise ValueError(
                "Either a SettingsParameters/URL positional argument "
                "or a dialect= keyword is required"
            )

    def _init_from_positional(
        self, value: str | t.Any, config: dict[str, t.Any]
    ) -> None:
        # Lazy import — only pay for it on the settings/URL paths
        from mountainash_settings import SettingsParameters

        if isinstance(value, SettingsParameters):
            self._init_from_settings(value, config)
        elif isinstance(value, str):
            if "://" in value:
                self._init_from_url(value, config)
            else:
                # Plain string — treat as dialect name
                self._init_from_dialect(value, config)
        else:
            raise TypeError(
                f"Expected SettingsParameters or str, got {type(value).__name__}"
            )

    def _init_from_dialect(
        self, dialect_name: str, config: dict[str, t.Any]
    ) -> None:
        if dialect_name not in DIALECTS:
            raise KeyError(
                f"Unknown ibis dialect {dialect_name!r}. "
                f"Available: {sorted(DIALECTS)}"
            )
        self.dialect = dialect_name
        self._spec: DialectSpec = DIALECTS[dialect_name]
        self._config = config

    def _init_from_dialect(
        self, dialect_name: str, config: dict[str, t.Any]
    ) -> None:
        if dialect_name not in DIALECTS:
            raise KeyError(
                f"Unknown ibis dialect {dialect_name!r}. "
                f"Available: {sorted(DIALECTS)}"
            )
        self.dialect = dialect_name
        self._spec: DialectSpec = DIALECTS[dialect_name]
        self._url: str | None = None
        self._config = config

    def _init_from_url(
        self, url: str, config: dict[str, t.Any]
    ) -> None:
        from urllib.parse import urlparse

        scheme = urlparse(url).scheme.lower()

        # Special case: MotherDuck URLs are "duckdb://md:..."
        if scheme == "duckdb" and url.startswith("duckdb://md:"):
            resolved_dialect = "motherduck"
        else:
            resolved_dialect = _SCHEME_TO_DIALECT.get(scheme)

        if resolved_dialect is None:
            raise ValueError(
                f"Cannot detect ibis dialect from URL scheme: {scheme!r}"
            )

        self.dialect = resolved_dialect
        self._spec = DIALECTS[resolved_dialect]
        self._url = url
        self._config = config

    def _init_from_settings(
        self, settings_params: t.Any, config: dict[str, t.Any]
    ) -> None:
        obj_settings = settings_params.settings_class.get_settings(
            settings_parameters=settings_params
        )
        descriptor = getattr(obj_settings, "__descriptor__", None)
        if descriptor is None or getattr(descriptor, "ibis_dialect", None) is None:
            raise ValueError(
                f"Settings class {type(obj_settings).__name__} has no "
                f"ibis_dialect on its descriptor"
            )
        resolved_dialect = descriptor.ibis_dialect
        if resolved_dialect not in DIALECTS:
            raise KeyError(
                f"Unknown ibis dialect {resolved_dialect!r} from descriptor. "
                f"Available: {sorted(DIALECTS)}"
            )
        driver_kwargs = obj_settings.to_driver_kwargs()
        driver_kwargs.update(config)

        self.dialect = resolved_dialect
        self._spec = DIALECTS[resolved_dialect]
        self._url = None
        self._config = driver_kwargs

    def connect(self) -> IbisConnection:
        """Build and return a live ibis connection."""
        if self._spec.connection_builder is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} has no connection_builder configured"
            )
        if self._url is not None:
            # URL path: delegate directly to ibis.connect() which
            # natively handles all URL forms and preserves all URL
            # components (host, port, credentials, database, query params).
            import ibis
            ibis_conn = ibis.connect(self._url, **self._config)
        else:
            # Settings/dialect path: go through the dialect builder
            # with empty-list normalization (e.g. DuckDB extensions=[]).
            cleaned_config = {
                k: v for k, v in self._config.items()
                if not (isinstance(v, (list, tuple)) and len(v) == 0)
            }
            ibis_conn = self._spec.connection_builder(**cleaned_config)
        return IbisConnection(ibis_conn, self._spec)
```

Key details:
- The type annotation for `settings_or_connection_string` uses `str | t.Any | None` rather than `str | SettingsParameters | None` to avoid a top-level import of `mountainash_settings`. The `SettingsParameters` isinstance check happens inside `_init_from_positional` with a lazy import.
- `_init_from_url` stores the raw URL on `self._url`. In `connect()`, when `_url` is set, the URL is passed directly to `ibis.connect(url)` — this natively handles all URL forms for all backends and preserves all URL components. The dialect builders are NOT used for the URL path (they don't all handle `connection_string` kwargs).
- `_init_from_settings` resolves the settings object, extracts `ibis_dialect` from the descriptor, and calls `to_driver_kwargs()`. Sets `_url = None`.
- `_init_from_dialect` is the existing path, unchanged except for initialising `_url = None`.
- `connect()` branches on `self._url`: URL path uses `ibis.connect(url)`, settings/dialect path uses the builder with empty-list filtering. The empty-list filter is essential because `DuckDBAuthSettings.to_driver_kwargs()` returns `extensions: []` by default, which `ibis.duckdb.connect()` rejects.

- [ ] **Step 4: Run the three error tests**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend.py::test_neither_positional_nor_dialect_raises tests/test_unit/backends/ibis/test_backend.py::test_both_positional_and_dialect_raises tests/test_unit/backends/ibis/test_backend.py::test_unknown_url_scheme_raises -v`

Expected: PASS — all three error cases now handled by the new constructor.

- [ ] **Step 5: Run existing tests to verify no regressions**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend.py -v`

Expected: All 4 existing tests + 3 new tests PASS. The existing tests use `IbisBackend(dialect="sqlite", ...)` which hits `_init_from_dialect` — the unchanged path.

- [ ] **Step 6: Commit**

```bash
git add src/mountainash_data/backends/ibis/backend.py tests/test_unit/backends/ibis/test_backend.py
git commit -m "feat(backend): new IbisBackend constructor with dispatch and error validation

Add _SCHEME_TO_DIALECT map, three-way dispatch (settings, URL, dialect),
empty-list normalization in connect(), and URL-direct via ibis.connect().
Error cases tested: no args, both args, unknown scheme."
```

---

### Task 2: Settings path — SQLite and DuckDB

Wire and test the `SettingsParameters` input form.

**Files:**
- Modify: `tests/test_unit/backends/ibis/test_backend.py`

- [ ] **Step 1: Write failing tests for settings path**

Add to `tests/test_unit/backends/ibis/test_backend.py`:

```python
from mountainash_data.backends.ibis.backend import IbisBackend, IbisConnection


def test_settings_path_sqlite():
    """Construct IbisBackend from SQLite SettingsParameters and connect."""
    from mountainash_settings import SettingsParameters
    from mountainash_data.core.settings import SQLiteAuthSettings, NoAuth

    params = SettingsParameters.create(
        settings_class=SQLiteAuthSettings,
        DATABASE=":memory:",
        auth=NoAuth(),
    )
    backend = IbisBackend(params)
    assert backend.dialect == "sqlite"
    conn = backend.connect()
    assert isinstance(conn, IbisConnection)
    tables = conn.list_tables()
    assert isinstance(tables, list)
    conn.close()


def test_settings_path_duckdb_empty_extensions():
    """DuckDB settings with default EXTENSIONS=[] must not crash ibis."""
    from mountainash_settings import SettingsParameters
    from mountainash_data.core.settings import DuckDBAuthSettings, NoAuth

    params = SettingsParameters.create(
        settings_class=DuckDBAuthSettings,
        DATABASE=":memory:",
        auth=NoAuth(),
    )
    backend = IbisBackend(params)
    assert backend.dialect == "duckdb"
    conn = backend.connect()  # Must not raise — empty-list filter active
    assert isinstance(conn, IbisConnection)
    conn.close()
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend.py::test_settings_path_sqlite tests/test_unit/backends/ibis/test_backend.py::test_settings_path_duckdb_empty_extensions -v`

Expected: PASS — the constructor's `_init_from_settings` path was implemented in Task 1.

If the DuckDB test fails with an error about `extensions=[]`, verify that `connect()` properly filters empty lists before calling the builder.

- [ ] **Step 3: Commit**

```bash
git add tests/test_unit/backends/ibis/test_backend.py
git commit -m "test(backend): settings path — SQLite and DuckDB SettingsParameters

Verifies constructor resolves SettingsParameters via descriptor.ibis_dialect
and to_driver_kwargs(). DuckDB test confirms empty-list normalization
filters extensions=[] before reaching ibis."
```

---

### Task 3: URL path — SQLite and DuckDB

Wire and test the connection URL input form.

**Files:**
- Modify: `tests/test_unit/backends/ibis/test_backend.py`

- [ ] **Step 1: Write failing tests for URL path**

Add to `tests/test_unit/backends/ibis/test_backend.py`:

```python
def test_url_path_sqlite():
    """Construct IbisBackend from sqlite:// URL and connect."""
    backend = IbisBackend("sqlite://")
    assert backend.dialect == "sqlite"
    conn = backend.connect()
    assert isinstance(conn, IbisConnection)
    conn.close()


def test_url_path_duckdb():
    """Construct IbisBackend from duckdb:// URL and connect."""
    backend = IbisBackend("duckdb://")
    assert backend.dialect == "duckdb"
    conn = backend.connect()
    assert isinstance(conn, IbisConnection)
    conn.close()


def test_url_path_preserves_database(tmp_path):
    """URL database component must reach the driver, not be discarded."""
    db_file = tmp_path / "test.db"
    backend = IbisBackend(f"sqlite:///{db_file}")
    assert backend.dialect == "sqlite"
    conn = backend.connect()
    assert isinstance(conn, IbisConnection)
    conn.close()
    assert db_file.exists()
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend.py::test_url_path_sqlite tests/test_unit/backends/ibis/test_backend.py::test_url_path_duckdb tests/test_unit/backends/ibis/test_backend.py::test_url_path_preserves_database -v`

Expected: PASS — the URL path calls `ibis.connect(url)` directly, which handles all URL forms natively.

The `test_url_path_preserves_database` test is the critical regression test identified by Codex review: it proves the URL's database path actually reaches the driver (the file must exist on disk after connect).

- [ ] **Step 3: Commit**

```bash
git add tests/test_unit/backends/ibis/test_backend.py
git commit -m "test(backend): URL path — sqlite://, duckdb://, file path preservation

Verifies URL dispatch via _SCHEME_TO_DIALECT, ibis.connect(url) delegation.
Critical regression: sqlite:///path creates the file on disk, proving
URL components are not discarded."
```

---

### Task 4: Full test suite and mark old docs abandoned

Run the entire test suite, then commit the abandoned old spec/plan.

**Files:**
- Modify: `docs/superpowers/specs/2026-04-26-to-relation-design.md` (already marked)
- Modify: `docs/superpowers/plans/2026-04-27-settings-aware-backends-to-relation.md` (already marked)

- [ ] **Step 1: Run full test suite**

Run: `hatch run test:test-quick`

Expected: All tests PASS (441+ existing + 8 new). No regressions.

If any existing tests fail, investigate — the existing `test_ibis_backend_satisfies_protocol`, `test_unknown_dialect_raises`, `test_all_registered_dialects_construct`, and `test_in_memory_sqlite_connect_and_inspect` should all still pass because they use `dialect=` keyword.

- [ ] **Step 2: Verify old spec and plan are marked abandoned**

Check that these files already have ABANDONED status (done earlier in the brainstorming session):
- `docs/superpowers/specs/2026-04-26-to-relation-design.md` — line 4 should say `ABANDONED`
- `docs/superpowers/plans/2026-04-27-settings-aware-backends-to-relation.md` — line 3 should say `ABANDONED`

- [ ] **Step 3: Commit abandoned docs**

```bash
git add docs/superpowers/specs/2026-04-26-to-relation-design.md docs/superpowers/plans/2026-04-27-settings-aware-backends-to-relation.md docs/superpowers/specs/2026-04-27-settings-aware-ibis-backend-design.md docs/superpowers/plans/2026-04-27-settings-aware-ibis-backend.md
git commit -m "chore(specs): abandon old to-relation spec/plan, add new settings-aware spec/plan

Old spec bundled to_relation() which has been descoped per revised
principle. New spec focuses solely on settings-aware IbisBackend
constructor (Phase 1 of connection consolidation)."
```

- [ ] **Step 4: Run full test suite one final time**

Run: `hatch run test:test-quick`

Expected: All tests PASS. Clean working tree (no uncommitted changes to source).
