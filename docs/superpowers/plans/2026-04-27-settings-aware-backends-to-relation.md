# Settings-Aware Backends + to_relation() Implementation Plan

> **Status:** ABANDONED -- superseded by `docs/superpowers/specs/2026-04-27-settings-aware-ibis-backend-design.md`. to_relation() descoped; constructor design revised.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `IbisBackend` accept `SettingsParameters` as an alternative constructor, add `to_relation()` to the Connection protocol and both Ibis connection paths, and declare `mountainash` as an optional dependency.

**Architecture:** `IbisBackend.__init__` detects whether it received a `str` (dialect) or `SettingsParameters` and resolves config accordingly. `to_relation()` is added to the `Connection` protocol and implemented on `IbisConnection` (new-style) and `BaseIbisConnection` (factory path), both using an import-guarded call to `mountainash.relations.relation()`. Iceberg gets a `NotImplementedError` stub.

**Tech Stack:** Python, ibis-framework, mountainash-settings, mountainash (optional)

**Spec:** `docs/superpowers/specs/2026-04-26-to-relation-design.md`

---

## File Map

| File | Action | What changes |
|------|--------|--------------|
| `src/mountainash_data/backends/ibis/backend.py` | Modify | Settings-aware `__init__` + `to_relation()` on `IbisConnection` |
| `src/mountainash_data/core/protocol.py` | Modify | Add `to_relation()` to `Connection` protocol |
| `src/mountainash_data/backends/ibis/connection.py` | Modify | Add `to_relation()` to `BaseIbisConnection` |
| `src/mountainash_data/backends/iceberg/connection.py` | Modify | Add `to_relation()` stub to `IcebergConnectionBase` |
| `pyproject.toml` | Modify | Add `relations` optional extra |
| `tests/test_unit/core/test_protocol.py` | Modify | Add `to_relation` to `_FakeConnection` |
| `tests/test_unit/backends/ibis/test_backend_settings.py` | Create | Tests for settings-aware `IbisBackend` |
| `tests/test_unit/backends/ibis/test_to_relation.py` | Create | Tests for `to_relation()` on both Ibis paths |

---

### Task 1: Settings-aware IbisBackend

**Files:**
- Modify: `src/mountainash_data/backends/ibis/backend.py:106-142`
- Create: `tests/test_unit/backends/ibis/test_backend_settings.py`

- [ ] **Step 1: Create test directory and write failing tests**

Create `tests/test_unit/backends/__init__.py` and `tests/test_unit/backends/ibis/__init__.py` if they don't exist, then create the test file:

```python
"""Tests for settings-aware IbisBackend constructor."""

import pytest
from mountainash_data.backends.ibis.backend import IbisBackend, IbisConnection
from mountainash_data.core.settings import SQLiteAuthSettings, DuckDBAuthSettings, NoAuth
from mountainash_settings import SettingsParameters


class TestIbisBackendFromSettings:
    """Test IbisBackend constructed from SettingsParameters."""

    def test_sqlite_settings_creates_backend(self):
        params = SettingsParameters.create(
            settings_class=SQLiteAuthSettings,
            kwargs={"DATABASE": ":memory:", "auth": NoAuth()},
        )
        backend = IbisBackend(params)
        assert backend.dialect == "sqlite"

    def test_duckdb_settings_creates_backend(self):
        params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:", "auth": NoAuth()},
        )
        backend = IbisBackend(params)
        assert backend.dialect == "duckdb"

    def test_settings_backend_connects(self):
        params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:", "auth": NoAuth()},
        )
        backend = IbisBackend(params)
        conn = backend.connect()
        try:
            assert isinstance(conn, IbisConnection)
            tables = conn.list_tables()
            assert isinstance(tables, list)
        finally:
            conn.close()

    def test_direct_dialect_still_works(self):
        backend = IbisBackend(dialect="duckdb", database=":memory:")
        conn = backend.connect()
        try:
            assert isinstance(conn, IbisConnection)
        finally:
            conn.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend_settings.py -v`
Expected: FAIL — `IbisBackend` does not accept `SettingsParameters`

- [ ] **Step 3: Implement settings-aware constructor**

In `src/mountainash_data/backends/ibis/backend.py`, replace the `IbisBackend` class (lines 106-142) with:

```python
class IbisBackend:
    """Ibis backend factory.

    Construction takes either a dialect name with config kwargs, or a
    SettingsParameters object that resolves both automatically.

    Usage:
        # Direct config
        backend = IbisBackend(dialect="sqlite", database=":memory:")

        # Settings-driven
        backend = IbisBackend(settings_params)

        conn = backend.connect()
        try:
            tables = conn.list_tables()
        finally:
            conn.close()
    """

    name = "ibis"

    def __init__(self, dialect: str | t.Any = "", **config: t.Any):
        from mountainash_settings import SettingsParameters

        if isinstance(dialect, SettingsParameters):
            settings_params = dialect
            settings = settings_params.settings_class.get_settings(settings_params)
            descriptor = settings.__descriptor__
            ibis_dialect = descriptor.ibis_dialect
            if ibis_dialect not in DIALECTS:
                raise KeyError(
                    f"Unknown ibis dialect {ibis_dialect!r}. "
                    f"Available: {sorted(DIALECTS)}"
                )
            self.dialect = ibis_dialect
            self._spec: DialectSpec = DIALECTS[ibis_dialect]
            driver_kwargs = settings.to_driver_kwargs()
            self._config = {
                k: v for k, v in driver_kwargs.items()
                if not (isinstance(v, (list, tuple)) and len(v) == 0)
            }
        else:
            if dialect not in DIALECTS:
                raise KeyError(
                    f"Unknown ibis dialect {dialect!r}. "
                    f"Available: {sorted(DIALECTS)}"
                )
            self.dialect = dialect
            self._spec = DIALECTS[dialect]
            self._config = config

    def connect(self) -> IbisConnection:
        """Build and return a live ibis connection."""
        if self._spec.connection_builder is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} has no connection_builder configured"
            )
        ibis_conn = self._spec.connection_builder(**self._config)
        return IbisConnection(ibis_conn, self._spec)
```

Note: The parameter is named `dialect` (not `dialect_or_settings`) to
preserve backward compatibility with `IbisBackend(dialect="sqlite")`.
Type detection via `isinstance` distinguishes `SettingsParameters` from a
string. The `SettingsParameters` import is inside `__init__` to avoid a
top-level dependency for the direct-config path.

Empty list/tuple values are filtered from settings-derived kwargs (e.g.
`extensions=[]` from DuckDB) because some ibis drivers reject them. This
matches the normalization in `BaseIbisConnection.connect_default()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_backend_settings.py -v`
Expected: PASS — all 4 tests

- [ ] **Step 5: Run full test suite**

Run: `hatch run test:test-quick`
Expected: All existing tests still pass

- [ ] **Step 6: Commit**

```bash
git add src/mountainash_data/backends/ibis/backend.py tests/test_unit/backends/ 
git commit -m "feat(backend): make IbisBackend settings-aware"
```

---

### Task 2: Add to_relation() to protocol and all implementations

**Files:**
- Modify: `src/mountainash_data/core/protocol.py:19-52`
- Modify: `src/mountainash_data/backends/ibis/backend.py:21-103` (IbisConnection)
- Modify: `src/mountainash_data/backends/ibis/connection.py:20-99` (BaseIbisConnection)
- Modify: `src/mountainash_data/backends/iceberg/connection.py:50-114` (IcebergConnectionBase)
- Modify: `tests/test_unit/core/test_protocol.py` (_FakeConnection)
- Create: `tests/test_unit/backends/ibis/test_to_relation.py`

- [ ] **Step 1: Add to_relation() to the Connection protocol**

In `src/mountainash_data/core/protocol.py`, add after `close()` (after line 52):

```python
    def to_relation(
        self, name: str, namespace: str | None = None
    ) -> t.Any:
        """Return a mountainash Relation for the named table.

        Requires the mountainash package. Raises ImportError if not installed,
        or NotImplementedError if the backend does not support it.
        """
        ...
```

- [ ] **Step 2: Update _FakeConnection in test_protocol.py**

In `tests/test_unit/core/test_protocol.py`, add to `_FakeConnection` (after `close`):

```python
    def to_relation(self, name: str, namespace: str | None = None) -> t.Any:
        return f"relation:{name}"
```

- [ ] **Step 3: Run protocol tests to verify they still pass**

Run: `hatch run test:test-target tests/test_unit/core/test_protocol.py -v`
Expected: PASS

- [ ] **Step 4: Add to_relation() to IbisConnection**

In `src/mountainash_data/backends/ibis/backend.py`, add to `IbisConnection`
(after `inspect_catalog`, before `close`):

```python
    def to_relation(
        self, name: str, namespace: str | None = None
    ) -> t.Any:
        """Return a mountainash Relation wrapping the named ibis table."""
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

- [ ] **Step 5: Add to_relation() to BaseIbisConnection**

In `src/mountainash_data/backends/ibis/connection.py`, add to
`BaseIbisConnection` (after `connect_default`, before `_connect` — after
line 137):

```python
    def to_relation(
        self, name: str, namespace: str | None = None
    ) -> t.Any:
        """Return a mountainash Relation wrapping the named ibis table."""
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

- [ ] **Step 6: Add to_relation() stub to IcebergConnectionBase**

In `src/mountainash_data/backends/iceberg/connection.py`, add to
`IcebergConnectionBase` (after `is_connected`, before the schema cache
section — after line 138):

```python
    def to_relation(
        self, name: str, namespace: str | None = None
    ) -> t.Any:
        """Not yet supported for Iceberg connections."""
        raise NotImplementedError(
            "to_relation() is not yet supported for Iceberg connections. "
            "Use table() to get the native pyiceberg Table object."
        )
```

- [ ] **Step 7: Write tests for to_relation()**

Create `tests/test_unit/backends/ibis/test_to_relation.py`:

```python
"""Tests for to_relation() on Ibis connection paths."""

import pytest
import typing as t
from unittest.mock import patch

from mountainash_data.backends.ibis.backend import IbisBackend, IbisConnection


class TestIbisConnectionToRelation:
    """Test to_relation() on the new-style IbisConnection path."""

    @pytest.fixture
    def conn(self):
        backend = IbisBackend(dialect="duckdb", database=":memory:")
        conn = backend.connect()
        yield conn
        conn.close()

    def test_to_relation_returns_relation(self, conn):
        try:
            from mountainash.relations import Relation
        except ImportError:
            pytest.skip("mountainash package not installed")

        conn._ibis_conn.raw_sql("CREATE TABLE test_tbl (id INTEGER, name VARCHAR)")
        result = conn.to_relation("test_tbl")
        assert isinstance(result, Relation)

    def test_to_relation_import_error(self, conn):
        conn._ibis_conn.raw_sql("CREATE TABLE test_tbl2 (id INTEGER)")
        with patch.dict("sys.modules", {"mountainash": None, "mountainash.relations": None}):
            with pytest.raises(ImportError, match="mountainash package is required"):
                conn.to_relation("test_tbl2")


class TestBaseIbisConnectionToRelation:
    """Test to_relation() on the settings/factory path."""

    @pytest.fixture
    def conn(self):
        from mountainash_data.core.settings import DuckDBAuthSettings, NoAuth
        from mountainash_settings import SettingsParameters
        from mountainash_data.core.factories import ConnectionFactory

        params = SettingsParameters.create(
            settings_class=DuckDBAuthSettings,
            kwargs={"DATABASE": ":memory:", "auth": NoAuth()},
        )
        connection = ConnectionFactory.get_connection(params)
        connection.connect()
        yield connection
        connection.disconnect()

    def test_to_relation_returns_relation(self, conn):
        try:
            from mountainash.relations import Relation
        except ImportError:
            pytest.skip("mountainash package not installed")

        conn.ibis_backend.raw_sql("CREATE TABLE test_tbl3 (id INTEGER, name VARCHAR)")
        result = conn.to_relation("test_tbl3")
        assert isinstance(result, Relation)


class TestIcebergToRelationStub:
    """Test that Iceberg raises NotImplementedError."""

    def test_raises_not_implemented(self):
        from mountainash_data.backends.iceberg.connection import IcebergConnectionBase
        with pytest.raises(NotImplementedError, match="not yet supported"):
            IcebergConnectionBase.to_relation(None, "some_table")
```

- [ ] **Step 8: Run tests**

Run: `hatch run test:test-target tests/test_unit/backends/ibis/test_to_relation.py -v`
Expected: Tests pass (or skip if mountainash not installed)

Run: `hatch run test:test-quick`
Expected: Full suite passes

- [ ] **Step 9: Commit**

```bash
git add src/mountainash_data/core/protocol.py \
       src/mountainash_data/backends/ibis/backend.py \
       src/mountainash_data/backends/ibis/connection.py \
       src/mountainash_data/backends/iceberg/connection.py \
       tests/test_unit/core/test_protocol.py \
       tests/test_unit/backends/ibis/test_to_relation.py
git commit -m "feat(protocol): add to_relation() to Connection protocol and Ibis implementations"
```

---

### Task 3: Add mountainash as optional dependency

**Files:**
- Modify: `pyproject.toml:51-68`

- [ ] **Step 1: Add relations extra to pyproject.toml**

In `pyproject.toml`, after the `trino` optional dependency line (line 68), add:

```toml
relations = ["mountainash"]
```

- [ ] **Step 2: Run tests**

Run: `hatch run test:test-quick`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "feat(deps): add mountainash as optional relations extra"
```

---

## Follow-up (not in this plan)

- **Phase 2:** Migrate `DatabaseUtils` consumers to `IbisBackend(settings_params)`
- **Phase 3:** Deprecate `ConnectionFactory`, `OperationsFactory`, 12 concrete subclasses
- **Dead code candidate:** After the legacy branch removal in PR #78,
  `BaseIbisConnection.ibis_connection_mode` and `connection_string_scheme`
  abstract properties are unused by `connect_default()` but still declared
  on all 12 subclasses. Track separately.

Tracked in: `mountainash-central/01.principles/mountainash-data/f.backlog/settings-aware-backends.md`
