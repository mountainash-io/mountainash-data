"""Tests for IbisBackend factory."""

import pytest
import polars as pl

from mountainash_data.backends.ibis.backend import IbisBackend
from mountainash_data.backends.ibis.dialects._registry import DIALECTS
from mountainash_data.core.protocol import Backend
from mountainash_data.core.namespace import Namespace


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
    backend.connect()
    try:
        assert backend.list_tables() == []
    finally:
        backend.close()


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


# ---------------------------------------------------------------------------
# Settings path
# ---------------------------------------------------------------------------

def test_settings_path_sqlite():
    """Construct IbisBackend from SQLite SettingsParameters and connect."""
    from mountainash_auth_client import NoAuthProfile
    from mountainash_settings import SettingsParameters
    from mountainash_data.core.settings import SQLiteBackendProfile

    params = SettingsParameters.create(
        settings_class=SQLiteBackendProfile,
        DATABASE=":memory:",
    )
    backend = IbisBackend(params)
    assert backend.dialect == "sqlite"
    backend.connect(auth_profile=NoAuthProfile())
    tables = backend.list_tables()
    assert isinstance(tables, list)
    backend.close()


def test_settings_path_sqlite_with_auth_profile():
    """Settings path threads auth through build_driver_kwargs via connect(auth_profile=...)."""
    from mountainash_auth_client import NoAuthProfile
    from mountainash_settings import SettingsParameters
    from mountainash_data.core.settings import SQLiteBackendProfile

    params = SettingsParameters.create(
        settings_class=SQLiteBackendProfile,
        DATABASE=":memory:",
    )
    backend = IbisBackend(params)
    assert backend.dialect == "sqlite"
    # auth_profile=None normalises to NoAuth, which is supported for sqlite
    result = backend.connect(auth_profile=NoAuthProfile())
    assert result is backend
    assert isinstance(backend.list_tables(), list)
    backend.close()


def test_settings_path_duckdb_empty_extensions():
    """DuckDB settings with default EXTENSIONS=[] must not crash ibis."""
    from mountainash_auth_client import NoAuthProfile
    from mountainash_settings import SettingsParameters
    from mountainash_data.core.settings import DuckDBBackendProfile

    params = SettingsParameters.create(
        settings_class=DuckDBBackendProfile,
        DATABASE=":memory:",
    )
    backend = IbisBackend(params)
    assert backend.dialect == "duckdb"
    backend.connect(auth_profile=NoAuthProfile())
    backend.close()


# ---------------------------------------------------------------------------
# URL path
# ---------------------------------------------------------------------------

def test_url_path_sqlite():
    """Construct IbisBackend from sqlite:// URL and connect."""
    backend = IbisBackend("sqlite://")
    assert backend.dialect == "sqlite"
    backend.connect()
    backend.close()


def test_url_path_duckdb():
    """Construct IbisBackend from duckdb:// URL and connect."""
    backend = IbisBackend("duckdb://")
    assert backend.dialect == "duckdb"
    backend.connect()
    backend.close()


def test_url_path_preserves_database(tmp_path):
    """URL database component must reach the driver, not be discarded."""
    db_file = tmp_path / "test.db"
    backend = IbisBackend(f"sqlite:///{db_file}")
    assert backend.dialect == "sqlite"
    backend.connect()
    backend.close()
    assert db_file.exists()


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def test_connect_returns_self():
    """connect() must return the backend instance itself."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    result = backend.connect()
    assert result is backend


def test_close_returns_self():
    """close() must return the backend instance itself."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    backend.connect()
    result = backend.close()
    assert result is backend


def test_context_manager():
    """with IbisBackend(...) as backend: must connect and close."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        assert backend.list_tables() == []
    # After exit, should be closed
    with pytest.raises(RuntimeError, match="not connected"):
        backend.list_tables()


def test_double_close_is_idempotent():
    """Calling close() twice must not raise."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    backend.connect()
    backend.close()
    backend.close()  # Must not raise


def test_use_before_connect_raises():
    """Calling methods before connect() must raise RuntimeError."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    with pytest.raises(RuntimeError, match="not connected"):
        backend.list_tables()


def test_use_after_close_raises():
    """Calling methods after close() must raise RuntimeError."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    backend.connect()
    backend.close()
    with pytest.raises(RuntimeError, match="not connected"):
        backend.list_tables()


def test_ibis_connection_accessor():
    """ibis_connection() returns the raw ibis backend object."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        raw = backend.ibis_connection()
        assert hasattr(raw, "list_tables")


def test_ibis_connection_before_connect_raises():
    """ibis_connection() before connect() must raise RuntimeError."""
    backend = IbisBackend(dialect="sqlite", database=":memory:")
    with pytest.raises(RuntimeError, match="not connected"):
        backend.ibis_connection()


def test_get_connection_accessor():
    """get_connection() returns our IbisConnection wrapper."""
    from mountainash_data.backends.ibis.backend import IbisConnection
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        conn = backend.get_connection()
        assert isinstance(conn, IbisConnection)


# ---------------------------------------------------------------------------
# DialectSpec hooks
# ---------------------------------------------------------------------------

def test_duckdb_dialect_routes_generic_upsert():
    """DuckDB DialectSpec has no hook (retired) but has upsert_style=ON_CONFLICT for generic path."""
    from mountainash_data.backends.ibis.dialects._registry import UpsertStyle
    spec = DIALECTS["duckdb"]
    assert spec.upsert_hook is None
    assert spec.upsert_style == UpsertStyle.ON_CONFLICT


def test_sqlite_dialect_uses_generic_index_path():
    """After cutover, sqlite has no index hooks and dispatches via index_caps."""
    spec = DIALECTS["sqlite"]
    assert spec.create_index_hook is None
    assert spec.drop_index_hook is None
    assert spec.index_caps is not None


def test_duckdb_family_index_hooks_removed():
    import mountainash_data.backends.ibis.operations as ops
    assert not hasattr(ops, "duckdb_family_create_index")
    assert not hasattr(ops, "duckdb_family_drop_index")


def test_no_dialect_carries_an_index_hook_post_cutover():
    """The generic path is the ONLY index path after cutover: no dialect carries
    a create/drop index hook, so the backend's hook-first branch (which forwards
    the new `where=` predicate) is never exercised — keeping it dead and safe.
    The hook fields remain only as a future override escape hatch; CONTRACT: any
    future create_index_hook MUST accept create_index's keyword signature,
    including `where` (the ibis predicate), and any drop_index_hook MUST accept
    `table_name`/`database`/`if_exists`."""
    for name, spec in DIALECTS.items():
        assert spec.create_index_hook is None, f"{name} unexpectedly has create_index_hook"
        assert spec.drop_index_hook is None, f"{name} unexpectedly has drop_index_hook"


def test_postgres_dialect_has_no_upsert_hook():
    """Postgres DialectSpec has no upsert_hook (not DuckDB family)."""
    spec = DIALECTS["postgres"]
    assert spec.upsert_hook is None


# ---------------------------------------------------------------------------
# Thin wrapper operations (fluent)
# ---------------------------------------------------------------------------

def test_create_table_returns_self():
    """create_table() must return self for fluent chaining."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        result = backend.create_table("t", {"id": [1, 2]})
        assert result is backend
        assert "t" in backend.list_tables()


def test_drop_table_returns_self():
    """drop_table() must return self."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1]})
        result = backend.drop_table("t")
        assert result is backend
        assert "t" not in backend.list_tables()


def test_insert_returns_self():
    """insert() must return self."""
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        backend.create_table("t", {"id": [1]})
        result = backend.insert("t", {"id": [2]})
        assert result is backend


def test_truncate_returns_self():
    """truncate() must return self."""
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        backend.create_table("t", {"id": [1]})
        result = backend.truncate("t")
        assert result is backend


def test_table_returns_ibis_table():
    """table() must return an ibis table expression."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1, 2]})
        tbl = backend.table("t")
        assert tbl is not None


def test_run_sql_returns_result():
    """run_sql() must return an ibis table expression."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1, 2, 3]})
        result = backend.run_sql("SELECT COUNT(*) as cnt FROM t")
        assert result is not None


def test_table_exists_returns_bool():
    """table_exists() must return True/False."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        assert backend.table_exists("nope") is False
        backend.create_table("t", {"id": [1]})
        assert backend.table_exists("t") is True


def test_table_exists_honors_database_namespace():
    """table_exists(database=...) must scope the check to that namespace (DEBT-9).

    A table living only in a non-default schema must be found via ``database=``
    and must NOT be found in the default namespace, and vice versa.
    """
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        raw = backend.ibis_connection()
        raw.raw_sql("CREATE SCHEMA tenant_a")
        raw.raw_sql("CREATE TABLE tenant_a.sleep (id INTEGER)")
        raw.raw_sql("CREATE TABLE main_only (id INTEGER)")

        # Table exists only in tenant_a.
        assert backend.table_exists("sleep", database="tenant_a") is True
        assert backend.table_exists("sleep") is False
        # Table exists only in the default namespace.
        assert backend.table_exists("main_only") is True
        assert backend.table_exists("main_only", database="tenant_a") is False


def test_table_exists_forwards_database_to_introspection(monkeypatch):
    """The ``database`` arg must reach the introspection call, not be dropped.

    Guards the swallowed-error path: ``IbisConnection.list_tables`` returns ``[]``
    on failure, so a test asserting only a bool return can pass on a version that
    never forwards ``database``. Assert the forwarding directly.
    """
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        seen: dict[str, str | None] = {}

        def fake_list_tables(namespace=None):
            seen["namespace"] = namespace
            return ["sleep"]

        monkeypatch.setattr(backend, "list_tables", fake_list_tables)
        assert backend.table_exists("sleep", database="tenant_a") is True
        assert seen == {"namespace": "tenant_a"}


def test_fluent_chaining():
    """Multiple fluent calls can be chained."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("a", {"id": [1]}).create_table("b", {"id": [2]})
        assert sorted(backend.list_tables()) == ["a", "b"]


# ---------------------------------------------------------------------------
# Hook-dispatched operations
# ---------------------------------------------------------------------------

def test_create_index_returns_self():
    """create_index() via hook must return self."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1], "name": ["a"]})
        result = backend.create_index("t", ["name"])
        assert result is backend


def test_create_unique_index_returns_self():
    """create_unique_index() must return self."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1], "name": ["a"]})
        result = backend.create_unique_index("t", ["name"])
        assert result is backend


def test_drop_index_returns_self():
    """drop_index() via hook must return self."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1], "name": ["a"]})
        backend.create_index("t", ["name"], index_name="idx_name")
        result = backend.drop_index("idx_name")
        assert result is backend


def test_index_exists():
    """index_exists() must detect created indexes."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1]})
        backend.create_index("t", ["id"], index_name="idx_id")
        assert backend.index_exists("idx_id") is True
        assert backend.index_exists("no_such_idx") is False


def test_list_indexes():
    """list_indexes() must return index info."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1], "name": ["a"]})
        backend.create_index("t", ["id"], index_name="idx_id")
        indexes = backend.list_indexes("t")
        assert isinstance(indexes, list)
        assert len(indexes) >= 1


def test_upsert_duckdb():
    """upsert() must work on DuckDB via hook."""
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        initial = pl.DataFrame({"id": [1, 2], "val": [10, 20]})
        backend.create_table("t", initial)
        backend.create_unique_index("t", ["id"])

        update = pl.DataFrame({"id": [2, 3], "val": [25, 30]})
        result = backend.upsert("t", update, conflict_columns=["id"])
        assert result is backend

        count_result = backend.run_sql("SELECT COUNT(*) as cnt FROM t")
        count = count_result.to_polars()["cnt"][0]
        assert count == 3


def test_upsert_unsupported_dialect_raises():
    """upsert() on a dialect with no upsert_style and no hook must raise NotImplementedError.

    clickhouse has neither upsert_style nor upsert_hook, so _generic_upsert receives
    style=None and raises NotImplementedError — the correct sentinel for unsupported dialects.
    postgres now has upsert_style=ON_CONFLICT so it routes through _generic_upsert successfully.
    """
    backend = IbisBackend(dialect="clickhouse")
    # Can't actually connect, so mock the connection state
    from mountainash_data.backends.ibis.backend import IbisConnection
    backend._conn = IbisConnection(None, DIALECTS["clickhouse"])
    with pytest.raises(NotImplementedError, match="does not support upsert"):
        backend.upsert("t", {}, conflict_columns=["id"])


# ---------------------------------------------------------------------------
# Namespace-carrying inspection + discovery (DEBT-10 Task 5)
# ---------------------------------------------------------------------------

def test_list_catalogs_degrades_to_current():
    """A catalog-less/simple backend still answers list_catalogs()."""
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        cats = backend.list_catalogs()
        assert isinstance(cats, list)
        assert len(cats) >= 1


def test_inspect_table_location_default_namespace():
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1]})
        info = backend.inspect_table("t")
        assert info.name == "t"
        assert info.location == Namespace()


def test_inspect_table_location_reflects_namespace():
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        raw = backend.ibis_connection()
        raw.raw_sql("CREATE SCHEMA tenant_a")
        raw.raw_sql("CREATE TABLE tenant_a.widgets (id INTEGER)")
        info = backend.inspect_table("widgets", namespace="tenant_a")
        assert info.location == Namespace(path=("tenant_a",))
        assert info.qualified_name == "tenant_a.widgets"


def test_list_namespaces_accepts_catalog_kwarg():
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        # catalog=None is the default; the kwarg must be accepted without error.
        assert isinstance(backend.list_namespaces(catalog=None), list)
