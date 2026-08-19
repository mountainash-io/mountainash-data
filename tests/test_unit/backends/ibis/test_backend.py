"""Tests for IbisBackend factory."""

import duckdb
import sqlite3
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


def test_transaction_none_dialect_required_false_noops_without_connection():
    # required=False must no-op even when the NONE backend is never connected
    import warnings
    be = IbisBackend(dialect="clickhouse")  # NONE support, not connected
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with be.transaction(required=False):
            pass  # must not raise RuntimeError("not connected")


def test_raw_driver_connection_duckdb_returns_native_handle():
    with IbisBackend(dialect="duckdb", database=":memory:") as be:
        raw = be.raw_driver_connection()
        assert isinstance(raw, duckdb.DuckDBPyConnection)
        # usable as a real handle
        assert raw.execute("SELECT 1").fetchone()[0] == 1


def test_raw_driver_connection_sqlite_returns_native_handle():
    with IbisBackend(dialect="sqlite", database=":memory:") as be:
        raw = be.raw_driver_connection()
        assert isinstance(raw, sqlite3.Connection)


def test_raw_driver_connection_requires_connected():
    be = IbisBackend(dialect="duckdb", database=":memory:")
    with pytest.raises(RuntimeError, match="not connected"):
        be.raw_driver_connection()


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


def test_table_exists_honors_namespace():
    """table_exists(namespace=...) scopes the check to that namespace (DEBT-9/10)."""
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        raw = backend.ibis_connection()
        raw.raw_sql("CREATE SCHEMA tenant_a")
        raw.raw_sql("CREATE TABLE tenant_a.sleep (id INTEGER)")
        raw.raw_sql("CREATE TABLE main_only (id INTEGER)")
        assert backend.table_exists("sleep", namespace="tenant_a") is True
        assert backend.table_exists("sleep") is False
        assert backend.table_exists("main_only") is True
        assert backend.table_exists("main_only", namespace="tenant_a") is False


def test_table_exists_forwards_namespace_to_introspection(monkeypatch):
    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        seen: dict[str, object] = {}

        def fake_list_tables(namespace=None):
            seen["namespace"] = namespace
            return ["sleep"]

        monkeypatch.setattr(backend, "list_tables", fake_list_tables)
        assert backend.table_exists("sleep", namespace="tenant_a") is True
        assert seen == {"namespace": "tenant_a"}


def test_database_keyword_rejected_on_table_exists():
    """Clean break: the old database= keyword no longer exists."""
    import pytest

    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        with pytest.raises(TypeError):
            backend.table_exists("t", database="x")


def test_create_and_drop_table_in_namespace():
    with IbisBackend(dialect="duckdb", database=":memory:") as backend:
        backend.ibis_connection().raw_sql("CREATE SCHEMA tenant_b")
        backend.create_table("gadgets", {"id": [1]}, namespace="tenant_b")
        assert backend.table_exists("gadgets", namespace="tenant_b") is True
        backend.drop_table("gadgets", namespace="tenant_b")
        assert backend.table_exists("gadgets", namespace="tenant_b") is False


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


def test_list_indexes_returns_typed_index_info():
    """list_indexes() returns typed physical index metadata."""
    from mountainash_data import IndexInfo

    with IbisBackend(dialect="sqlite", database=":memory:") as backend:
        backend.create_table("t", {"id": [1], "name": ["a"]})
        backend.create_index("t", ["id"], index_name="idx_id", unique=True)
        indexes = backend.list_indexes("t")
        assert isinstance(indexes, list)
        assert len(indexes) == 1
        row = indexes[0]
        assert isinstance(row, IndexInfo)
        assert row.name == "idx_id"
        assert row.columns == ("id",)
        assert row.definition is not None and "idx_id" in row.definition
        assert row.unique is True


def test_list_indexes_validates_before_custom_hook():
    from dataclasses import replace

    from mountainash_data import IndexInfo
    from mountainash_data.backends.ibis.dialects._registry import DIALECTS

    calls = []

    def hook(conn, table_name, namespace):
        calls.append((table_name, namespace))
        return [IndexInfo("ix", False, False, ("id",))]

    backend = IbisBackend(dialect="sqlite", database=":memory:")
    backend._spec = replace(
        DIALECTS["sqlite"],
        get_list_indexes_sql=None,
        list_indexes_hook=hook,
    )
    backend.connect()
    try:
        with pytest.raises(ValueError, match="simple identifier"):
            backend.list_indexes("bad; name")
        assert calls == []
        assert backend.list_indexes("t") == [
            IndexInfo("ix", False, False, ("id",))
        ]
        assert calls == [("t", None)]
    finally:
        backend.close()
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


# ---------------------------------------------------------------------------
# transaction() / supports_transactions (Gap 3 Task 3)
# ---------------------------------------------------------------------------

def test_supports_transactions_introspection():
    assert IbisBackend(dialect="duckdb", database=":memory:").supports_transactions is True
    assert IbisBackend(dialect="clickhouse").supports_transactions is False


def test_transaction_ibis_level_op_rolls_back(tmp_path):
    # the consumer's REAL shape: an ibis-level op (create_table) inside transaction()
    db = str(tmp_path / "t.db")
    with IbisBackend(dialect="duckdb", database=db) as be:
        import pandas as pd
        with pytest.raises(ValueError):
            with be.transaction():
                be.create_table("t", pd.DataFrame({"x": [1]}))
                raise ValueError("boom")
        assert "t" not in be.list_tables()   # rolled back


def test_transaction_commits(tmp_path):
    db = str(tmp_path / "t.db")
    with IbisBackend(dialect="duckdb", database=db) as be:
        raw = be.raw_driver_connection()
        raw.execute("CREATE TABLE t (x INT)")
        with be.transaction():
            raw.execute("INSERT INTO t VALUES (1)")
        assert raw.execute("SELECT count(*) FROM t").fetchone()[0] == 1


def test_transaction_rolls_back(tmp_path):
    db = str(tmp_path / "t.db")
    with IbisBackend(dialect="duckdb", database=db) as be:
        raw = be.raw_driver_connection()
        raw.execute("CREATE TABLE t (x INT)")
        with pytest.raises(ValueError):
            with be.transaction():
                raw.execute("INSERT INTO t VALUES (1)")
                raise ValueError("boom")
        assert raw.execute("SELECT count(*) FROM t").fetchone()[0] == 0


def test_transaction_nested_joins(tmp_path):
    db = str(tmp_path / "t.db")
    with IbisBackend(dialect="duckdb", database=db) as be:
        raw = be.raw_driver_connection()
        raw.execute("CREATE TABLE t (x INT)")
        # nested MUST NOT raise "cannot start a transaction within a transaction"
        with be.transaction():
            with be.transaction():
                raw.execute("INSERT INTO t VALUES (1)")
        assert raw.execute("SELECT count(*) FROM t").fetchone()[0] == 1


def test_in_transaction_false_outside_and_true_inside():
    with IbisBackend(dialect="duckdb", database=":memory:") as be:
        assert be.in_transaction() is False
        with be.transaction():
            assert be.in_transaction() is True
        assert be.in_transaction() is False


def test_in_transaction_true_at_nested_depth():
    with IbisBackend(dialect="duckdb", database=":memory:") as be:
        with be.transaction():
            with be.transaction():
                assert be.in_transaction() is True
            assert be.in_transaction() is True
        assert be.in_transaction() is False


def test_in_transaction_false_after_rollback():
    with IbisBackend(dialect="duckdb", database=":memory:") as be:
        with pytest.raises(ValueError):
            with be.transaction():
                raise ValueError("boom")
        assert be.in_transaction() is False


def test_in_transaction_true_while_poisoned_before_unwind():
    # Public-surface pin of spec §5.4: poisoned-but-open reads True.
    from mountainash_data.core.errors import TransactionPoisonedError
    with IbisBackend(dialect="duckdb", database=":memory:") as be:
        with pytest.raises(TransactionPoisonedError):
            with be.transaction():
                try:
                    with be.transaction():
                        raise ValueError("inner")
                except ValueError:
                    pass
                assert be.in_transaction() is True  # poisoned, still open
        assert be.in_transaction() is False


def test_in_transaction_none_dialect_returns_false():
    be = IbisBackend(dialect="clickhouse")  # TransactionSupport.NONE, not connected
    assert be.in_transaction() is False


def test_in_transaction_not_connected_returns_false():
    be = IbisBackend(dialect="duckdb", database=":memory:")  # never connect()ed
    assert be.in_transaction() is False


def test_in_transaction_after_close_returns_false():
    be = IbisBackend(dialect="duckdb", database=":memory:")
    be.connect()
    be.close()
    assert be.in_transaction() is False


def test_in_transaction_returns_false_when_handle_resolution_raises(monkeypatch):
    # A property-backed raw_handle_attr could raise a driver-specific (non-
    # RuntimeError) error on a dropped connection. The predicate must swallow
    # it and answer False, never propagate. monkeypatch replaces the bound
    # method with a zero-arg callable; in_transaction() calls it with no args.
    be = IbisBackend(dialect="duckdb", database=":memory:")
    be.connect()

    def _boom():
        raise OSError("driver connection dropped")

    monkeypatch.setattr(be, "raw_driver_connection", _boom)
    assert be.in_transaction() is False


@pytest.mark.parametrize("dialect", ["duckdb", "sqlite"])
def test_raw_driver_connection_identity_is_stable(dialect):
    # Load-bearing for cross-wrapper correctness (spec §3.4/§5.6): the handle
    # must be the SAME object on every call so two wrappers over one raw conn
    # compute one id. Pinned for every locally testable dialect.
    with IbisBackend(dialect=dialect, database=":memory:") as be:
        assert be.raw_driver_connection() is be.raw_driver_connection()
