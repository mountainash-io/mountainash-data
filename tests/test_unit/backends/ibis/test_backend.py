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
    from mountainash_settings import SettingsParameters
    from mountainash_data.core.settings import SQLiteAuthSettings, NoAuth

    params = SettingsParameters.create(
        settings_class=SQLiteAuthSettings,
        DATABASE=":memory:",
        auth=NoAuth(),
    )
    backend = IbisBackend(params)
    assert backend.dialect == "sqlite"
    backend.connect()
    tables = backend.list_tables()
    assert isinstance(tables, list)
    backend.close()


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
    backend.connect()
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
