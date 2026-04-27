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
    from mountainash_data.backends.ibis.backend import IbisConnection

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
    from mountainash_data.backends.ibis.backend import IbisConnection

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


# ---------------------------------------------------------------------------
# URL path
# ---------------------------------------------------------------------------

def test_url_path_sqlite():
    """Construct IbisBackend from sqlite:// URL and connect."""
    from mountainash_data.backends.ibis.backend import IbisConnection

    backend = IbisBackend("sqlite://")
    assert backend.dialect == "sqlite"
    conn = backend.connect()
    assert isinstance(conn, IbisConnection)
    conn.close()


def test_url_path_duckdb():
    """Construct IbisBackend from duckdb:// URL and connect."""
    from mountainash_data.backends.ibis.backend import IbisConnection

    backend = IbisBackend("duckdb://")
    assert backend.dialect == "duckdb"
    conn = backend.connect()
    assert isinstance(conn, IbisConnection)
    conn.close()


def test_url_path_preserves_database(tmp_path):
    """URL database component must reach the driver, not be discarded."""
    from mountainash_data.backends.ibis.backend import IbisConnection

    db_file = tmp_path / "test.db"
    backend = IbisBackend(f"sqlite:///{db_file}")
    assert backend.dialect == "sqlite"
    conn = backend.connect()
    assert isinstance(conn, IbisConnection)
    conn.close()
    assert db_file.exists()
