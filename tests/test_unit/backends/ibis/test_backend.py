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
