"""A missing backend driver fails at the load point naming the mountainash-data extra."""

import dataclasses
import sys

import duckdb
import ibis
import pytest

from mountainash_data.backends.ibis.backend import IbisBackend


@pytest.fixture
def block_modules(monkeypatch):
    """Make modules unimportable for one test, the way an absent package is."""

    def block(*roots: str, ibis_backend: str) -> None:
        if ibis_backend in vars(ibis):
            monkeypatch.delattr(ibis, ibis_backend)
        prefixes = (*roots, f"ibis.backends.{ibis_backend}")
        for name in [m for m in sys.modules
                     if any(m == p or m.startswith(p + ".") for p in prefixes)]:
            monkeypatch.delitem(sys.modules, name)
        for root in roots:
            monkeypatch.setitem(sys.modules, root, None)

    return block


def _hint(dialect: str) -> str:
    return f"pip install 'mountainash-data[{dialect}]'"


def _failing_connect(exc: BaseException) -> IbisBackend:
    """A backend whose driver is installed (duckdb, in every test env) but whose
    connection attempt raises ``exc``."""

    def builder(**_):
        raise exc

    backend = IbisBackend(dialect="duckdb", database=":memory:")
    backend._spec = dataclasses.replace(backend._spec, connection_builder=builder)
    return backend


def test_construction_needs_no_driver(block_modules):
    block_modules("duckdb", ibis_backend="duckdb")
    backend = IbisBackend(dialect="duckdb")
    assert backend.dialect == "duckdb"


def test_dialect_connect_names_extra_and_keeps_cause(block_modules):
    block_modules("duckdb", ibis_backend="duckdb")
    with pytest.raises(ImportError) as info:
        IbisBackend(dialect="duckdb", database=":memory:").connect()
    assert _hint("duckdb") in str(info.value)
    original = info.value.__cause__
    assert isinstance(original, ModuleNotFoundError)
    assert original.name == "duckdb"
    assert str(original) in str(info.value)


def test_url_connect_names_alias_extra(block_modules):
    block_modules("duckdb", ibis_backend="duckdb")
    with pytest.raises(ImportError, match=r"mountainash-data\[motherduck\]"):
        IbisBackend("duckdb://md:my_db").connect()


def test_alias_dialect_names_its_own_extra(block_modules):
    block_modules("psycopg", ibis_backend="postgres")
    with pytest.raises(ImportError) as info:
        IbisBackend(dialect="redshift", host="h", database="d").connect()
    assert _hint("redshift") in str(info.value)
    assert _hint("postgres") not in str(info.value)


def test_raw_adoption_names_extra(block_modules):
    raw = duckdb.connect()
    try:
        block_modules("duckdb", ibis_backend="duckdb")
        with pytest.raises(ImportError) as info:
            IbisBackend.from_raw_connection(raw, dialect="duckdb")
        assert _hint("duckdb") in str(info.value)
        assert isinstance(info.value.__cause__, ModuleNotFoundError)
    finally:
        raw.close()


def test_native_library_failure_is_not_relabelled():
    """psycopg without libpq raises a plain ImportError: no extra fixes that."""
    native = ImportError("no pq wrapper available")
    with pytest.raises(ImportError) as info:
        _failing_connect(native).connect()
    assert info.value is native


def test_missing_module_while_connecting_is_not_relabelled():
    """A sub-dependency missing at connect time (e.g. Spark Connect's grpc under
    a plain pyspark install) is not fixed by the backend extra; keep it as is."""
    connect_dep = ImportError("Spark Connect requires grpcio")
    connect_dep.__cause__ = ModuleNotFoundError("No module named 'grpc'", name="grpc")
    with pytest.raises(ImportError) as info:
        _failing_connect(connect_dep).connect()
    assert info.value is connect_dep


def test_connection_errors_pass_through():
    refused = OSError("connection refused")
    with pytest.raises(OSError) as info:
        _failing_connect(refused).connect()
    assert info.value is refused
