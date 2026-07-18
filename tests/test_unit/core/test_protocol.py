"""Tests for core.protocol — structural Protocol definitions."""

from __future__ import annotations

from mountainash_data.core.inspection import CatalogInfo, NamespaceInfo, TableInfo
from mountainash_data.core.namespace import Namespace, NamespaceLike
from mountainash_data.core.protocol import Backend


class _FakeConnection:
    def __init__(self):
        self.closed = False

    def list_namespaces(self, catalog: str | None = None) -> list[str]:
        return ["public"]

    def list_catalogs(self) -> list[str]:
        return ["main"]

    def list_tables(self, namespace: NamespaceLike = None) -> list[str]:
        return ["users"]

    def inspect_table(self, name: str, namespace: NamespaceLike = None) -> TableInfo:
        return TableInfo(name=name, columns=[], location=Namespace.coerce(namespace))

    def inspect_namespace(self, name: str) -> NamespaceInfo:
        return NamespaceInfo(location=Namespace(path=(name,)), tables=["users"])

    def inspect_catalog(self, catalog: str | None = None) -> CatalogInfo:
        return CatalogInfo(name=catalog or "fake", namespaces=[])

    def close(self) -> None:
        self.closed = True


class _FakeBackend:
    name = "fake"

    def connect(self) -> _FakeConnection:
        return _FakeConnection()


def test_fake_backend_satisfies_protocol():
    backend: Backend = _FakeBackend()
    assert backend.name == "fake"


def test_discovery_methods_present():
    conn = _FakeConnection()
    assert conn.list_catalogs() == ["main"]
    assert conn.list_namespaces() == ["public"]


def test_inspect_table_carries_location():
    conn = _FakeConnection()
    info = conn.inspect_table("users", namespace=("a", "b"))
    assert isinstance(info, TableInfo)
    assert info.location == Namespace(path=("a", "b"))


def test_connection_close_idempotent_marker():
    conn = _FakeConnection()
    conn.close()
    assert conn.closed is True


def test_protocol_declares_raw_driver_connection():
    assert hasattr(Backend, "raw_driver_connection")


def test_iceberg_backend_satisfies_widened_protocol():
    import pytest
    pytest.importorskip("pyiceberg")
    from mountainash_data.backends.iceberg.backend import IcebergBackend
    assert hasattr(IcebergBackend, "raw_driver_connection")


def test_protocol_declares_transaction():
    from mountainash_data.core.protocol import Backend
    assert hasattr(Backend, "transaction")


def test_backend_protocol_declares_in_transaction():
    assert hasattr(Backend, "in_transaction")


def test_ibis_backend_satisfies_protocol_including_in_transaction():
    from mountainash_data.backends.ibis.backend import IbisBackend
    be = IbisBackend(dialect="duckdb", database=":memory:")
    assert isinstance(be, Backend)  # runtime_checkable: presence of protocol methods
    assert callable(be.in_transaction)


class _StubBackend:
    """Minimal non-Ibis structural implementer of the Backend protocol.

    Standing guard that the protocol stays satisfiable by a second backend and
    does not silently collapse into 'whatever IbisBackend does'. Not shipped,
    not registered — test-only.
    """

    name = "stub"

    def connect(self):
        return self

    def close(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def list_tables(self, namespace=None):
        return []

    def list_namespaces(self, catalog=None):
        return []

    def list_catalogs(self):
        return []

    def inspect_table(self, name, namespace=None):
        return TableInfo(name=name, columns=[])

    def inspect_namespace(self, name):
        return NamespaceInfo(location=Namespace(), tables=[])

    def inspect_catalog(self, catalog=None):
        return CatalogInfo(name=catalog or "stub", namespaces=[])

    def raw_driver_connection(self):
        raise RuntimeError("stub has no driver handle")

    @property
    def supports_transactions(self):
        return False

    def transaction(self, *, required=True):
        raise RuntimeError("stub does not support transactions")

    def in_transaction(self):
        return False


def test_non_ibis_stub_satisfies_backend_protocol():
    stub = _StubBackend()
    assert isinstance(stub, Backend)
    # catalog tier — the seam Iceberg originally motivated, kept generic
    assert stub.list_catalogs() == []
    assert stub.inspect_catalog().name == "stub"
    assert stub.list_namespaces(catalog="anything") == []
    assert stub.in_transaction() is False
    assert stub.supports_transactions is False
