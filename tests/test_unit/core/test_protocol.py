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
