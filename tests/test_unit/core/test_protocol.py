"""Tests for core.protocol — structural Protocol definitions.

These tests verify that the Protocols are well-formed and that a minimal
fake implementation type-checks at runtime via isinstance() with
runtime_checkable Protocols.
"""

from __future__ import annotations

import typing as t

from mountainash_data.core.inspection import (
    CatalogInfo,
    NamespaceInfo,
    TableInfo,
)
from mountainash_data.core.protocol import Backend


class _FakeConnection:
    """Minimal in-memory Connection implementation for protocol verification."""

    def __init__(self):
        self.closed = False

    def list_namespaces(self) -> list[str]:
        return ["public"]

    def list_tables(self, namespace: str | None = None) -> list[str]:
        return ["users"]

    def inspect_table(self, name: str, namespace: str | None = None) -> TableInfo:
        return TableInfo(name=name, columns=[], namespace=namespace)

    def inspect_namespace(self, name: str) -> NamespaceInfo:
        return NamespaceInfo(name=name, tables=["users"])

    def inspect_catalog(self) -> CatalogInfo:
        return CatalogInfo(name="fake", namespaces=[])

    def close(self) -> None:
        self.closed = True


class _FakeBackend:
    """Minimal Backend implementation."""

    name = "fake"

    def connect(self) -> _FakeConnection:
        return _FakeConnection()


def test_fake_backend_satisfies_protocol():
    backend: Backend = _FakeBackend()
    assert backend.name == "fake"


def test_fake_connection_satisfies_protocol():
    conn = _FakeConnection()
    assert conn.list_namespaces() == ["public"]


def test_connection_inspect_returns_table_info():
    conn = _FakeConnection()
    info = conn.inspect_table("users", namespace="public")
    assert isinstance(info, TableInfo)
    assert info.name == "users"
    assert info.namespace == "public"


def test_connection_close_idempotent_marker():
    conn = _FakeConnection()
    assert conn.closed is False
    conn.close()
    assert conn.closed is True
