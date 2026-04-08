"""Backend and Connection protocols.

This is the structural contract every backend implementation must
satisfy. Implementations are plain classes — there is no inheritance.
"""

from __future__ import annotations

import typing as t

from mountainash_data.core.inspection import (
    CatalogInfo,
    NamespaceInfo,
    TableInfo,
)


@t.runtime_checkable
class Connection(t.Protocol):
    """A live, owned connection to a backend.

    Connections are obtained by calling Backend.connect(). They expose
    physical introspection and lifecycle methods. Logical query
    construction is the job of mountainash-expressions, reached via
    to_relation() on backends that support it.
    """

    def list_namespaces(self) -> list[str]:
        """Return the names of all namespaces (schemas) visible to this connection."""
        ...

    def list_tables(self, namespace: str | None = None) -> list[str]:
        """Return the names of tables in the given namespace."""
        ...

    def inspect_table(
        self, name: str, namespace: str | None = None
    ) -> TableInfo:
        """Return shared-model metadata for one table."""
        ...

    def inspect_namespace(self, name: str) -> NamespaceInfo:
        """Return shared-model metadata for one namespace."""
        ...

    def inspect_catalog(self) -> CatalogInfo:
        """Return shared-model metadata for the connection's catalog."""
        ...

    def close(self) -> None:
        """Release the connection. Idempotent."""
        ...


@t.runtime_checkable
class Backend(t.Protocol):
    """A factory for Connections to a particular backend service.

    Backends are constructed with config and are stateless from the
    consumer's perspective. State lives on the Connection returned by
    connect().
    """

    name: str

    def connect(self) -> Connection:
        """Open a connection. Caller is responsible for closing it."""
        ...
