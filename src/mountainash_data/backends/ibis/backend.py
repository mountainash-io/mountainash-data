"""IbisBackend — implements core.protocol.Backend for ibis-supported backends.

This is the new-style entry point that bypasses the legacy settings-class path.
The IbisBackend takes a dialect name and raw config kwargs, builds the ibis
backend connection directly via the DialectSpec.connection_builder, and returns
an IbisConnection that satisfies core.protocol.Connection.
"""

from __future__ import annotations

import typing as t

from mountainash_data.backends.ibis.dialects._registry import DIALECTS, DialectSpec
from mountainash_data.core.protocol import Backend, Connection
from mountainash_data.core.inspection import (
    CatalogInfo,
    ColumnInfo,
    NamespaceInfo,
    TableInfo,
)


class IbisConnection:
    """A live ibis connection satisfying core.protocol.Connection.

    Wraps an ibis backend object and exposes the standard Connection interface.
    Constructed by IbisBackend.connect() — not intended to be instantiated directly.
    """

    def __init__(self, ibis_conn: t.Any, dialect_spec: DialectSpec) -> None:
        self._ibis_conn = ibis_conn
        self._dialect_spec = dialect_spec
        self._closed = False

    def list_namespaces(self) -> list[str]:
        """Return the names of all namespaces (schemas/databases) visible to this connection."""
        try:
            # ibis backends vary — some expose list_databases, some list_schemas
            if hasattr(self._ibis_conn, "list_databases"):
                return self._ibis_conn.list_databases()
            if hasattr(self._ibis_conn, "list_schemas"):
                return self._ibis_conn.list_schemas()
            return []
        except Exception as e:
            print(f"Error listing namespaces: {e}")
            return []

    def list_tables(self, namespace: str | None = None) -> list[str]:
        """Return the names of tables in the given namespace."""
        try:
            if namespace is not None:
                return self._ibis_conn.list_tables(database=namespace)
            return self._ibis_conn.list_tables()
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    def inspect_table(
        self, name: str, namespace: str | None = None
    ) -> TableInfo:
        """Return shared-model metadata for one table."""
        from mountainash_data.backends.ibis.inspect import table_to_info

        try:
            ibis_table = self._ibis_conn.table(name, database=namespace)
            return table_to_info(ibis_table, name=name, namespace=namespace)
        except Exception as e:
            raise ValueError(f"Could not inspect table {name!r}: {e}") from e

    def inspect_namespace(self, name: str) -> NamespaceInfo:
        """Return shared-model metadata for one namespace."""
        try:
            tables = self.list_tables(namespace=name)
            return NamespaceInfo(name=name, tables=tables)
        except Exception as e:
            raise ValueError(f"Could not inspect namespace {name!r}: {e}") from e

    def inspect_catalog(self) -> CatalogInfo:
        """Return shared-model metadata for the connection's catalog."""
        namespaces = self.list_namespaces()
        ns_infos = [
            NamespaceInfo(name=ns, tables=self.list_tables(namespace=ns))
            for ns in namespaces
        ]
        return CatalogInfo(
            name=self._dialect_spec.ibis_backend_name,
            namespaces=ns_infos,
        )

    def close(self) -> None:
        """Release the connection. Idempotent."""
        if not self._closed:
            try:
                if hasattr(self._ibis_conn, "disconnect"):
                    self._ibis_conn.disconnect()
            except Exception:
                pass
            finally:
                self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class IbisBackend:
    """Ibis backend factory.

    Construction takes a dialect name (e.g. 'postgres') and config.
    connect() returns a live connection that satisfies
    core.protocol.Connection.

    Usage:
        backend = IbisBackend(dialect="sqlite", database=":memory:")
        conn = backend.connect()
        try:
            tables = conn.list_tables()
        finally:
            conn.close()
    """

    name = "ibis"

    def __init__(self, dialect: str, **config: t.Any):
        if dialect not in DIALECTS:
            raise KeyError(
                f"Unknown ibis dialect {dialect!r}. "
                f"Available: {sorted(DIALECTS)}"
            )
        self.dialect = dialect
        self._spec: DialectSpec = DIALECTS[dialect]
        self._config = config

    def connect(self) -> IbisConnection:
        """Build and return a live ibis connection."""
        if self._spec.connection_builder is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} has no connection_builder configured"
            )
        ibis_conn = self._spec.connection_builder(**self._config)
        return IbisConnection(ibis_conn, self._spec)
