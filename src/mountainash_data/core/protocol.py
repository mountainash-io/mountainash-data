"""Backend protocol.

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
from mountainash_data.core.namespace import NamespaceLike


@t.runtime_checkable
class Backend(t.Protocol):
    """The single handle for interacting with a backend service.

    Backends are constructed with config, connected via connect(),
    used for inspection and operations, then closed.
    """

    name: str

    def connect(self) -> t.Self: ...
    def close(self) -> t.Self: ...
    def __enter__(self) -> t.Self: ...
    def __exit__(self, *args: t.Any) -> None: ...

    def list_tables(self, namespace: NamespaceLike = None) -> list[str]: ...
    def list_namespaces(self, catalog: str | None = None) -> list[str]: ...
    def list_catalogs(self) -> list[str]: ...

    def inspect_table(
        self, name: str, namespace: NamespaceLike = None
    ) -> TableInfo: ...

    def inspect_namespace(self, name: str) -> NamespaceInfo: ...
    def inspect_catalog(self, catalog: str | None = None) -> CatalogInfo: ...

    def raw_driver_connection(self) -> t.Any:
        """Return the underlying native driver handle (escape hatch).

        For SQL backends this is a live PEP-249 / native connection
        (duckdb.DuckDBPyConnection, psycopg.Connection, sqlite3.Connection,
        ...) suitable for transactions, DDL, information_schema, and
        driver-specific idioms. The handle *kind* varies by backend (DBAPI
        connection / client object / session object) and is NOT guaranteed to
        be DBAPI-conformant — callers must not assume DBAPI semantics without
        first checking the concrete backend. Raises (never returns ``None`` as
        a sentinel) if not connected or the backend exposes no driver handle.
        """
        ...

    @property
    def supports_transactions(self) -> bool:
        """True if transaction() opens a real unit of work (transaction_support is not NONE)."""
        ...

    def transaction(self, *, required: bool = True) -> t.ContextManager[None]:
        """Reentrant unit of work. Outermost issues BEGIN, nested calls join,
        outermost COMMITs, any exception ROLLBACKs the whole unit. required=True
        raises TransactionUnsupportedError on a backend with no transaction
        concept; required=False warns once and runs as a no-op. Statements run
        through this backend/ibis participate only while the driver is autocommit
        (an adopted autocommit-off connection is refused with
        TransactionIntegrityError). See spec §5.1–5.3."""
        ...
