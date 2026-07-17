"""IcebergBackend — implements core.protocol.Backend for iceberg catalogs."""

from __future__ import annotations

import contextlib
import typing as t

from mountainash_data.backends.iceberg.catalogs.rest import IcebergRestConnection
from mountainash_data.backends.iceberg.connection import IcebergConnectionBase
from mountainash_data.core.errors import TransactionUnsupportedError
from mountainash_data.core._warn import warn_once


_CATALOG_REGISTRY: dict[str, type[IcebergConnectionBase]] = {
    "rest": IcebergRestConnection,
}


class IcebergBackend:
    """Iceberg backend — single entry point for iceberg catalog interaction.

    Construction takes a catalog type (e.g. ``'rest'``) and config kwargs.
    ``connect()`` returns ``self``. Use as a context manager.
    """

    name = "iceberg"

    def __init__(self, catalog: str, **config: t.Any) -> None:
        if catalog not in _CATALOG_REGISTRY:
            raise KeyError(
                f"Unknown iceberg catalog type {catalog!r}. "
                f"Available: {sorted(_CATALOG_REGISTRY)}"
            )
        self._catalog_cls = _CATALOG_REGISTRY[catalog]
        self._config = config
        self._conn: IcebergConnectionBase | None = None

    def connect(self) -> IcebergBackend:
        """Open a connection. Returns self for fluent chaining."""
        if self._conn is None:
            self._conn = self._catalog_cls(**self._config)
        return self

    def close(self) -> IcebergBackend:
        """Release the connection. Idempotent. Returns self."""
        if self._conn is not None:
            if hasattr(self._conn, "close"):
                self._conn.close()
            self._conn = None
        return self

    def __enter__(self) -> IcebergBackend:
        self.connect()
        return self

    def __exit__(self, *args: t.Any) -> None:
        self.close()

    def _require_connected(self) -> IcebergConnectionBase:
        if self._conn is None:
            raise RuntimeError(
                "IcebergBackend is not connected. Call connect() first."
            )
        return self._conn

    def list_tables(self, namespace: t.Any = None) -> list[str]:
        return self._require_connected().list_tables(namespace=namespace)

    def list_namespaces(self, catalog: str | None = None) -> list[str]:
        return self._require_connected().list_namespaces(catalog=catalog)

    def list_catalogs(self) -> list[str]:
        return self._require_connected().list_catalogs()

    def inspect_table(self, name: str, namespace: t.Any = None) -> t.Any:
        return self._require_connected().inspect_table(name, namespace=namespace)

    def inspect_namespace(self, name: str) -> t.Any:
        return self._require_connected().inspect_namespace(name)

    def inspect_catalog(self, catalog: str | None = None) -> t.Any:
        return self._require_connected().inspect_catalog(catalog=catalog)

    def raw_driver_connection(self) -> t.Any:
        """Return the underlying pyiceberg Catalog (native handle)."""
        return self._require_connected().catalog_backend

    @property
    def supports_transactions(self) -> bool:
        return False

    @contextlib.contextmanager
    def transaction(self, *, required: bool = True):
        """Iceberg has no connection-level cross-table transaction; declines.

        required=True raises; required=False warns ONCE and no-ops. (pyiceberg
        offers table-scoped transactions — a future capability, not this one.)
        """
        if required:
            raise TransactionUnsupportedError(
                "iceberg has no connection-level transaction; use table-scoped "
                "pyiceberg transactions, or call transaction(required=False)."
            )
        warn_once("iceberg", "iceberg has no transaction support; transaction() is a no-op.")
        yield

    def in_transaction(self) -> bool:
        """Iceberg has no connection-level unit of work (supports_transactions
        is False); nothing can be active. pyiceberg table-scoped transactions
        are a separate future capability."""
        return False
