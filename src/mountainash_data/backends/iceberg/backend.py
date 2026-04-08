"""IcebergBackend — implements core.protocol.Backend for iceberg catalogs.

NOTE: ``to_relation()`` is intentionally NOT implemented on this backend.
It requires mountainash-expressions to gain an Iceberg adapter, which is
a separate work item. The gap is documented in the Phase 3 spec.
"""

from __future__ import annotations

import typing as t

from mountainash_data.backends.iceberg.catalogs.rest import IcebergRestConnection
from mountainash_data.backends.iceberg.connection import IcebergConnectionBase
from mountainash_data.core.protocol import Connection


_CATALOG_REGISTRY: dict[str, type[IcebergConnectionBase]] = {
    "rest": IcebergRestConnection,
}


class IcebergBackend:
    """Iceberg backend factory.

    Construction takes a catalog type (e.g. ``'rest'``) and config kwargs.
    ``connect()`` returns a live ``IcebergConnectionBase`` instance that
    satisfies ``core.protocol.Connection``.

    Args:
        catalog: One of the keys in ``_CATALOG_REGISTRY`` (currently
            only ``'rest'``).
        **config: Keyword arguments forwarded verbatim to the connection
            class constructor (minus the ``db_auth_settings_parameters``
            which must be provided separately when calling ``connect()``).

    Raises:
        KeyError: If ``catalog`` is not a known catalog type.

    Example::

        backend = IcebergBackend(catalog="rest", uri="http://localhost:8181")
        conn = backend.connect()
        tables = conn.list_tables()
        conn.close()
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

    def connect(self) -> Connection:
        """Open a connection. Caller is responsible for closing it.

        Note: The legacy IcebergConnectionBase requires a
        ``db_auth_settings_parameters`` argument. When ``_config`` does not
        include one, this will raise at the base class constructor level.
        This mirrors the legacy behaviour.
        """
        return self._catalog_cls(**self._config)
