"""IbisBackend — implements core.protocol.Backend for ibis-supported backends.

This is the new-style entry point that bypasses the legacy settings-class path.
The IbisBackend takes a dialect name and raw config kwargs, builds the ibis
backend connection directly via the DialectSpec.connection_builder, and returns
an IbisConnection that satisfies core.protocol.Connection.
"""

from __future__ import annotations

import typing as t

from mountainash_data.backends.ibis.dialects._registry import DIALECTS, DialectSpec
from mountainash_data.core.inspection import (
    CatalogInfo,
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


# ---------------------------------------------------------------------------
# Scheme → dialect reverse lookup (built once from the DIALECTS registry)
# ---------------------------------------------------------------------------
def _build_scheme_to_dialect() -> dict[str, str]:
    """Build a map from URL scheme (e.g. 'sqlite', 'postgres') to dialect name."""
    result: dict[str, str] = {}
    for dialect_name, spec in DIALECTS.items():
        # connection_string_scheme is e.g. "postgres://", "duckdb://md:"
        scheme = spec.connection_string_scheme.split("://")[0].lower()
        # First dialect wins — e.g. "postgres" maps to "postgres", not "redshift"
        if scheme not in result:
            result[scheme] = dialect_name
    # Common aliases
    result.setdefault("postgresql", result.get("postgres", "postgres"))
    return result


_SCHEME_TO_DIALECT: dict[str, str] = _build_scheme_to_dialect()


class IbisBackend:
    """Ibis backend — single entry point for all Ibis connections.

    Three input forms, all producing IbisConnection via connect():

        # Settings object (deployment, env-driven config)
        backend = IbisBackend(settings_params)

        # Connection URL (universal connection strings)
        backend = IbisBackend("postgresql://user:pass@host:5432/db")

        # Dialect keyword + kwargs (tests, scripts)
        backend = IbisBackend(dialect="sqlite", database=":memory:")
    """

    name = "ibis"

    def __init__(
        self,
        settings_or_connection_string: str | t.Any | None = None,
        /,
        *,
        dialect: str | None = None,
        **config: t.Any,
    ):
        if settings_or_connection_string is not None and dialect is not None:
            raise ValueError(
                "Cannot specify both a positional settings/URL argument "
                "and dialect= keyword"
            )

        if settings_or_connection_string is not None:
            self._init_from_positional(settings_or_connection_string, config)
        elif dialect is not None:
            self._init_from_dialect(dialect, config)
        else:
            raise ValueError(
                "Either a SettingsParameters/URL positional argument "
                "or a dialect= keyword is required"
            )

    def _init_from_positional(
        self, value: str | t.Any, config: dict[str, t.Any]
    ) -> None:
        # Lazy import — only pay for it on the settings/URL paths
        from mountainash_settings import SettingsParameters

        if isinstance(value, SettingsParameters):
            self._init_from_settings(value, config)
        elif isinstance(value, str):
            if "://" in value:
                self._init_from_url(value, config)
            else:
                # Plain string — treat as dialect name
                self._init_from_dialect(value, config)
        else:
            raise TypeError(
                f"Expected SettingsParameters or str, got {type(value).__name__}"
            )

    def _init_from_dialect(
        self, dialect_name: str, config: dict[str, t.Any]
    ) -> None:
        if dialect_name not in DIALECTS:
            raise KeyError(
                f"Unknown ibis dialect {dialect_name!r}. "
                f"Available: {sorted(DIALECTS)}"
            )
        self.dialect = dialect_name
        self._spec: DialectSpec = DIALECTS[dialect_name]
        self._url: str | None = None
        self._config = config

    def _init_from_url(
        self, url: str, config: dict[str, t.Any]
    ) -> None:
        from urllib.parse import urlparse

        scheme = urlparse(url).scheme.lower()

        # Special case: MotherDuck URLs are "duckdb://md:..."
        if scheme == "duckdb" and url.startswith("duckdb://md:"):
            resolved_dialect = "motherduck"
        else:
            resolved_dialect = _SCHEME_TO_DIALECT.get(scheme)

        if resolved_dialect is None:
            raise ValueError(
                f"Cannot detect ibis dialect from URL scheme: {scheme!r}"
            )

        self.dialect = resolved_dialect
        self._spec = DIALECTS[resolved_dialect]
        self._url = url
        self._config = config

    def _init_from_settings(
        self, settings_params: t.Any, config: dict[str, t.Any]
    ) -> None:
        obj_settings = settings_params.settings_class.get_settings(
            settings_parameters=settings_params
        )
        descriptor = getattr(obj_settings, "__descriptor__", None)
        if descriptor is None or getattr(descriptor, "ibis_dialect", None) is None:
            raise ValueError(
                f"Settings class {type(obj_settings).__name__} has no "
                f"ibis_dialect on its descriptor"
            )
        resolved_dialect = descriptor.ibis_dialect
        if resolved_dialect not in DIALECTS:
            raise KeyError(
                f"Unknown ibis dialect {resolved_dialect!r} from descriptor. "
                f"Available: {sorted(DIALECTS)}"
            )
        driver_kwargs = obj_settings.to_driver_kwargs()
        driver_kwargs.update(config)

        self.dialect = resolved_dialect
        self._spec = DIALECTS[resolved_dialect]
        self._url = None
        self._config = driver_kwargs

    def connect(self) -> IbisConnection:
        """Build and return a live ibis connection."""
        if self._spec.connection_builder is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} has no connection_builder configured"
            )
        if self._url is not None:
            # URL path: delegate directly to ibis.connect() which
            # natively handles all URL forms and preserves all URL
            # components (host, port, credentials, database, query params).
            import ibis
            ibis_conn = ibis.connect(self._url, **self._config)
        else:
            # Settings/dialect path: go through the dialect builder
            # with empty-list normalization (e.g. DuckDB extensions=[]).
            cleaned_config = {
                k: v for k, v in self._config.items()
                if not (isinstance(v, (list, tuple)) and len(v) == 0)
            }
            ibis_conn = self._spec.connection_builder(**cleaned_config)
        return IbisConnection(ibis_conn, self._spec)
