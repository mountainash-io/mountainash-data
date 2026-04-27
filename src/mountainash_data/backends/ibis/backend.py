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
        self._conn: IbisConnection | None = None

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
        self._conn: IbisConnection | None = None

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
        self._conn: IbisConnection | None = None

    def _require_connected(self) -> IbisConnection:
        if self._conn is None:
            raise RuntimeError(
                "IbisBackend is not connected. Call connect() first."
            )
        return self._conn

    def connect(self) -> IbisBackend:
        """Build a live ibis connection. Returns self for fluent chaining."""
        if self._conn is not None:
            return self
        if self._spec.connection_builder is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} has no connection_builder configured"
            )
        if self._url is not None:
            import ibis
            ibis_conn = ibis.connect(self._url, **self._config)
        else:
            cleaned_config = {
                k: v for k, v in self._config.items()
                if not (isinstance(v, (list, tuple)) and len(v) == 0)
            }
            ibis_conn = self._spec.connection_builder(**cleaned_config)
        self._conn = IbisConnection(ibis_conn, self._spec)
        return self

    def close(self) -> IbisBackend:
        """Release the connection. Idempotent. Returns self."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        return self

    def __enter__(self) -> IbisBackend:
        self.connect()
        return self

    def __exit__(self, *args: t.Any) -> None:
        self.close()

    def ibis_connection(self) -> t.Any:
        """Return the raw ibis backend object."""
        return self._require_connected()._ibis_conn

    def get_connection(self) -> IbisConnection:
        """Return the internal IbisConnection wrapper."""
        return self._require_connected()

    # --- Inspection (terminal — delegates to IbisConnection) ---

    def list_tables(self, namespace: str | None = None) -> list[str]:
        return self._require_connected().list_tables(namespace=namespace)

    def list_namespaces(self) -> list[str]:
        return self._require_connected().list_namespaces()

    def inspect_table(
        self, name: str, namespace: str | None = None
    ) -> TableInfo:
        return self._require_connected().inspect_table(name, namespace=namespace)

    def inspect_namespace(self, name: str) -> NamespaceInfo:
        return self._require_connected().inspect_namespace(name)

    def inspect_catalog(self) -> CatalogInfo:
        return self._require_connected().inspect_catalog()

    # --- Thin wrapper operations (fluent — return self) ---

    def create_table(
        self,
        name: str,
        obj: t.Any,
        *,
        schema: t.Any | None = None,
        database: str | None = None,
        temp: bool = False,
        overwrite: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        conn._ibis_conn.create_table(
            name, obj=obj, schema=schema, database=database,
            temp=temp, overwrite=overwrite,
        )
        return self

    def drop_table(
        self,
        name: str,
        *,
        database: str | None = None,
        force: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        conn._ibis_conn.drop_table(name, database=database, force=force)
        return self

    def create_view(
        self,
        name: str,
        obj: t.Any,
        *,
        database: str | None = None,
        overwrite: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        conn._ibis_conn.create_view(name, obj=obj, database=database, overwrite=overwrite)
        return self

    def drop_view(
        self,
        name: str,
        *,
        database: str | None = None,
        force: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        conn._ibis_conn.drop_view(name, database=database, force=force)
        return self

    def insert(
        self,
        name: str,
        obj: t.Any,
        *,
        database: str | None = None,
        overwrite: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        conn._ibis_conn.insert(name, obj=obj, database=database, overwrite=overwrite)
        return self

    def truncate(
        self,
        name: str,
        *,
        database: str | None = None,
        schema: str | None = None,
    ) -> IbisBackend:
        conn = self._require_connected()
        # ibis SQLBackend.truncate_table() accepts only table_name + database;
        # schema is not a standard kwarg at the SQLBackend level.
        kwargs: dict[str, t.Any] = {}
        if database is not None:
            kwargs["database"] = database
        conn._ibis_conn.truncate_table(name, **kwargs)
        return self

    def rename_table(self, old_name: str, new_name: str) -> IbisBackend:
        if self._spec.rename_table_hook is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support rename_table"
            )
        conn = self._require_connected()
        self._spec.rename_table_hook(conn._ibis_conn, old_name, new_name)
        return self

    # --- Terminal operations (return data) ---

    def table(self, name: str, *, database: str | None = None) -> t.Any:
        conn = self._require_connected()
        return conn._ibis_conn.table(name, database=database)

    def table_exists(
        self, name: str, database: str | None = None
    ) -> bool:
        tables = self.list_tables()
        return name in tables

    def run_sql(
        self,
        query: str,
        *,
        schema: t.Any | None = None,
        dialect: str | None = None,
    ) -> t.Any:
        conn = self._require_connected()
        return conn._ibis_conn.sql(query, schema=schema, dialect=dialect)

    def run_expr(
        self,
        expr: t.Any,
        *,
        params: dict | None = None,
        limit: str | None = "default",
        **kwargs: t.Any,
    ) -> t.Any:
        conn = self._require_connected()
        return conn._ibis_conn.execute(expr, params=params, limit=limit, **kwargs)

    def to_sql(
        self,
        expr: t.Any,
        *,
        params: t.Any = None,
        limit: str | None = None,
        pretty: bool = False,
        **kwargs: t.Any,
    ) -> str | None:
        conn = self._require_connected()
        return conn._ibis_conn.compile(expr, params=params, limit=limit, pretty=pretty, **kwargs)

    # --- Hook-dispatched operations (fluent — return self) ---

    def upsert(
        self,
        name: str,
        obj: t.Any,
        *,
        conflict_columns: list[str] | str,
        update_columns: list[str] | str | None = None,
        conflict_action: str = "UPDATE",
        update_condition: str | None = None,
        database: str | None = None,
        schema: str | None = None,
    ) -> IbisBackend:
        if self._spec.upsert_hook is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support upsert"
            )
        conn = self._require_connected()
        self._spec.upsert_hook(
            conn._ibis_conn, name, obj,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
            conflict_action=conflict_action,
            update_condition=update_condition,
            database=database,
            schema=schema,
        )
        return self

    def create_index(
        self,
        table_name: str,
        columns: list[str] | str,
        *,
        index_name: str | None = None,
        unique: bool = False,
        index_type: str | None = None,
        where_condition: str | None = None,
        database: str | None = None,
        if_not_exists: bool = True,
    ) -> IbisBackend:
        if self._spec.create_index_hook is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support create_index"
            )
        conn = self._require_connected()
        self._spec.create_index_hook(
            conn._ibis_conn, table_name, columns,
            index_name=index_name, unique=unique, index_type=index_type,
            where_condition=where_condition, database=database,
            if_not_exists=if_not_exists,
        )
        return self

    def create_unique_index(
        self,
        table_name: str,
        columns: list[str] | str,
        *,
        index_name: str | None = None,
        where_condition: str | None = None,
        database: str | None = None,
    ) -> IbisBackend:
        return self.create_index(
            table_name, columns,
            index_name=index_name, unique=True,
            where_condition=where_condition, database=database,
        )

    def drop_index(
        self,
        index_name: str,
        *,
        table_name: str | None = None,
        database: str | None = None,
        if_exists: bool = True,
    ) -> IbisBackend:
        if self._spec.drop_index_hook is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support drop_index"
            )
        conn = self._require_connected()
        self._spec.drop_index_hook(
            conn._ibis_conn, index_name,
            table_name=table_name, database=database, if_exists=if_exists,
        )
        return self

    def index_exists(
        self,
        index_name: str,
        *,
        table_name: str | None = None,
        database: str | None = None,
    ) -> bool:
        if self._spec.get_index_exists_sql is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support index_exists"
            )
        conn = self._require_connected()
        check_sql = self._spec.get_index_exists_sql(index_name, table_name, database)
        result = conn._ibis_conn.sql(check_sql)
        if result is None:
            return False
        import mountainash as ma
        count = ma.relation(result).to_dict()["count"][0]
        return count > 0

    def list_indexes(
        self,
        table_name: str,
        *,
        database: str | None = None,
    ) -> list[dict]:
        if self._spec.get_list_indexes_sql is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support list_indexes"
            )
        conn = self._require_connected()
        list_sql = self._spec.get_list_indexes_sql(table_name, database)
        result = conn._ibis_conn.sql(list_sql)
        if result is None:
            return []
        import mountainash as ma
        return ma.relation(result).to_dicts()
