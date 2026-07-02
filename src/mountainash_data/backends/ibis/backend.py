"""IbisBackend — implements core.protocol.Backend for ibis-supported backends.

This is the new-style entry point that bypasses the legacy settings-class path.
The IbisBackend takes a dialect name and raw config kwargs, builds the ibis
backend connection directly via the DialectSpec.connection_builder, and returns
an IbisConnection that satisfies core.protocol.Connection.
"""

from __future__ import annotations

import typing as t

from mountainash_data.backends.ibis.dialects._registry import DIALECTS, DialectSpec
from mountainash_data.backends.ibis.operations import _generic_add_columns, _generic_rename_table, _generic_upsert
from mountainash_data.backends.ibis._index import (
    _generic_create_index,
    _generic_drop_index,
    _generic_index_exists,
)
from mountainash_data.core.inspection import (
    CatalogInfo,
    NamespaceInfo,
    TableInfo,
)
from mountainash_data.core.factories.connection_factory import (
    build_driver_kwargs,
    apply_auth_adapter,
    provider_for_dialect,
    provider_for_scheme,
)
from mountainash_data.core.namespace import Namespace, NamespaceLike
from mountainash_auth_client import PasswordAuthProfile


def _render_ibis_database(ns: Namespace) -> tuple[str, str] | str | None:
    """Render a coerced Namespace to ibis's native `database=` value (native ops).

    ibis models exactly `catalog -> database -> table`, so a namespace path
    deeper than one level is unrepresentable and raises at this boundary.
    """
    if len(ns.path) > 1:
        raise ValueError(
            f"ibis backends support a single namespace level; got path={ns.path!r}. "
            f"Use Namespace(catalog=..., path=(one_level,)) to target a catalog."
        )
    level = ns.path[0] if ns.path else None
    if ns.catalog is not None:
        if level is None:
            raise ValueError(
                "A catalog-qualified ibis namespace requires one path level."
            )
        return (ns.catalog, level)
    return level


def _render_ibis_namespace_single(ns: Namespace, *, op: str) -> str | None:
    """Render for the manual-SQL families (upsert/add_columns/index).

    These build engine-native SQL and feed scalar index-introspection literals,
    which cannot address a foreign catalog (postgres has no cross-database SQL;
    the index builders take one scalar namespace literal). Reject a
    catalog-qualified namespace here with a remedial ValueError (spec §8) rather
    than emit broken three-part SQL downstream.
    """
    if ns.catalog is not None:
        raise ValueError(
            f"{op} does not support catalog-qualified namespaces "
            f"(catalog={ns.catalog!r}): it builds engine-native SQL that cannot "
            f"address a foreign catalog. Use a native-delegating op, or omit the catalog."
        )
    if len(ns.path) > 1:
        raise ValueError(
            f"ibis backends support a single namespace level; got path={ns.path!r}."
        )
    return ns.path[0] if ns.path else None


class IbisConnection:
    """A live ibis connection satisfying core.protocol.Connection.

    Wraps an ibis backend object and exposes the standard Connection interface.
    Constructed by IbisBackend.connect() — not intended to be instantiated directly.
    """

    def __init__(self, ibis_conn: t.Any, dialect_spec: DialectSpec) -> None:
        self._ibis_conn = ibis_conn
        self._dialect_spec = dialect_spec
        self._closed = False

    def list_namespaces(self, catalog: str | None = None) -> list[str]:
        """Return the names of all namespaces (schemas/databases) visible to this connection."""
        try:
            # ibis backends vary — some expose list_databases, some list_schemas
            if hasattr(self._ibis_conn, "list_databases"):
                if catalog is not None:
                    return self._ibis_conn.list_databases(catalog=catalog)
                return self._ibis_conn.list_databases()
            if hasattr(self._ibis_conn, "list_schemas"):
                return self._ibis_conn.list_schemas()
            return []
        except Exception as e:
            print(f"Error listing namespaces: {e}")
            return []

    def list_catalogs(self) -> list[str]:
        """Return catalogs visible to this connection. Degrades, never raises.

        Not every ibis backend exposes catalogs (only the CanListCatalog mixin
        does). Fall back to the connection's current catalog, then — for
        backends with no catalog concept at all (e.g. sqlite) — to the
        dialect's own ibis backend name as a single-entry pseudo-catalog, so
        callers always get at least one entry back for a live connection.
        """
        try:
            if hasattr(self._ibis_conn, "list_catalogs"):
                return list(self._ibis_conn.list_catalogs())
        except Exception as e:
            print(f"Error listing catalogs: {e}")
        current = getattr(self._ibis_conn, "current_catalog", None)
        if current is not None:
            return [current]
        return [self._dialect_spec.ibis_backend_name]

    def list_tables(self, namespace: NamespaceLike = None) -> list[str]:
        """Return the names of tables in the given namespace."""
        rendered = _render_ibis_database(Namespace.coerce(namespace))
        try:
            if rendered is not None:
                return self._ibis_conn.list_tables(database=rendered)
            return self._ibis_conn.list_tables()
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    def inspect_table(self, name: str, namespace: NamespaceLike = None) -> TableInfo:
        """Return shared-model metadata for one table."""
        from mountainash_data.backends.ibis.inspect import table_to_info

        ns = Namespace.coerce(namespace)
        rendered = _render_ibis_database(ns)
        try:
            ibis_table = self._ibis_conn.table(name, database=rendered)
            return table_to_info(ibis_table, name=name, location=ns)
        except Exception as e:
            raise ValueError(f"Could not inspect table {name!r}: {e}") from e

    def inspect_namespace(self, name: str) -> NamespaceInfo:
        """Return shared-model metadata for one namespace."""
        try:
            tables = self.list_tables(namespace=name)
            return NamespaceInfo(location=Namespace(path=(name,)), tables=tables)
        except Exception as e:
            raise ValueError(f"Could not inspect namespace {name!r}: {e}") from e

    def inspect_catalog(self, catalog: str | None = None) -> CatalogInfo:
        """Return shared-model metadata for the connection's catalog."""
        namespaces = self.list_namespaces(catalog=catalog)
        ns_infos = [
            NamespaceInfo(location=Namespace(path=(ns,)), tables=self.list_tables(namespace=ns))
            for ns in namespaces
        ]
        return CatalogInfo(
            name=catalog or self._dialect_spec.ibis_backend_name,
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
        self._profile = None
        self._dialect_config = config
        self._config: dict[str, t.Any] | None = None
        self._conn: IbisConnection | None = None

    def _init_from_url(
        self, url: str, config: dict[str, t.Any]
    ) -> None:
        from urllib.parse import urlparse

        scheme = urlparse(url).scheme.lower()

        # Special case: MotherDuck URLs are "duckdb://md:..."
        resolved_dialect: str | None
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
        self._profile = None
        self._url_config = config
        self._config = None
        self._conn = None

    def _init_from_settings(
        self, settings_params: t.Any, config: dict[str, t.Any]
    ) -> None:
        obj_settings = settings_params.settings_class.get_settings(
            settings_parameters=settings_params
        )
        descriptor = getattr(obj_settings, "__spec__", None)
        if descriptor is None or getattr(descriptor, "ibis_dialect", None) is None:
            raise ValueError(
                f"Settings class {type(obj_settings).__name__} has no "
                f"ibis_dialect on its spec"
            )
        resolved_dialect = descriptor.ibis_dialect
        if resolved_dialect not in DIALECTS:
            raise KeyError(
                f"Unknown ibis dialect {resolved_dialect!r} from spec. "
                f"Available: {sorted(DIALECTS)}"
            )
        self.dialect = resolved_dialect
        self._spec = DIALECTS[resolved_dialect]
        self._url = None
        self._profile = obj_settings          # settings path
        self._extra_config = config           # caller **config overrides
        self._config = None
        self._conn = None

    def _require_connected(self) -> IbisConnection:
        if self._conn is None:
            raise RuntimeError(
                "IbisBackend is not connected. Call connect() first."
            )
        return self._conn

    def connect(self, auth_profile: t.Any = None) -> IbisBackend:
        """Build a live ibis connection, optionally applying an auth profile.

        auth_profile is L2 credential data (a *AuthProfile). It is composed onto
        the backend config here (L3) — config is built at connect time, not init.
        Returns self for fluent chaining.
        """
        if self._conn is not None:
            return self
        if self._spec.connection_builder is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} has no connection_builder configured"
            )
        if self._profile is not None:                       # settings path
            cfg = build_driver_kwargs(self._profile, auth_profile)
            cfg.update(self._extra_config)
            self._config = cfg
            ibis_conn = self._connect_via_builder()
        elif self._url is not None:                         # URL path
            config, clean_url = self._resolve_url_auth(self._url, auth_profile)
            config.update(self._url_config)                 # caller extras apply on top
            self._config = config
            import ibis
            ibis_conn = ibis.connect(clean_url, **self._config)
        else:                                               # direct-dialect path
            self._config = self._resolve_dialect_auth(auth_profile)
            ibis_conn = self._connect_via_builder()
        self._conn = IbisConnection(ibis_conn, self._spec)
        return self

    def _connect_via_builder(self) -> t.Any:
        # preserves the prior empty-list/tuple filtering before connection_builder
        assert self._config is not None  # connect() sets it before dispatch
        assert self._spec.connection_builder is not None  # guarded in connect()
        cleaned_config = {
            k: v for k, v in self._config.items()
            if not (isinstance(v, (list, tuple)) and len(v) == 0)
        }
        return self._spec.connection_builder(**cleaned_config)

    def _resolve_dialect_auth(self, auth_profile: t.Any) -> dict[str, t.Any]:
        base = dict(self._dialect_config)
        if auth_profile is None:
            return base
        provider = provider_for_dialect(self.dialect)
        return apply_auth_adapter(provider, base, auth_profile)

    def _resolve_url_auth(self, url: str, auth_profile: t.Any) -> tuple[dict[str, t.Any], str]:
        from urllib.parse import urlsplit, urlunsplit, unquote
        parts = urlsplit(url)
        has_url_creds = bool(parts.username)
        if has_url_creds and auth_profile is not None:
            raise ValueError(
                "both URL credentials and an explicit auth_profile given"
            )
        config: dict[str, t.Any] = {}
        clean = url
        if has_url_creds:
            netloc = parts.hostname or ""
            if parts.port:
                netloc += f":{parts.port}"
            clean = urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
            auth_profile = PasswordAuthProfile(
                USERNAME=unquote(parts.username or ""),
                PASSWORD=unquote(parts.password) if parts.password else "",
            )
        if auth_profile is not None:
            provider = provider_for_scheme(parts.scheme)
            config = apply_auth_adapter(provider, config, auth_profile)
        return config, clean

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

    def list_tables(self, namespace: NamespaceLike = None) -> list[str]:
        return self._require_connected().list_tables(namespace=namespace)

    def list_namespaces(self, catalog: str | None = None) -> list[str]:
        return self._require_connected().list_namespaces(catalog=catalog)

    def list_catalogs(self) -> list[str]:
        return self._require_connected().list_catalogs()

    def inspect_table(self, name: str, namespace: NamespaceLike = None) -> TableInfo:
        return self._require_connected().inspect_table(name, namespace=namespace)

    def inspect_namespace(self, name: str) -> NamespaceInfo:
        return self._require_connected().inspect_namespace(name)

    def inspect_catalog(self, catalog: str | None = None) -> CatalogInfo:
        return self._require_connected().inspect_catalog(catalog=catalog)

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
        conn = self._require_connected()
        hook = self._spec.rename_table_hook
        if hook is not None:
            hook(conn._ibis_conn, old_name, new_name)
        else:
            _generic_rename_table(conn._ibis_conn, old_name, new_name)
        return self

    # --- Terminal operations (return data) ---

    def table(self, name: str, *, database: str | None = None) -> t.Any:
        conn = self._require_connected()
        return conn._ibis_conn.table(name, database=database)

    def table_exists(
        self, name: str, database: str | None = None
    ) -> bool:
        # ibis exposes no native table_exists; scope the membership check to the
        # requested namespace by forwarding database= through list_tables (DEBT-9).
        return name in self.list_tables(namespace=database)

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
        update_condition: t.Any = None,  # ConditionPredicate | None
        database: str | None = None,
        schema: str | None = None,
    ) -> IbisBackend:
        conn = self._require_connected()
        hook = self._spec.upsert_hook
        if hook is not None:
            hook(
                conn._ibis_conn, name, obj,
                conflict_columns=conflict_columns,
                update_columns=update_columns,
                conflict_action=conflict_action,
                update_condition=update_condition,
                database=database,
                schema=schema,
            )
        else:
            _generic_upsert(
                conn._ibis_conn, name, obj, style=self._spec.upsert_style,
                conflict_columns=conflict_columns,
                update_columns=update_columns,
                conflict_action=conflict_action,
                update_condition=update_condition,
                database=database,
                schema=schema,
            )
        return self

    def add_columns(
        self,
        name: str,
        source: t.Any,
        *,
        database: str | None = None,
    ) -> IbisBackend:
        """Additively evolve `name`: add columns present in `source` but
        missing from the table. `source` is a frame (types inferred) or a
        ``{column: dtype}`` mapping. Additive, idempotent, dialect-agnostic.
        """
        conn = self._require_connected()
        hook = self._spec.add_columns_hook
        if hook is not None:
            hook(conn._ibis_conn, name, source, database=database)
        else:
            _generic_add_columns(
                conn._ibis_conn, name, source, database=database
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
        where: t.Any = None,  # IndexPredicate | None
        database: str | None = None,
        if_not_exists: bool = True,
    ) -> IbisBackend:
        conn = self._require_connected()
        hook = self._spec.create_index_hook
        if hook is not None:
            hook(
                conn._ibis_conn, table_name, columns,
                index_name=index_name, unique=unique, index_type=index_type,
                where=where, database=database, if_not_exists=if_not_exists,
            )
        elif self._spec.index_caps is not None:
            _generic_create_index(
                conn._ibis_conn, table_name, columns,
                index_name=index_name, unique=unique, index_type=index_type,
                where=where, database=database, if_not_exists=if_not_exists,
                caps=self._spec.index_caps,
                exists_sql_fn=self._spec.get_index_exists_sql,
            )
        else:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support create_index"
            )
        return self

    def create_unique_index(
        self,
        table_name: str,
        columns: list[str] | str,
        *,
        index_name: str | None = None,
        where: t.Any = None,  # IndexPredicate | None
        database: str | None = None,
    ) -> IbisBackend:
        return self.create_index(
            table_name, columns,
            index_name=index_name, unique=True, where=where, database=database,
        )

    def drop_index(
        self,
        index_name: str,
        *,
        table_name: str | None = None,
        database: str | None = None,
        if_exists: bool = True,
    ) -> IbisBackend:
        conn = self._require_connected()
        hook = self._spec.drop_index_hook
        if hook is not None:
            hook(
                conn._ibis_conn, index_name,
                table_name=table_name, database=database, if_exists=if_exists,
            )
        elif self._spec.index_caps is not None:
            _generic_drop_index(
                conn._ibis_conn, index_name,
                table_name=table_name, database=database, if_exists=if_exists,
                caps=self._spec.index_caps,
                exists_sql_fn=self._spec.get_index_exists_sql,
            )
        else:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support drop_index"
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
        return _generic_index_exists(
            conn._ibis_conn, index_name,
            table_name=table_name, database=database,
            exists_sql_fn=self._spec.get_index_exists_sql,
        )

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
