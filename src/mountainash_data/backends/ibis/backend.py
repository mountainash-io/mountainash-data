"""IbisBackend — implements core.protocol.Backend for ibis-supported backends.

This is the new-style entry point that bypasses the legacy settings-class path.
The IbisBackend takes a dialect name and raw config kwargs, builds the ibis
backend connection directly via the DialectSpec.connection_builder, and returns
an IbisConnection that satisfies core.protocol.Connection.
"""

from __future__ import annotations

import typing as t

from mountainash_data.backends.ibis.dialects._registry import DIALECTS, DialectSpec, TransactionSupport
from mountainash_data.backends.ibis._transaction import run_transaction, is_active
from mountainash_data.backends.ibis._sqlite_compat import ensure_sqlite_nat_adapter
from mountainash_data.backends.ibis._adoption import (
    apply_options, snapshot_options, restore_options,
)
from mountainash_data.backends.ibis.operations import (
    _generic_add_columns,
    _generic_rename_table,
    _generic_upsert,
    _validate_simple_identifier,
)
from mountainash_data.backends.ibis._index import (
    _generic_create_index,
    _generic_drop_index,
    _generic_index_exists,
)
from mountainash_data.backends.ibis._index_inspection import _generic_list_indexes
from mountainash_data.core.inspection import (
    CatalogInfo,
    IndexInfo,
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

    def __init__(
        self,
        ibis_conn: t.Any,
        dialect_spec: DialectSpec,
        *,
        owns_connection: bool = True,
    ) -> None:
        self._ibis_conn = ibis_conn
        self._dialect_spec = dialect_spec
        self._closed = False
        self._owns_connection = owns_connection

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
        ns_infos = []
        for ns in namespaces:
            location = Namespace(catalog=catalog, path=(ns,))
            ns_infos.append(
                NamespaceInfo(location=location, tables=self.list_tables(namespace=location))
            )
        return CatalogInfo(
            name=catalog or self._dialect_spec.ibis_backend_name,
            namespaces=ns_infos,
        )

    def close(self) -> None:
        """Release the connection. Idempotent.

        When the connection was adopted (owns_connection=False), the
        underlying ibis connection belongs to the caller and is left open;
        only this wrapper is marked closed.
        """
        if not self._closed:
            try:
                if self._owns_connection and hasattr(self._ibis_conn, "disconnect"):
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

    @classmethod
    def from_ibis_connection(
        cls,
        ibis_conn: t.Any,
        *,
        dialect: str,
        owns_connection: bool = False,
        apply_session_options: t.Optional[dict[str, t.Any]] = None,
    ) -> IbisBackend:
        """Adopt an existing live ibis connection.

        The adopted connection shares the caller's session and transaction
        state — e.g. ``ibis.duckdb.from_connection(raw_duckdb_conn)`` wraps
        the caller's own DuckDB connection, so ``BEGIN``/``COMMIT`` issued on
        the raw connection bracket this backend's writes too. By default the
        backend does NOT own the connection: ``close()`` releases the wrapper
        but leaves the underlying connection open for the caller.

        apply_session_options re-applies a caller-declared end-state for the
        session options ibis mutates on adoption (the ibis backend is already
        built, so the pre-adoption value cannot be snapshotted here — use
        from_raw_connection(preserve_session=True) for faithful restore).
        """
        backend = cls(dialect=dialect)
        ibis_conn = backend._adapt_ibis_connection(ibis_conn)
        backend._conn = IbisConnection(
            ibis_conn, backend._spec, owns_connection=owns_connection
        )
        if apply_session_options:
            apply_options(
                backend.raw_driver_connection(),
                backend._spec.adoption_mutations,
                apply_session_options,
            )
        return backend

    @classmethod
    def from_raw_connection(
        cls,
        raw_conn: t.Any,
        *,
        dialect: str,
        owns_connection: bool = False,
        preserve_session: bool = False,
    ) -> IbisBackend:
        """Adopt a *raw driver* connection (not an ibis backend).

        This constructor owns the raw->ibis adoption step, so when
        preserve_session=True it snapshots the session options ibis mutates on
        adoption BEFORE calling ibis's from_connection, then restores them —
        leaving the caller's session uncorrupted. preserve_session=False (the
        default) reproduces plain ibis adoption behaviour.
        """
        import importlib

        backend = cls(dialect=dialect)
        # Gate (fable finding 4): only verified dialects have a known-good raw
        # adoption path; others must use from_ibis_connection.
        if not backend._spec.raw_adoption_verified:
            raise NotImplementedError(
                f"raw adoption not yet verified for {dialect!r}; construct the ibis "
                f"connection yourself and use IbisBackend.from_ibis_connection(...)."
            )
        options = backend._spec.adoption_mutations
        snapshot = snapshot_options(raw_conn, options) if preserve_session else {}

        # ibis's from_connection runs _post_connect, which mutates the session
        # BEFORE returning. If adoption raises after that, the caller's session is
        # already stomped — restore in the finally so a failed adoption does not
        # leave the session corrupted (Codex review).
        ibis_backend_module = importlib.import_module(
            f"ibis.backends.{backend._spec.ibis_backend_name}"
        )
        try:
            ibis_conn = ibis_backend_module.Backend.from_connection(raw_conn)
        finally:
            if preserve_session and snapshot:
                restore_options(raw_conn, options, snapshot)

        ibis_conn = backend._adapt_ibis_connection(ibis_conn)
        backend._conn = IbisConnection(
            ibis_conn, backend._spec, owns_connection=owns_connection
        )
        return backend

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
        ibis_conn = self._adapt_ibis_connection(ibis_conn)
        self._conn = IbisConnection(ibis_conn, self._spec)
        return self

    def _adapt_ibis_connection(self, ibis_conn: t.Any) -> t.Any:
        adapter = self._spec.ibis_connection_adapter
        return adapter(ibis_conn) if adapter is not None else ibis_conn

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

    def raw_driver_connection(self) -> t.Any:
        """Return the underlying native driver handle (see Backend protocol).

        Reads the per-dialect ``raw_handle_attr`` off the ibis backend. Works
        for connections this backend opened AND adopted ones. Raises if not
        connected or the handle is absent.
        """
        conn = self._require_connected()
        attr = self._spec.raw_handle_attr
        handle = getattr(conn._ibis_conn, attr, None)
        if handle is None:
            raise RuntimeError(
                f"No native driver handle on the {self.dialect!r} ibis backend "
                f"(expected attribute {attr!r}); the connection may be closed."
            )
        return handle

    @property
    def supports_transactions(self) -> bool:
        return self._spec.transaction_support is not TransactionSupport.NONE

    def transaction(self, *, required: bool = True) -> t.ContextManager[None]:
        support = self._spec.transaction_support
        raw = self.raw_driver_connection() if support is not TransactionSupport.NONE else None
        return run_transaction(
            raw,
            support=support,
            begin_statement=self._spec.begin_statement,
            dialect=self.dialect,
            required=required,
            autocommit_probe=self._spec.autocommit_probe,
            in_transaction_probe=self._spec.in_transaction_probe,
            raw_execute_hook=self._spec.raw_execute_hook,
        )

    def in_transaction(self) -> bool:
        """True if a unit of work opened via transaction() is currently active
        on this backend's raw connection (any nesting depth).

        Runtime companion to the static supports_transactions flag. Total:
        returns False — never raises — for NONE dialects, a backend that was
        never connected or has been closed, and a connection whose native
        handle has gone away. A point-in-time snapshot, not a lock.
        """
        if self._spec.transaction_support is TransactionSupport.NONE:
            return False
        if self._conn is None:
            return False
        try:
            raw = self.raw_driver_connection()
        except Exception:
            # Unresolvable/absent native handle == no live unit of work.
            # raw_driver_connection() raises RuntimeError on an absent handle
            # attr; a property-backed handle could raise a driver-specific
            # error on a dropped connection. Either way the answer is "no
            # active tx"; a genuine fault surfaces on the caller's next op.
            return False
        return is_active(raw)

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
        namespace: NamespaceLike = None,
        temp: bool = False,
        overwrite: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        rendered = _render_ibis_database(Namespace.coerce(namespace))
        ensure_sqlite_nat_adapter()
        conn._ibis_conn.create_table(
            name, obj=obj, schema=schema, database=rendered,
            temp=temp, overwrite=overwrite,
        )
        return self

    def drop_table(
        self,
        name: str,
        *,
        namespace: NamespaceLike = None,
        force: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        rendered = _render_ibis_database(Namespace.coerce(namespace))
        conn._ibis_conn.drop_table(name, database=rendered, force=force)
        return self

    def create_view(
        self,
        name: str,
        obj: t.Any,
        *,
        namespace: NamespaceLike = None,
        overwrite: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        rendered = _render_ibis_database(Namespace.coerce(namespace))
        conn._ibis_conn.create_view(name, obj=obj, database=rendered, overwrite=overwrite)
        return self

    def drop_view(
        self,
        name: str,
        *,
        namespace: NamespaceLike = None,
        force: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        rendered = _render_ibis_database(Namespace.coerce(namespace))
        conn._ibis_conn.drop_view(name, database=rendered, force=force)
        return self

    def insert(
        self,
        name: str,
        obj: t.Any,
        *,
        namespace: NamespaceLike = None,
        overwrite: bool = False,
    ) -> IbisBackend:
        conn = self._require_connected()
        rendered = _render_ibis_database(Namespace.coerce(namespace))
        ensure_sqlite_nat_adapter()
        conn._ibis_conn.insert(name, obj=obj, database=rendered, overwrite=overwrite)
        return self

    def truncate(
        self,
        name: str,
        *,
        namespace: NamespaceLike = None,
        schema: str | None = None,
    ) -> IbisBackend:
        conn = self._require_connected()
        rendered = _render_ibis_database(Namespace.coerce(namespace))
        # ibis SQLBackend.truncate_table() accepts only table_name + database;
        # schema is not a standard kwarg at the SQLBackend level.
        kwargs: dict[str, t.Any] = {}
        if rendered is not None:
            kwargs["database"] = rendered
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

    def table(self, name: str, *, namespace: NamespaceLike = None) -> t.Any:
        conn = self._require_connected()
        rendered = _render_ibis_database(Namespace.coerce(namespace))
        return conn._ibis_conn.table(name, database=rendered)

    def table_exists(self, name: str, namespace: NamespaceLike = None) -> bool:
        # ibis exposes no native table_exists; scope the membership check to the
        # requested namespace by forwarding through list_tables (DEBT-9/10).
        # Pass the RAW namespace — list_tables coerces once (no double render).
        return name in self.list_tables(namespace=namespace)

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
        namespace: NamespaceLike = None,
        schema: str | None = None,
    ) -> IbisBackend:
        conn = self._require_connected()
        rendered = _render_ibis_namespace_single(Namespace.coerce(namespace), op="upsert")
        hook = self._spec.upsert_hook
        if hook is not None:
            hook(
                conn._ibis_conn, name, obj,
                conflict_columns=conflict_columns,
                update_columns=update_columns,
                conflict_action=conflict_action,
                update_condition=update_condition,
                namespace=rendered,
                schema=schema,
            )
        else:
            _generic_upsert(
                conn._ibis_conn, name, obj, style=self._spec.upsert_style,
                conflict_columns=conflict_columns,
                update_columns=update_columns,
                conflict_action=conflict_action,
                update_condition=update_condition,
                namespace=rendered,
                schema=schema,
            )
        return self

    def add_columns(
        self,
        name: str,
        source: t.Any,
        *,
        namespace: NamespaceLike = None,
    ) -> IbisBackend:
        """Additively evolve `name`: add columns present in `source` but
        missing from the table. `source` is a frame (types inferred) or a
        ``{column: dtype}`` mapping. Additive, idempotent, dialect-agnostic.
        """
        conn = self._require_connected()
        rendered = _render_ibis_namespace_single(Namespace.coerce(namespace), op="add_columns")
        hook = self._spec.add_columns_hook
        if hook is not None:
            hook(conn._ibis_conn, name, source, namespace=rendered)
        else:
            _generic_add_columns(
                conn._ibis_conn, name, source, namespace=rendered
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
        namespace: NamespaceLike = None,
        if_not_exists: bool = True,
    ) -> IbisBackend:
        conn = self._require_connected()
        rendered = _render_ibis_namespace_single(Namespace.coerce(namespace), op="create_index")
        hook = self._spec.create_index_hook
        if hook is not None:
            hook(
                conn._ibis_conn, table_name, columns,
                index_name=index_name, unique=unique, index_type=index_type,
                where=where, namespace=rendered, if_not_exists=if_not_exists,
            )
        elif self._spec.index_caps is not None:
            _generic_create_index(
                conn._ibis_conn, table_name, columns,
                index_name=index_name, unique=unique, index_type=index_type,
                where=where, namespace=rendered, if_not_exists=if_not_exists,
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
        namespace: NamespaceLike = None,
    ) -> IbisBackend:
        # Gate here so a catalog-qualified namespace raises naming THIS method,
        # not the create_index it delegates to (the delegated call re-renders,
        # but by then the namespace has passed and won't raise again).
        _render_ibis_namespace_single(Namespace.coerce(namespace), op="create_unique_index")
        return self.create_index(
            table_name, columns,
            index_name=index_name, unique=True, where=where, namespace=namespace,
        )

    def drop_index(
        self,
        index_name: str,
        *,
        table_name: str | None = None,
        namespace: NamespaceLike = None,
        if_exists: bool = True,
    ) -> IbisBackend:
        conn = self._require_connected()
        rendered = _render_ibis_namespace_single(Namespace.coerce(namespace), op="drop_index")
        hook = self._spec.drop_index_hook
        if hook is not None:
            hook(
                conn._ibis_conn, index_name,
                table_name=table_name, namespace=rendered, if_exists=if_exists,
            )
        elif self._spec.index_caps is not None:
            _generic_drop_index(
                conn._ibis_conn, index_name,
                table_name=table_name, namespace=rendered, if_exists=if_exists,
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
        namespace: NamespaceLike = None,
    ) -> bool:
        if self._spec.get_index_exists_sql is None:
            raise NotImplementedError(
                f"Dialect {self.dialect!r} does not support index_exists"
            )
        conn = self._require_connected()
        rendered = _render_ibis_namespace_single(Namespace.coerce(namespace), op="index_exists")
        return _generic_index_exists(
            conn._ibis_conn, index_name,
            table_name=table_name, namespace=rendered,
            exists_sql_fn=self._spec.get_index_exists_sql,
        )

    def list_indexes(
        self,
        table_name: str,
        *,
        namespace: NamespaceLike = None,
    ) -> list[IndexInfo]:
        conn = self._require_connected()
        rendered = _render_ibis_namespace_single(
            Namespace.coerce(namespace), op="list_indexes"
        )
        _validate_simple_identifier(table_name, kind="table_name")
        if rendered is not None:
            _validate_simple_identifier(rendered, kind="namespace")
        if self._spec.list_indexes_hook is not None:
            return self._spec.list_indexes_hook(
                conn._ibis_conn, table_name, rendered
            )
        if self._spec.index_caps is not None:
            list_sql = self._spec.get_list_indexes_sql
            if list_sql is None:
                raise RuntimeError("index capability lacks a list-index implementation")
            return _generic_list_indexes(
                conn._ibis_conn, table_name, rendered, list_sql
            )
        raise NotImplementedError(
            f"Dialect {self.dialect!r} does not support list_indexes"
        )
