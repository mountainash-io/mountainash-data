"""Iceberg connection: catalog/namespace lifecycle and read-side inspection.

Created in Phase 3 by deduplicating and splitting the legacy
base_pyiceberg_connection.py and base_pyiceberg_operations.py.

This module provides ``IcebergConnectionBase``, the abstract base for all
Iceberg catalog connections. It handles:
- Lifecycle: open/close/reconnect
- Catalog and namespace accessors
- Table loading with retry (``table()`` / ``get_schema()``)
- Inspection: ``list_namespaces``, ``list_tables``, ``inspect_table``,
  ``inspect_namespace``, ``inspect_catalog``
- Schema caching
- Thin delegation wrappers to ``operations.py`` for all mutations

``to_relation()`` is intentionally NOT implemented — it requires
mountainash-expressions to gain an Iceberg adapter, which is a separate
work item tracked in the spec.
"""

from __future__ import annotations

import typing as t
from abc import abstractmethod
from time import sleep

from mountainash_data.core.connection import BaseDBConnection
from mountainash_data.core.constants import (
    CONST_DB_ABSTRACTION_LAYER,
    CONST_DB_PROVIDER_TYPE,
)
from mountainash_data.core.inspection import (
    CatalogInfo,
    NamespaceInfo,
    TableInfo,
)

from mountainash_dataframes import DataFrameUtils, SupportedDataFrames
from mountainash_dataframes.constants import CONST_DATAFRAME_FRAMEWORK
from mountainash_settings import SettingsParameters

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import SortOrder


class IcebergConnectionBase(BaseDBConnection):
    """Abstract base for Iceberg catalog connections.

    Concrete subclasses (e.g. ``IcebergRestConnection``) implement
    ``catalog_backend``, ``db_backend_name``, and ``settings_class``.
    """

    def __init__(
        self,
        db_auth_settings_parameters: SettingsParameters,
    ) -> None:
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)
        self._schema_cache: dict = {}

    # ------------------------------------------------------------------
    # Abstract properties (implemented by concrete subclasses)
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def catalog_backend(self) -> t.Optional[Catalog | t.Any]:
        """The live pyiceberg Catalog handle, or None if not yet connected."""
        ...

    # ------------------------------------------------------------------
    # BaseDBConnection abstract properties
    # ------------------------------------------------------------------

    @property
    def db_abstraction_layer(self) -> CONST_DB_ABSTRACTION_LAYER:
        return CONST_DB_ABSTRACTION_LAYER.PYICEBERG

    @property
    def provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        return CONST_DB_PROVIDER_TYPE.PYICEBERG_REST

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(
        self,
        connection_string: t.Optional[str] = None,
        connection_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
        **kwargs: t.Any,
    ) -> Catalog:
        """Ensure a catalog connection is open, returning the backend handle.

        If already connected, this is a no-op (idempotent).
        """
        if self.catalog_backend is None:
            self.connect_default(**kwargs)
        return self.catalog_backend

    def connect_default(self, **kwargs: t.Any) -> Catalog:
        """Connect using credentials from the configured settings class."""
        if self.catalog_backend is None:
            settings_class = self.db_auth_settings_parameters.settings_class
            if settings_class is None:
                raise ValueError("Settings class is required for the database connection")
            obj_settings = settings_class.get_settings(settings_parameters=self.db_auth_settings_parameters)
            connection_kwargs = obj_settings.to_driver_kwargs()
            self._catalog_backend: RestCatalog = RestCatalog(**connection_kwargs)
        return self.catalog_backend

    def _connect(
        self,
        connection_kwargs: t.Optional[t.Dict[str, str]],
    ) -> Catalog:
        """Low-level connect hook. Raise NotImplementedError by default."""
        raise NotImplementedError

    def close(self) -> None:
        """Release the connection. Idempotent."""
        self.disconnect()

    def disconnect(self) -> None:
        """Close the connection to the catalog."""
        if self.catalog_backend is not None:
            self._catalog_backend = None

    def is_connected(self) -> bool:
        """Return True if a live catalog handle exists."""
        return self.catalog_backend is not None

    # ------------------------------------------------------------------
    # Schema cache
    # ------------------------------------------------------------------

    def get_schema(
        self,
        table_name: str | t.Tuple[str, ...],
        refresh: bool = False,
    ) -> t.Optional[Schema]:
        """Return the Iceberg schema for ``table_name``, with caching.

        Args:
            table_name: Table identifier.
            refresh: Force a cache bypass when True.

        Returns:
            ``Schema`` object, or None if the table cannot be loaded.
        """
        if not refresh and table_name in self._schema_cache:
            return self._schema_cache[table_name]

        table_ref = self.table(table_name)
        if table_ref is None:
            return None

        try:
            schema = table_ref.schema()
            self._schema_cache[table_name] = schema
            return schema
        except Exception as e:
            print(f"Error getting schema for {table_name}: {e}")
            return None

    def clear_schema_cache(
        self,
        table_name: t.Optional[str | t.Tuple[str, ...]] = None,
    ) -> None:
        """Clear schema cache for a specific table or all tables.

        Args:
            table_name: If provided, clear only that entry. If None,
                clear the whole cache.
        """
        if table_name:
            if table_name in self._schema_cache:
                del self._schema_cache[table_name]
        else:
            self._schema_cache.clear()

    # ------------------------------------------------------------------
    # Table loading
    # ------------------------------------------------------------------

    def table(
        self,
        table_name: str | t.Tuple[str, ...],
        max_attempts: int = 3,
        retry_delay: float = 0.5,
    ) -> t.Optional[Table]:
        """Load a table reference with built-in retry logic.

        Args:
            table_name: Table identifier.
            max_attempts: Maximum number of load attempts.
            retry_delay: Seconds to wait between retries.

        Returns:
            ``Table`` reference, or None after all attempts fail.
        """
        self.connect()

        for attempt in range(max_attempts):
            table_ref = self.catalog_backend.load_table(table_name)

            if table_ref is not None:
                return table_ref

            if attempt < max_attempts - 1:
                print(
                    f"Table reference for {table_name} returned None "
                    f"(attempt {attempt + 1}/{max_attempts}), retrying..."
                )
                sleep(retry_delay)

        print(
            f"Failed to get table reference for {table_name} "
            f"after {max_attempts} attempts"
        )
        return None

    # Alias used by the protocol and some legacy callers
    def load_table(
        self,
        table_name: str | t.Tuple[str, ...],
    ) -> t.Optional[Table]:
        """Alias for ``table()`` with default retry settings."""
        return self.table(table_name)

    # ------------------------------------------------------------------
    # Inspection (satisfies core.protocol.Connection)
    # ------------------------------------------------------------------

    def list_namespaces(
        self,
        parent: t.Optional[str | t.Tuple[str, ...]] = None,
    ) -> t.Optional[list]:
        """Return all namespaces visible to this catalog connection.

        Args:
            parent: Optional parent namespace to list sub-namespaces of.

        Returns:
            List of namespace tuples/names, or None if not connected.
        """
        self.connect()
        return (
            self.catalog_backend.list_namespaces(parent)
            if self.catalog_backend is not None
            else None
        )

    def list_tables(
        self,
        namespace: str | t.Tuple[str, ...] | None = None,
    ) -> list[str]:
        """Return the names of tables in ``namespace``.

        Args:
            namespace: Namespace to list. Backend-specific subclasses
                implement ``_list_tables`` to provide the actual listing.

        Returns:
            List of table name strings.
        """
        self.connect()
        return self._list_tables(namespace=namespace)

    def _list_tables(
        self,
        namespace: str | None = None,
    ) -> list[str]:
        """Hook for subclasses to implement namespace-scoped table listing."""
        raise NotImplementedError

    def inspect_table(
        self,
        name: str,
        namespace: t.Optional[str] = None,
    ) -> TableInfo:
        """Return shared-model metadata for one table.

        Args:
            name: Simple table name.
            namespace: Namespace the table belongs to, if known.

        Returns:
            ``TableInfo`` populated from the live table schema.
        """
        from mountainash_data.backends.iceberg.inspect import table_to_info

        identifier = (namespace, name) if namespace else name
        iceberg_table = self.table(identifier)
        if iceberg_table is None:
            raise ValueError(f"Table not found: {identifier!r}")

        catalog_name = getattr(self.catalog_backend, "name", None)
        return table_to_info(
            iceberg_table,
            name=name,
            namespace=namespace,
            catalog=catalog_name,
        )

    def inspect_namespace(self, name: str) -> NamespaceInfo:
        """Return shared-model metadata for one namespace.

        Args:
            name: Namespace identifier.

        Returns:
            ``NamespaceInfo`` with table names discovered from the catalog.
        """
        from mountainash_data.backends.iceberg.inspect import namespace_to_info

        table_names = self._list_tables(namespace=name)
        catalog_name = getattr(self.catalog_backend, "name", None)
        return namespace_to_info(name, table_names, catalog=catalog_name)

    def inspect_catalog(self) -> CatalogInfo:
        """Return shared-model metadata for the connection's catalog.

        Returns:
            ``CatalogInfo`` containing all namespaces and their tables.
        """
        from mountainash_data.backends.iceberg.inspect import (
            catalog_to_info,
            namespace_to_info,
        )

        self.connect()
        raw_namespaces = self.catalog_backend.list_namespaces()
        catalog_name = getattr(self.catalog_backend, "name", "iceberg")

        namespace_infos = []
        for ns in raw_namespaces:
            ns_name = ns[0] if isinstance(ns, (tuple, list)) else str(ns)
            try:
                table_names = self._list_tables(namespace=ns_name)
            except NotImplementedError:
                table_names = []
            namespace_infos.append(
                namespace_to_info(ns_name, table_names, catalog=catalog_name)
            )

        return catalog_to_info(catalog_name, namespace_infos)

    # ------------------------------------------------------------------
    # Table and view existence checks
    # ------------------------------------------------------------------

    def table_exists(
        self,
        table_name: str | t.Tuple[str, ...] | None = None,
    ) -> bool:
        """Return True if the table exists in the catalog."""
        self.connect()
        return (
            self.catalog_backend.table_exists(table_name)
            if self.catalog_backend is not None
            else None
        )

    def view_exists(
        self,
        view_name: str | t.Tuple[str, ...] | None = None,
    ) -> bool:
        """Return True if the view exists in the catalog."""
        self.connect()
        return (
            self.catalog_backend.view_exists(view_name)
            if self.catalog_backend is not None
            else None
        )

    def rename_table(
        self,
        old_name: str,
        new_name: str,
    ) -> None:
        """Rename a table in the catalog."""
        self.connect()
        return (
            self._rename_table(old_name=old_name, new_name=new_name)
            if self.catalog_backend is not None
            else None
        )

    def _rename_table(self, old_name: str, new_name: str) -> None:
        """Hook for subclasses to implement table renaming."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # SQL passthrough (not supported)
    # ------------------------------------------------------------------

    def run_sql(
        self,
        query: str,
        schema: Schema | None = None,
        dialect: str | None = None,
    ) -> t.Optional[Table]:
        """Raise NotImplementedError — Iceberg does not support raw SQL."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # DataFrame accessors
    # ------------------------------------------------------------------

    def table_as_ibis_dataframe(
        self,
        table_name: str,
        tablename_prefix: t.Optional[str] = None,
    ) -> t.Optional[SupportedDataFrames]:
        """Return the table as an Ibis-compatible dataframe.

        Args:
            table_name: Simple table name.
            tablename_prefix: Optional prefix for Ibis table registration.

        Returns:
            Ibis-backed dataframe, or None if the table could not be loaded.
        """
        result: Table | None = self.table(table_name=table_name)
        return DataFrameUtils.to_ibis(result, tablename_prefix=tablename_prefix)

    def table_as_polars_dataframe(
        self,
        table_name: str,
        tablename_prefix: t.Optional[str] = None,
        dataframe_framework: t.Optional[str] = CONST_DATAFRAME_FRAMEWORK.POLARS,
    ) -> t.Optional[SupportedDataFrames]:
        """Return the table as a Polars dataframe.

        Args:
            table_name: Simple table name.
            tablename_prefix: Unused; kept for API symmetry with ibis accessor.
            dataframe_framework: Target framework; defaults to POLARS.

        Returns:
            Dataframe in the requested framework, or None if not loaded.
        """
        result: Table | None = self.table(table_name=table_name)
        if result is None:
            return None

        if dataframe_framework is None:
            dataframe_framework = CONST_DATAFRAME_FRAMEWORK.POLARS

        return DataFrameUtils.cast_dataframe(result, dataframe_framework=dataframe_framework)

    def table_as_native_dataframe(
        self,
        object_name: str,
        schema: str | None = None,
        database: str | None = None,
        dataframe_framework: t.Optional[str] = CONST_DATAFRAME_FRAMEWORK.POLARS,
    ) -> t.Optional[SupportedDataFrames]:
        """Return the table as a native dataframe in the specified framework."""
        return self.table_as_polars_dataframe(
            table_name=object_name,
            dataframe_framework=dataframe_framework,
        )

    # ------------------------------------------------------------------
    # Mutation delegation wrappers (delegate to operations.py)
    # All wrappers use a local import to avoid circular dependencies.
    # ------------------------------------------------------------------

    def create_table(
        self,
        table_name: str | t.Tuple[str, ...],
        schema: Schema,
        df: t.Optional[t.Any] = None,
        location: str | None = None,
        partition_spec: t.Optional[PartitionSpec] = None,
        sort_order: t.Optional[SortOrder] = None,
        overwrite: t.Optional[bool] = False,
    ) -> t.Optional[Table]:
        """Create an Iceberg table. Delegates to ``operations.create_table``."""
        from mountainash_data.backends.iceberg import operations

        return operations.create_table(
            self,
            table_name,
            schema,
            df=df,
            location=location,
            partition_spec=partition_spec,
            sort_order=sort_order,
            overwrite=overwrite,
        )

    def drop_table(
        self,
        table_name: str | t.Tuple[str, ...],
        purge: t.Optional[bool] = False,
    ) -> bool:
        """Drop an Iceberg table. Delegates to ``operations.drop_table``."""
        from mountainash_data.backends.iceberg import operations

        return operations.drop_table(self, table_name, purge=purge)

    def insert(
        self,
        table_name: str | t.Tuple[str, ...],
        df: t.Any,
        prevent_duplicates: t.Optional[bool] = False,
    ) -> bool:
        """Insert data. Delegates to ``operations.insert``."""
        from mountainash_data.backends.iceberg import operations

        return operations.insert(self, table_name, df, prevent_duplicates)

    def upsert(
        self,
        table_name: str | t.Tuple[str, ...],
        df: t.Any,
        natural_key_columns: list[str] | None = None,
        when_matched_update_all: bool = True,
        when_not_matched_insert_all: bool = True,
        case_sensitive: bool = True,
    ) -> None:
        """Upsert data. Delegates to ``operations.upsert``."""
        from mountainash_data.backends.iceberg import operations

        return operations.upsert(
            self,
            table_name,
            df,
            natural_key_columns=natural_key_columns,
            when_matched_update_all=when_matched_update_all,
            when_not_matched_insert_all=when_not_matched_insert_all,
            case_sensitive=case_sensitive,
        )

    def truncate(
        self,
        table_name: str | t.Tuple[str, ...],
    ) -> None:
        """Truncate a table. Delegates to ``operations.truncate``."""
        from mountainash_data.backends.iceberg import operations

        return operations.truncate(self, table_name)

    def create_view(
        self,
        view_name: str | t.Tuple[str, ...],
    ) -> None:
        """Create a view. Delegates to ``operations.create_view``."""
        from mountainash_data.backends.iceberg import operations

        return operations.create_view(self, view_name)

    def drop_view(
        self,
        view_name: str | t.Tuple[str, ...],
    ) -> bool:
        """Drop a view. Delegates to ``operations.drop_view``."""
        from mountainash_data.backends.iceberg import operations

        return operations.drop_view(self, view_name)

    # ------------------------------------------------------------------
    # Retry helper (exposed for subclass use)
    # ------------------------------------------------------------------

    def retry_operation(
        self,
        operation_func: t.Callable,
        max_attempts: int = 3,
    ) -> t.Any:
        """Retry a PyIceberg operation that might fail due to commit conflicts."""
        from mountainash_data.backends.iceberg import operations

        return operations.retry_operation(operation_func, max_attempts)
