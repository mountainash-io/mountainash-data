"""REST catalog implementation for the Iceberg backend.

Merges the legacy pyiceberg_rest_connection.py and pyiceberg_rest_operations.py
into a single concrete connection class for the REST catalog.

NOTE on the ``_upsert`` override:
    The legacy REST files both contain an ``_upsert`` implementation that uses
    raw SQL cursor operations (DuckDB-style syntax). This appears to be a
    copy-paste artefact from a DuckDB connection — it is incorrect for an Iceberg
    REST catalog and would fail at runtime. It is preserved here verbatim to
    avoid silently changing behaviour, but is marked as broken. The base
    ``upsert()`` (which delegates to ``operations.upsert()``) is the correct
    path for REST catalogs. This override will be removed in Phase 6.

NOTE on ``_list_tables``:
    The legacy ``pyiceberg_rest_operations.py`` contained a classmethod version
    of ``_list_tables`` that referenced ``cls.catalog_backend`` — which is an
    instance property and cannot be accessed on the class. That implementation
    was broken. The correct instance-method implementation is provided here,
    delegating to ``self.catalog_backend.list_tables()``.
"""

from __future__ import annotations

import contextlib
import typing as t
import uuid

import ibis.expr.types.relations as ir
from pydantic_settings import BaseSettings
from pyiceberg.catalog import Catalog

from mountainash_settings import SettingsParameters
from mountainash_data.core.constants import CONST_DB_BACKEND
from mountainash_data.core.settings import PyIcebergRestAuthSettings
from mountainash_data.backends.iceberg.connection import IcebergConnectionBase


class IcebergRestConnection(IcebergConnectionBase):
    """Concrete Iceberg connection for a REST catalog endpoint."""

    def __init__(
        self,
        db_auth_settings_parameters: SettingsParameters,
        connection_mode: t.Optional[str] = None,
    ) -> None:
        self._catalog_backend: t.Optional[Catalog] = None
        self.supports_upsert = True
        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

    # ------------------------------------------------------------------
    # Abstract property implementations
    # ------------------------------------------------------------------

    @property
    def catalog_backend(self) -> t.Optional[Catalog]:
        return self._catalog_backend

    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.PYICEBERG

    @property
    def settings_class(self) -> t.Type[BaseSettings]:
        return PyIcebergRestAuthSettings

    # ------------------------------------------------------------------
    # List tables (instance-method version; fixes the broken classmethod
    # that existed in the legacy pyiceberg_rest_operations.py)
    # ------------------------------------------------------------------

    def _list_tables(
        self,
        namespace: str | None = None,
    ) -> t.List[str]:
        """Return table names within ``namespace`` from the REST catalog."""
        return (
            self.catalog_backend.list_tables(namespace=namespace)
            if self.catalog_backend is not None
            else []
        )

    # ------------------------------------------------------------------
    # Legacy _upsert override (preserved verbatim from the REST legacy files;
    # flagged as incorrect for REST catalogs — see module docstring).
    # ------------------------------------------------------------------

    def _upsert(
        self,
        table_name: str,
        df: ir.Table | t.Any,
        natural_key_columns: list[str] | str,
        data_columns: list[str] | str,
        database: str | None = None,
        schema: str | None = None,
    ) -> None:
        """BROKEN: legacy SQL-cursor upsert copied from DuckDB implementation.

        This method uses DuckDB cursor semantics (``catalog_backend.con.cursor``)
        which are not available on a pyiceberg RestCatalog. It is preserved here
        for behavioural parity with the legacy code only.

        Consumers should call ``upsert()`` (the base class method) instead,
        which delegates to ``operations.upsert()`` and uses native pyiceberg
        merge semantics.
        """
        if isinstance(natural_key_columns, str):
            natural_key_columns = [natural_key_columns]

        if len(natural_key_columns) == 0:
            raise ValueError("Natural Keys must be provided")

        if isinstance(data_columns, str):
            data_columns = [data_columns]

        if len(data_columns) == 0:
            raise ValueError("Data Columns must be provided")

        if not self.table_exists(table_name=table_name, database=database):
            raise ValueError(f"Target Upsert table '{table_name}' does not exist")

        table_suffix: str = str(uuid.uuid4()).replace("-", "")
        staging_table_name = f"temp_upsert_{table_suffix}"

        list_all_columns = natural_key_columns + data_columns
        sql_all_columns = ", ".join(list_all_columns) if list_all_columns else ""
        sql_natural_keys = ", ".join(natural_key_columns) if natural_key_columns else ""
        sql_value_fields = (
            ", ".join([f"{col} = excluded.{col}" for col in data_columns])
            if data_columns
            else ""
        )

        upsert_sql = (
            f"INSERT INTO {database}.{table_name}({sql_all_columns}) "
            f"SELECT {sql_all_columns} FROM {staging_table_name} "
            f"ON CONFLICT ({sql_natural_keys}) DO UPDATE SET {sql_value_fields}"
        )

        with contextlib.closing(self.catalog_backend.con.cursor()) as cur:
            cur.execute("BEGIN TRANSACTION;")
            cur.register(staging_table_name, df)
            cur.execute(f"{upsert_sql}")
            cur.unregister(staging_table_name)
            cur.execute("COMMIT;")

    # ------------------------------------------------------------------
    # Index helpers (legacy, REST-specific; rarely used)
    # ------------------------------------------------------------------

    def unique_index_exists(
        self,
        table_name: str,
        natural_key_columns: list[str],
        database: str | None = None,
    ) -> bool:
        """Check if a unique index exists (legacy DuckDB-style check)."""
        if not natural_key_columns:
            return

        index_name = self.create_unique_index_name(table_name, natural_key_columns)

        check_index_sql = f"""
        SELECT COUNT(*) as index_exists
        FROM pg_catalog.pg_indexes
        WHERE indexname = '{index_name}'
        AND tablename = '{table_name}'
        """

        index_exists = (
            self.run_sql_as_catalog_dataframe(check_index_sql)
            .get_column_as_list("index_exists")[0]
            > 0
        )

        return index_exists

    def create_unique_index(
        self,
        table_name: str,
        natural_key_columns: list[str],
        database: str | None = None,
    ) -> bool:
        """Create a unique index (legacy DuckDB-style operation)."""
        if isinstance(natural_key_columns, str):
            natural_key_columns = [natural_key_columns]

        if len(natural_key_columns) == 0:
            raise ValueError("Natural Keys must be provided")

        index_exists = self.unique_index_exists(
            table_name=table_name,
            natural_key_columns=natural_key_columns,
            database=database,
        )

        if not index_exists:
            qualified_table = table_name
            if database:
                qualified_table = f"{database}.{qualified_table}"

            index_name = self.create_unique_index_name(table_name, natural_key_columns)
            sql_natural_keys = ", ".join(natural_key_columns)
            create_index_sql = (
                f"CREATE UNIQUE INDEX {index_name} "
                f"ON {qualified_table} ({sql_natural_keys});"
            )

            with contextlib.closing(self.catalog_backend.con.cursor()) as cur:
                cur.execute(create_index_sql)

    def create_unique_index_name(
        self,
        table_name: str,
        natural_key_columns: list[str],
    ) -> str:
        """Generate a canonical index name from table and key columns."""
        natural_key_columns.sort()
        return f"idx_{table_name}_{'_'.join(natural_key_columns)}"
