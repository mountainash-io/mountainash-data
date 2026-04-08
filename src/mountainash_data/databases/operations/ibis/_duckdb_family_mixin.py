"""
Shared operations mixin for DuckDB-family databases.

DuckDB, MotherDuck, and SQLite share the same SQL syntax for:
- INSERT ON CONFLICT (upserts)
- CREATE INDEX
- Partial indexes (WHERE clause)
"""

import typing as t
import contextlib
import warnings
import uuid

import ibis.expr.types.relations as ir
from ibis.backends.sql import SQLBackend

import mountainash as ma

from ...constants import CONST_CONFLICT_ACTION, CONST_INDEX_TYPE
from ._base_ibis_mixin import _BaseIbisMixin

class _DuckDBFamilyOperationsMixin(_BaseIbisMixin):
    """
    Shared operations for DuckDB-family databases.

    This mixin provides common implementation for databases that share
    DuckDB's SQL syntax: DuckDB, MotherDuck, and SQLite.
    """

    # ===========================
    # INDEX MANAGEMENT
    # ===========================

    @classmethod
    def create_index(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        columns: list[str] | str,
        *,
        index_name: str | None = None,
        unique: bool = False,
        index_type: str | None = None,
        where_condition: str | None = None,
        database: str | None = None,
        if_not_exists: bool = True
    ) -> bool:
        """
        Create an index on specified columns.

        Note: DuckDB/SQLite don't support explicit index type specification.
        BTREE is the default and implicit type.
        """
        columns_list = cls._normalize_columns(columns)

        if index_name is None:
            index_name = cls._generate_index_name(table_name, columns_list, unique=unique)

        qualified_table = cls._format_qualified_table(table_name, database=database)
        columns_sql = ", ".join(columns_list)

        unique_sql = "UNIQUE " if unique else ""
        if_not_exists_sql = "IF NOT EXISTS " if if_not_exists else ""
        where_sql = f" WHERE {where_condition}" if where_condition else ""

        # Warn if non-default index type is requested
        if index_type and index_type != CONST_INDEX_TYPE.BTREE:
            warnings.warn(
                f"{cls.db_backend_name}: Index type {index_type} not supported, using default BTREE"
            )

        create_sql = (
            f"CREATE {unique_sql}INDEX {if_not_exists_sql}{index_name} "
            f"ON {qualified_table} ({columns_sql}){where_sql}"
        )

        try:
            with contextlib.closing(ibis_backend.con.cursor()) as cur:
                cur.execute(create_sql)
            return True
        except Exception as e:
            print(f"Error creating index {index_name}: {e}")
            return False

    @classmethod
    def drop_index(
        cls,
        ibis_backend: SQLBackend,
        index_name: str,
        *,
        table_name: str | None = None,
        database: str | None = None,
        if_exists: bool = True
    ) -> bool:
        """
        Drop an index.

        Note: table_name parameter is accepted for API consistency but not used
        in DuckDB/SQLite DROP INDEX syntax.
        """
        if_exists_sql = "IF EXISTS " if if_exists else ""
        drop_sql = f"DROP INDEX {if_exists_sql}{index_name}"

        try:
            with contextlib.closing(ibis_backend.con.cursor()) as cur:
                cur.execute(drop_sql)
            return True
        except Exception as e:
            print(f"Error dropping index {index_name}: {e}")
            return False

    @classmethod
    def index_exists(
        cls,
        ibis_backend: SQLBackend,
        index_name: str,
        *,
        table_name: str | None = None,
        database: str | None = None
    ) -> bool:
        """
        Check if an index exists.

        Uses database-specific system catalogs via _get_index_exists_sql.
        """
        check_sql = cls._get_index_exists_sql(index_name, table_name, database)
        result = cls.run_sql(ibis_backend, check_sql)

        if result is None:
            return False

        count = ma.relation(result).to_dict()["count"][0]
        return count > 0

    @classmethod
    def list_indexes(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        *,
        database: str | None = None
    ) -> list[dict]:
        """
        List all indexes for a table.

        Uses database-specific system catalogs via _get_list_indexes_sql.
        """
        list_sql = cls._get_list_indexes_sql(table_name, database)
        result = cls.run_sql(ibis_backend, list_sql)

        if result is None:
            return []

        # Convert Ibis table to list of dicts
        return ma.relation(result).to_dicts()

    @classmethod
    def _get_index_exists_sql(
        cls,
        index_name: str,
        table_name: str | None,
        database: str | None
    ) -> str:
        """
        Provider-specific SQL to check index existence.

        Must be implemented by concrete classes.
        """
        raise NotImplementedError(
            f"{cls.__name__}: _get_index_exists_sql must be implemented"
        )

    @classmethod
    def _get_list_indexes_sql(
        cls,
        table_name: str,
        database: str | None
    ) -> str:
        """
        Provider-specific SQL to list all indexes for a table.

        Must be implemented by concrete classes.
        """
        raise NotImplementedError(
            f"{cls.__name__}: _get_list_indexes_sql must be implemented"
        )

    # ===========================
    # UPSERT OPERATIONS
    # ===========================

    @classmethod
    def _upsert(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        df: ir.Table | t.Any,
        *,
        conflict_columns: list[str] | str,
        update_columns: list[str] | str | None = None,
        conflict_action: str = CONST_CONFLICT_ACTION.UPDATE,
        update_condition: str | None = None,
        database: str | None = None,
        schema: str | None = None,
    ) -> None:
        """
        Perform upsert using INSERT ... ON CONFLICT syntax.

        Supports:
        - Simple upsert: INSERT ON CONFLICT DO UPDATE
        - Insert-ignore: INSERT ON CONFLICT DO NOTHING
        - Conditional updates: UPDATE with WHERE clause
        - Auto-detection: update all columns except conflict columns
        """
        # Normalize inputs
        conflict_cols = cls._normalize_columns(conflict_columns)

        # Auto-detect columns from dataframe
        all_columns = ma.relation(df).columns

        if update_columns is None:
            # Default: update all columns except conflict columns
            update_cols = [col for col in all_columns if col not in conflict_cols]
        else:
            update_cols = cls._normalize_columns(update_columns)

        # Validate table exists
        if not cls.table_exists(ibis_backend, table_name, database=database):
            raise ValueError(f"Target table '{table_name}' does not exist")

        # Validate conflict action
        if conflict_action not in [CONST_CONFLICT_ACTION.UPDATE, CONST_CONFLICT_ACTION.NOTHING]:
            raise ValueError(
                f"conflict_action must be '{CONST_CONFLICT_ACTION.UPDATE}' or "
                f"'{CONST_CONFLICT_ACTION.NOTHING}', got '{conflict_action}'"
            )

        # Warn if UPDATE parameters provided with NOTHING action
        if conflict_action == CONST_CONFLICT_ACTION.NOTHING:
            if update_cols or update_condition:
                warnings.warn(
                    "update_columns and update_condition are ignored when "
                    "conflict_action='NOTHING'"
                )

        # Generate unique staging table name
        staging_table = f"temp_upsert_{uuid.uuid4().hex[:8]}"
        qualified_table = cls._format_qualified_table(table_name, database=database, schema=schema)

        # Build SQL components
        all_cols_sql = ", ".join(all_columns)
        conflict_cols_sql = ", ".join(conflict_cols)

        if conflict_action == CONST_CONFLICT_ACTION.UPDATE:
            if not update_cols:
                raise ValueError(
                    "No columns to update. Either provide update_columns or ensure "
                    "dataframe has columns beyond conflict_columns"
                )
            update_set_sql = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
            where_sql = f" WHERE {update_condition}" if update_condition else ""
            on_conflict_sql = (
                f"ON CONFLICT ({conflict_cols_sql}) DO UPDATE SET {update_set_sql}{where_sql}"
            )
        else:  # NOTHING
            on_conflict_sql = f"ON CONFLICT ({conflict_cols_sql}) DO NOTHING"

        # SQLite requires WHERE clause to avoid parsing ambiguity with ON CONFLICT
        # See: https://sqlite.org/lang_upsert.html#parsing_ambiguity
        upsert_sql = f"""
            INSERT INTO {qualified_table} ({all_cols_sql})
            SELECT {all_cols_sql} FROM {staging_table}
            WHERE true
            {on_conflict_sql}
        """

        # Debug output
        print(f"DEBUG: Upsert SQL:\n{upsert_sql}")
        print(f"DEBUG: Qualified table: {qualified_table}")
        print(f"DEBUG: Staging table: {staging_table}")
        print(f"DEBUG: Conflict columns: {conflict_cols}")
        print(f"DEBUG: Update columns: {update_cols}")

        try:
            # Check if this is DuckDB (has register method)
            if hasattr(ibis_backend.con, 'register'):
                # DuckDB/MotherDuck: Use register for in-memory dataframe
                with contextlib.closing(ibis_backend.con.cursor()) as cur:
                    cur.execute("BEGIN TRANSACTION")
                    cur.register(staging_table, df)
                    cur.execute(upsert_sql)
                    cur.unregister(staging_table)
                    cur.execute("COMMIT")
            else:
                # SQLite: Create temp table outside transaction, then upsert
                # Create temp table from dataframe
                ibis_backend.create_table(staging_table, df, temp=True, overwrite=True)

                try:
                    # Execute upsert
                    with contextlib.closing(ibis_backend.con.cursor()) as cur:
                        cur.execute(upsert_sql)
                        ibis_backend.con.commit()
                finally:
                    # Clean up temp table
                    try:
                        ibis_backend.drop_table(staging_table, force=True)
                    except Exception:
                        pass  # Temp table may auto-drop

        except Exception as e:
            print(f"Error during upsert to {table_name}: {e}")
            raise
