"""Merged ibis operations module.

Consolidates:
- base_ibis_operations.py (676 LOC) — actual operations implementation
- _base_ibis_mixin.py (98 LOC) — helper methods as module-level functions
- _duckdb_family_mixin.py (314 LOC) — DuckDB-family shared SQL as class + module functions
- Per-dialect index SQL functions (duckdb, sqlite, motherduck)

This module is the single source of operations truth for all ibis backends.
The original files have been replaced with shims that re-export from here.
"""

import typing as t

from abc import abstractmethod, ABC
import contextlib
import warnings
import uuid

import ibis
import ibis.expr.types.relations as ir
from ibis.expr.schema import SchemaLike
from ibis.backends.sql import SQLBackend

from mountainash_settings import SettingsParameters
import mountainash as ma

from mountainash_data.core.constants import (
    CONST_DB_BACKEND,
    CONST_CONFLICT_ACTION,
    CONST_INDEX_TYPE,
)


# ===========================================================================
# MODULE-LEVEL HELPER FUNCTIONS
# Salvaged from _base_ibis_mixin.py — converted from mixin methods to functions
# ===========================================================================

def _generate_index_name(
    table_name: str,
    columns: list[str],
    *,
    unique: bool = False,
    suffix: str | None = None
) -> str:
    """Generate standardized index name."""
    sorted_cols = sorted(columns)
    prefix = "uidx" if unique else "idx"
    col_part = "_".join(sorted_cols)
    parts = [prefix, table_name, col_part]
    if suffix:
        parts.append(suffix)
    return "_".join(parts)


def _format_qualified_table(
    table_name: str,
    *,
    database: str | None = None,
    schema: str | None = None
) -> str:
    """Format fully qualified table name."""
    parts = []
    if database:
        parts.append(database)
    if schema:
        parts.append(schema)
    parts.append(table_name)
    return ".".join(parts)


def _normalize_columns(
    columns: list[str] | str
) -> list[str]:
    """Normalize column input to list."""
    if isinstance(columns, str):
        return [columns]
    if not columns:
        raise ValueError("At least one column must be specified")
    return list(columns)


# ===========================================================================
# PER-DIALECT CAPABILITY HOOK FUNCTIONS
# Salvaged from duckdb_ibis_operations.py, sqlite_ibis_operations.py,
# motherduck_ibis_operations.py
# ===========================================================================

# --- DuckDB ---

def duckdb_get_index_exists_sql(
    index_name: str,
    table_name: str | None,
    database: str | None
) -> str:
    """DuckDB uses duckdb_indexes() system function."""
    where_clauses = [f"index_name = '{index_name}'"]
    if table_name:
        where_clauses.append(f"table_name = '{table_name}'")
    if database:
        where_clauses.append(f"database_name = '{database}'")

    where_sql = " AND ".join(where_clauses)
    return f"SELECT COUNT(*) as count FROM duckdb_indexes() WHERE {where_sql}"


def duckdb_get_list_indexes_sql(
    table_name: str,
    database: str | None
) -> str:
    """DuckDB uses duckdb_indexes() system function."""
    where_clauses = [f"table_name = '{table_name}'"]
    if database:
        where_clauses.append(f"database_name = '{database}'")

    where_sql = " AND ".join(where_clauses)
    return f"""
        SELECT
            index_name as name,
            sql as definition,
            is_unique as unique
        FROM duckdb_indexes()
        WHERE {where_sql}
    """


# --- SQLite ---

def sqlite_get_index_exists_sql(
    index_name: str,
    table_name: str | None,
    database: str | None
) -> str:
    """SQLite uses sqlite_master system table.
    Note: database parameter is not used as SQLite doesn't support cross-database queries.
    """
    where_clauses = [
        "type = 'index'",
        f"name = '{index_name}'"
    ]
    if table_name:
        where_clauses.append(f"tbl_name = '{table_name}'")

    where_sql = " AND ".join(where_clauses)
    return f"SELECT COUNT(*) as count FROM sqlite_master WHERE {where_sql}"


def sqlite_get_list_indexes_sql(
    table_name: str,
    database: str | None
) -> str:
    """SQLite uses sqlite_master system table.
    Note: database parameter is not used as SQLite doesn't support cross-database queries.
    """
    return f"""
        SELECT
            name,
            sql as definition,
            CASE WHEN sql LIKE '%UNIQUE%' THEN 1 ELSE 0 END as "unique"
        FROM sqlite_master
        WHERE type = 'index'
        AND tbl_name = '{table_name}'
    """


# --- MotherDuck ---

def motherduck_get_index_exists_sql(
    index_name: str,
    table_name: str | None,
    database: str | None
) -> str:
    """MotherDuck uses DuckDB's duckdb_indexes() system function."""
    where_clauses = [f"index_name = '{index_name}'"]
    if table_name:
        where_clauses.append(f"table_name = '{table_name}'")
    if database:
        where_clauses.append(f"database_name = '{database}'")

    where_sql = " AND ".join(where_clauses)
    return f"SELECT COUNT(*) as count FROM duckdb_indexes() WHERE {where_sql}"


def motherduck_get_list_indexes_sql(
    table_name: str,
    database: str | None
) -> str:
    """MotherDuck uses DuckDB's duckdb_indexes() system function."""
    where_clauses = [f"table_name = '{table_name}'"]
    if database:
        where_clauses.append(f"database_name = '{database}'")

    where_sql = " AND ".join(where_clauses)
    return f"""
        SELECT
            index_name as name,
            sql as definition,
            is_unique as unique
        FROM duckdb_indexes()
        WHERE {where_sql}
    """


# MotherDuck-specific list_tables override
def motherduck_list_tables(
    ibis_backend: t.Any,
    like: str | None = None,
    database: str | None = None,
) -> list[str]:
    """MotherDuck-specific list_tables using DuckDB backend's database parameter."""
    return ibis_backend.list_tables(like=like, database=database) if ibis_backend is not None else []


# ===========================================================================
# _DuckDBFamilyOperationsMixin
# Salvaged from _duckdb_family_mixin.py — retained as class for backward
# compatibility. Internal helpers now delegate to module-level functions.
# ===========================================================================

class _DuckDBFamilyOperationsMixin:
    """
    Shared operations for DuckDB-family databases.

    This class provides common implementation for databases that share
    DuckDB's SQL syntax: DuckDB, MotherDuck, and SQLite.

    Internal helpers delegate to module-level functions (see top of file).
    """

    # Delegate helper methods to module-level functions so existing code
    # that calls cls._generate_index_name() etc. still works.
    _generate_index_name = staticmethod(_generate_index_name)
    _format_qualified_table = staticmethod(_format_qualified_table)
    _normalize_columns = staticmethod(_normalize_columns)

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
        """Create an index using DuckDB/SQLite syntax."""
        columns_list = _normalize_columns(columns)

        if index_name is None:
            index_name = _generate_index_name(table_name, columns_list, unique=unique)

        qualified_table = _format_qualified_table(table_name, database=database)
        columns_sql = ", ".join(columns_list)

        unique_sql = "UNIQUE " if unique else ""
        if_not_exists_sql = "IF NOT EXISTS " if if_not_exists else ""
        where_sql = f" WHERE {where_condition}" if where_condition else ""

        if index_type and index_type != CONST_INDEX_TYPE.BTREE:
            warnings.warn(
                f"Index type {index_type} not supported, using default BTREE"
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
        """Drop an index."""
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
        """Check if an index exists."""
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
        """List all indexes for a table."""
        list_sql = cls._get_list_indexes_sql(table_name, database)
        result = cls.run_sql(ibis_backend, list_sql)

        if result is None:
            return []

        return ma.relation(result).to_dicts()

    @classmethod
    def _get_index_exists_sql(
        cls,
        index_name: str,
        table_name: str | None,
        database: str | None
    ) -> str:
        """Provider-specific SQL to check index existence. Must be overridden."""
        raise NotImplementedError(
            f"{cls.__name__}: _get_index_exists_sql must be implemented"
        )

    @classmethod
    def _get_list_indexes_sql(
        cls,
        table_name: str,
        database: str | None
    ) -> str:
        """Provider-specific SQL to list all indexes for a table. Must be overridden."""
        raise NotImplementedError(
            f"{cls.__name__}: _get_list_indexes_sql must be implemented"
        )

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
        """Perform upsert using INSERT ... ON CONFLICT syntax."""
        conflict_cols = _normalize_columns(conflict_columns)
        all_columns = ma.relation(df).columns

        if update_columns is None:
            update_cols = [col for col in all_columns if col not in conflict_cols]
        else:
            update_cols = _normalize_columns(update_columns)

        if not cls.table_exists(ibis_backend, table_name, database=database):
            raise ValueError(f"Target table '{table_name}' does not exist")

        if conflict_action not in [CONST_CONFLICT_ACTION.UPDATE, CONST_CONFLICT_ACTION.NOTHING]:
            raise ValueError(
                f"conflict_action must be '{CONST_CONFLICT_ACTION.UPDATE}' or "
                f"'{CONST_CONFLICT_ACTION.NOTHING}', got '{conflict_action}'"
            )

        if conflict_action == CONST_CONFLICT_ACTION.NOTHING:
            if update_cols or update_condition:
                warnings.warn(
                    "update_columns and update_condition are ignored when "
                    "conflict_action='NOTHING'"
                )

        staging_table = f"temp_upsert_{uuid.uuid4().hex[:8]}"
        qualified_table = _format_qualified_table(table_name, database=database, schema=schema)

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

        upsert_sql = f"""
            INSERT INTO {qualified_table} ({all_cols_sql})
            SELECT {all_cols_sql} FROM {staging_table}
            WHERE true
            {on_conflict_sql}
        """

        print(f"DEBUG: Upsert SQL:\n{upsert_sql}")
        print(f"DEBUG: Qualified table: {qualified_table}")
        print(f"DEBUG: Staging table: {staging_table}")
        print(f"DEBUG: Conflict columns: {conflict_cols}")
        print(f"DEBUG: Update columns: {update_cols}")

        try:
            if hasattr(ibis_backend.con, 'register'):
                with contextlib.closing(ibis_backend.con.cursor()) as cur:
                    cur.execute("BEGIN TRANSACTION")
                    cur.register(staging_table, df)
                    cur.execute(upsert_sql)
                    cur.unregister(staging_table)
                    cur.execute("COMMIT")
            else:
                ibis_backend.create_table(staging_table, df, temp=True, overwrite=True)

                try:
                    with contextlib.closing(ibis_backend.con.cursor()) as cur:
                        cur.execute(upsert_sql)
                        ibis_backend.con.commit()
                finally:
                    try:
                        ibis_backend.drop_table(staging_table, force=True)
                    except Exception:
                        pass
        except Exception as e:
            print(f"Error during upsert to {table_name}: {e}")
            raise


# Compatibility shim: _BaseIbisMixin was the original mixin class
class _BaseIbisMixin:
    """DEPRECATED compatibility shim — methods are now module-level functions
    in mountainash_data.backends.ibis.operations."""

    _generate_index_name = staticmethod(_generate_index_name)
    _format_qualified_table = staticmethod(_format_qualified_table)
    _normalize_columns = staticmethod(_normalize_columns)


# ===========================================================================
# BaseIbisOperations
# Salvaged verbatim from base_ibis_operations.py.
# ===========================================================================

class BaseIbisOperations(ABC):

    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ):
        ...

    ## SQL Queries

    @property
    @abstractmethod
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.DUCKDB

    @classmethod
    def run_sql(cls,
            ibis_backend: SQLBackend,
            query: str,
            /,
            *,
            schema: SchemaLike | None = None,
            dialect: str | None = None,
        ) -> t.Optional[ir.Table]:

        try:
            return ibis_backend.sql(query,
                                        schema=schema,
                                        dialect=dialect
                            )
        except Exception as e:
            print(f"Error executing SQL: {e}")
            return None

    @classmethod
    def run_expr(
        cls,
        ibis_backend: ibis.BaseBackend,
        ibis_expr: ir.Expr,
        /,
        params: t.Dict | None = None,
        limit: str | None = "default",
        **kwargs: t.Any,
        ) -> t.Any:

        try:
            return ibis_backend.execute(ibis_expr,
                                        params=params,
                                        limit=limit,
                                        **kwargs
                            ) if ibis_backend is not None else None
        except Exception as e:
            print(f"Error executing expression: {e}")
            return None

    @classmethod
    def to_sql(
        cls,
        ibis_backend: ibis.BaseBackend,
        expr: ir.Expr,
        /,
        params=None,
        limit: str | None = None,
        pretty: bool = False,
        **kwargs: t.Any,
        ) -> t.Optional[str]:

        try:
            return ibis_backend.compile(expr,
                                        params=params,
                                        limit=limit,
                                        pretty=pretty,
                                        **kwargs
                            ) if ibis_backend is not None else None
        except Exception as e:
            print(f"Error compiling expression to SQL: {e}")
            return None

    ## Tables
    @classmethod
    def table(
        cls,
        ibis_backend: ibis.BaseBackend,
        object_name: str,
        /,
        schema: str | None = None,
        database: tuple[str, str] | str | None = None
        ) -> t.Optional[ir.Table]:

        try:
            return ibis_backend.table(object_name,
                                        database=database
                                        ) if ibis_backend is not None else None
        except Exception as e:
            print(f"Error getting table {object_name}: {e}")
            return None

    @classmethod
    def create_table(cls,
                    ibis_backend: ibis.BaseBackend,
                     table_name: str,
                     df: ir.Table|t.Any,
                     /,
                     schema: t.Optional[ibis.Schema] = None,
                     database: str | None = None,
                     temp: bool = False,
                     overwrite: bool = False,
                     ) -> None:
        try:
            ibis_backend.create_table(table_name,
                                        obj=df,
                                        schema=schema,
                                        database=database,
                                        temp=temp,
                                        overwrite=overwrite) if ibis_backend is not None else None
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")
            return None

    @classmethod
    def drop_table(
        cls,
        ibis_backend: ibis.BaseBackend,
        table_name: str,
        /,
        database:  str | None = None,
        force: bool = False,
        ) -> bool:

        try:
            ibis_backend.drop_table(table_name,
                                        database=database,
                                        force=force) if ibis_backend is not None else None
            return True
        except Exception:
            return False

    ## Views
    @classmethod
    def create_view(
        cls,
        ibis_backend: ibis.BaseBackend,
        view_name: str,
        ibis_table_expr: ir.Table,
        /,
        database: str | None = None,
        schema: str | None = None,
        overwrite: bool = False,
        ) -> t.Optional[ir.Table]:

        try:
            return ibis_backend.create_view(view_name,
                                    obj=ibis_table_expr,
                                    database=database,
                                    overwrite=overwrite) if ibis_backend is not None else None
        except Exception as e:
            print(f"Error creating view {view_name}: {e}")
            return None

    @classmethod
    def drop_view(
        cls,
        ibis_backend: ibis.BaseBackend,
        view_name: str,
        /,
        database: str | None = None,
        schema: str | None = None,
        force: bool = False,
    ) -> bool:

        try:
            ibis_backend.drop_view(view_name,
                                    database=database,
                                    force=force) if ibis_backend is not None else None
            return True
        except Exception:
            print(f"Error dropping view {view_name}")
            return False

    # Backend Data Manipulation

    @classmethod
    def insert(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        /,
        df: ir.Table|t.Any,
        database: str | None = None,
        schema: str | None = None,
        overwrite: bool = False,
    ) -> bool:

        try:
            ibis_backend.insert(table_name,
                                    obj=df,
                                    database=database,
                                    overwrite=overwrite) if ibis_backend is not None else None
            return True
        except Exception as e:
            print(f"Error inserting into table {table_name}: {e}")
            return False

    @classmethod
    def truncate(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        /,
        database: str | None = None,
        schema: str | None = None
    ) -> None:

        try:
            ibis_backend.truncate_table(
                                        table_name,
                                        schema=schema,
                                        database=database) if ibis_backend is not None else None
        except Exception as e:
            print(f"Error truncating table {table_name}: {e}")
            return None

    @classmethod
    def upsert(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        df: ir.Table|t.Any,
        /,
        conflict_columns: list[str] | str,
        update_columns: list[str] | str | None = None,
        conflict_action: str = CONST_CONFLICT_ACTION.UPDATE,
        update_condition: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        ) -> None:

        try:
            cls._upsert(
                ibis_backend,
                table_name,
                df,
                conflict_columns=conflict_columns,
                update_columns=update_columns,
                conflict_action=conflict_action,
                update_condition=update_condition,
                database=database,
                schema=schema) if ibis_backend is not None else None
        except Exception as e:
            print(f"Error upserting into table {table_name}: {e}")
            raise

    @classmethod
    @abstractmethod
    def _upsert(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        df: ir.Table|t.Any,
        *,
        conflict_columns: list[str] | str,
        update_columns: list[str] | str | None = None,
        conflict_action: str = CONST_CONFLICT_ACTION.UPDATE,
        update_condition: str | None = None,
        database: str | None = None,
        schema: str | None = None,
    ) -> None:
        raise NotImplementedError(f"Upsert is not implemented for this backend")

    # ===========================
    # INDEX MANAGEMENT
    # ===========================

    @classmethod
    @abstractmethod
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
        raise NotImplementedError(f"create_index is not implemented for this backend")

    @classmethod
    def create_unique_index(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        columns: list[str] | str,
        *,
        index_name: str | None = None,
        where_condition: str | None = None,
        database: str | None = None
    ) -> bool:
        return cls.create_index(
            ibis_backend, table_name, columns,
            index_name=index_name, unique=True,
            where_condition=where_condition, database=database
        )

    @classmethod
    @abstractmethod
    def drop_index(
        cls,
        ibis_backend: SQLBackend,
        index_name: str,
        *,
        table_name: str | None = None,
        database: str | None = None,
        if_exists: bool = True
    ) -> bool:
        raise NotImplementedError(f"drop_index is not implemented for this backend")

    @classmethod
    @abstractmethod
    def index_exists(
        cls,
        ibis_backend: SQLBackend,
        index_name: str,
        *,
        table_name: str | None = None,
        database: str | None = None
    ) -> bool:
        raise NotImplementedError(f"index_exists is not implemented for this backend")

    @classmethod
    @abstractmethod
    def list_indexes(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        *,
        database: str | None = None
    ) -> list[dict]:
        raise NotImplementedError(f"list_indexes is not implemented for this backend")

    # ===========================
    # t.Optionally Implemented Functions
    # ===========================

    @classmethod
    def list_tables(cls,
                ibis_backend: ibis.BaseBackend,
                table_name: str | None = None,
                database: tuple[str, str] | str | None = None
                    ) -> t.List[str]:

        try:
            return ibis_backend.list_tables(like=table_name, database=database) if ibis_backend is not None else []
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    @classmethod
    def rename_table(cls,
                ibis_backend: ibis.BaseBackend,
                old_name: str,
                new_name: str,
                ) -> None:

        try:
            return cls._rename_table(old_name=old_name, new_name=new_name) if ibis_backend is not None else None
        except Exception as e:
            print(f"Error renaming table {old_name} to {new_name}: {e}")
            return None

    @classmethod
    @abstractmethod
    def _rename_table(cls,
                old_name: str,
                new_name: str,
                ) -> None:
        raise NotImplementedError

    @classmethod
    def table_exists(cls,
                ibis_backend: ibis.BaseBackend,
                table_name: str | None = None,
                database: tuple[str, str] | str | None = None
                    ) -> bool:

        tables = cls.list_tables(ibis_backend, table_name=table_name, database=database)
        return True if table_name in tables else False


# ===========================================================================
# Concrete per-dialect operations classes
# Preserved as concrete classes so the OperationsFactory can instantiate them.
# Each class combines _DuckDBFamilyOperationsMixin (concrete upsert/index
# implementations) with BaseIbisOperations (abstract template) and provides
# the dialect-specific index catalog SQL via overriding _get_index_exists_sql
# and _get_list_indexes_sql.
# ===========================================================================

class DuckDB_IbisOperations(_DuckDBFamilyOperationsMixin, BaseIbisOperations):
    """DuckDB operations — concrete implementation."""

    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.DUCKDB

    @classmethod
    def _rename_table(cls, old_name: str, new_name: str) -> None:
        raise NotImplementedError(f"{CONST_DB_BACKEND.DUCKDB}: rename_table not yet implemented")

    @classmethod
    def _get_index_exists_sql(cls, index_name: str, table_name: str | None, database: str | None) -> str:
        return duckdb_get_index_exists_sql(index_name, table_name, database)

    @classmethod
    def _get_list_indexes_sql(cls, table_name: str, database: str | None) -> str:
        return duckdb_get_list_indexes_sql(table_name, database)


class SQLite_IbisOperations(_DuckDBFamilyOperationsMixin, BaseIbisOperations):
    """SQLite operations — concrete implementation."""

    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.SQLITE

    @classmethod
    def _rename_table(cls, old_name: str, new_name: str) -> None:
        raise NotImplementedError(f"{CONST_DB_BACKEND.SQLITE}: rename_table not yet implemented")

    @classmethod
    def _get_index_exists_sql(cls, index_name: str, table_name: str | None, database: str | None) -> str:
        return sqlite_get_index_exists_sql(index_name, table_name, database)

    @classmethod
    def _get_list_indexes_sql(cls, table_name: str, database: str | None) -> str:
        return sqlite_get_list_indexes_sql(table_name, database)


class MotherDuck_IbisOperations(_DuckDBFamilyOperationsMixin, BaseIbisOperations):
    """MotherDuck operations — concrete implementation."""

    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.MOTHERDUCK

    @classmethod
    def _rename_table(cls, old_name: str, new_name: str) -> None:
        raise NotImplementedError(f"{CONST_DB_BACKEND.MOTHERDUCK}: rename_table not yet implemented")

    @classmethod
    def _get_index_exists_sql(cls, index_name: str, table_name: str | None, database: str | None) -> str:
        return motherduck_get_index_exists_sql(index_name, table_name, database)

    @classmethod
    def _get_list_indexes_sql(cls, table_name: str, database: str | None) -> str:
        return motherduck_get_list_indexes_sql(table_name, database)


class Trino_IbisOperations(BaseIbisOperations):
    """Trino operations — connection mode is handled via DialectSpec registry."""

    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.TRINO

    @classmethod
    def _rename_table(cls, old_name: str, new_name: str) -> None:
        raise NotImplementedError(f"{CONST_DB_BACKEND.TRINO}: rename_table not yet implemented")

    @classmethod
    def _upsert(cls, *args, **kwargs) -> None:
        raise NotImplementedError(f"{CONST_DB_BACKEND.TRINO}: upsert not supported")

    @classmethod
    def create_index(cls, *args, **kwargs) -> bool:
        raise NotImplementedError(f"{CONST_DB_BACKEND.TRINO}: create_index not supported")

    @classmethod
    def drop_index(cls, *args, **kwargs) -> bool:
        raise NotImplementedError(f"{CONST_DB_BACKEND.TRINO}: drop_index not supported")

    @classmethod
    def index_exists(cls, *args, **kwargs) -> bool:
        raise NotImplementedError(f"{CONST_DB_BACKEND.TRINO}: index_exists not supported")

    @classmethod
    def list_indexes(cls, *args, **kwargs) -> list[dict]:
        raise NotImplementedError(f"{CONST_DB_BACKEND.TRINO}: list_indexes not supported")
