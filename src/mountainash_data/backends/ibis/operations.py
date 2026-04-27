"""Ibis operations module — module-level hook functions only.

Contains:
- Module-level helper functions: _generate_index_name, _format_qualified_table, _normalize_columns
- Per-dialect SQL functions: duckdb, sqlite, motherduck index SQL generators
- Standalone hook functions: duckdb_family_create_index, duckdb_family_drop_index,
  duckdb_family_upsert
"""

import typing as t
import contextlib
import warnings
import uuid

import mountainash as ma

from mountainash_data.core.constants import (
    CONST_CONFLICT_ACTION,
    CONST_INDEX_TYPE,
)


# ===========================================================================
# MODULE-LEVEL HELPER FUNCTIONS
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
# STANDALONE HOOK FUNCTIONS
# ===========================================================================

def duckdb_family_create_index(
    ibis_conn: t.Any,
    table_name: str,
    columns: list[str] | str,
    *,
    index_name: str | None = None,
    unique: bool = False,
    index_type: str | None = None,
    where_condition: str | None = None,
    database: str | None = None,
    if_not_exists: bool = True,
) -> None:
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

    with contextlib.closing(ibis_conn.con.cursor()) as cur:
        cur.execute(create_sql)


def duckdb_family_drop_index(
    ibis_conn: t.Any,
    index_name: str,
    *,
    table_name: str | None = None,
    database: str | None = None,
    if_exists: bool = True,
) -> None:
    """Drop an index using DuckDB/SQLite syntax."""
    if_exists_sql = "IF EXISTS " if if_exists else ""
    drop_sql = f"DROP INDEX {if_exists_sql}{index_name}"

    with contextlib.closing(ibis_conn.con.cursor()) as cur:
        cur.execute(drop_sql)


def duckdb_family_upsert(
    ibis_conn: t.Any,
    table_name: str,
    df: t.Any,
    *,
    conflict_columns: list[str] | str,
    update_columns: list[str] | str | None = None,
    conflict_action: str = CONST_CONFLICT_ACTION.UPDATE,
    update_condition: str | None = None,
    database: str | None = None,
    schema: str | None = None,
) -> None:
    """Perform upsert using INSERT ... ON CONFLICT syntax (DuckDB/SQLite)."""
    conflict_cols = _normalize_columns(conflict_columns)
    all_columns = ma.relation(df).columns

    if update_columns is None:
        update_cols = [col for col in all_columns if col not in conflict_cols]
    else:
        update_cols = _normalize_columns(update_columns)

    tables = ibis_conn.list_tables()
    if table_name not in tables:
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
    else:
        on_conflict_sql = f"ON CONFLICT ({conflict_cols_sql}) DO NOTHING"

    upsert_sql = f"""
        INSERT INTO {qualified_table} ({all_cols_sql})
        SELECT {all_cols_sql} FROM {staging_table}
        WHERE true
        {on_conflict_sql}
    """

    if hasattr(ibis_conn.con, 'register'):
        with contextlib.closing(ibis_conn.con.cursor()) as cur:
            cur.execute("BEGIN TRANSACTION")
            cur.register(staging_table, df)
            cur.execute(upsert_sql)
            cur.unregister(staging_table)
            cur.execute("COMMIT")
    else:
        ibis_conn.create_table(staging_table, df, temp=True, overwrite=True)
        try:
            with contextlib.closing(ibis_conn.con.cursor()) as cur:
                cur.execute(upsert_sql)
                ibis_conn.con.commit()
        finally:
            try:
                ibis_conn.drop_table(staging_table, force=True)
            except Exception:
                pass
