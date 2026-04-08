"""DEPRECATED: DuckDB-family operations are now in
mountainash_data.backends.ibis.operations."""

from mountainash_data.backends.ibis.operations import (  # noqa: F401
    _DuckDBFamilyOperationsMixin,
    duckdb_get_index_exists_sql,
    duckdb_get_list_indexes_sql,
    sqlite_get_index_exists_sql,
    sqlite_get_list_indexes_sql,
    motherduck_get_index_exists_sql,
    motherduck_get_list_indexes_sql,
    motherduck_list_tables,
)
