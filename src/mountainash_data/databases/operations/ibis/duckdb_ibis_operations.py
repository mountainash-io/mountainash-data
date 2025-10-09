

from .base_ibis_operations import BaseIbisOperations
from ._duckdb_family_mixin import _DuckDBFamilyOperationsMixin

from ...constants import CONST_DB_BACKEND



class DuckDB_IbisOperations(_DuckDBFamilyOperationsMixin, BaseIbisOperations):

    # From BaseDBConnection
    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.DUCKDB

    @classmethod
    def _rename_table(cls, old_name: str, new_name: str) -> None:
        """Rename table (not yet implemented for DuckDB)."""
        raise NotImplementedError(f"{CONST_DB_BACKEND.DUCKDB}: rename_table not yet implemented")

    # ===========================
    # PROVIDER-SPECIFIC INDEX CATALOG QUERIES
    # ===========================

    @classmethod
    def _get_index_exists_sql(
        cls,
        index_name: str,
        table_name: str | None,
        database: str | None
    ) -> str:
        """
        DuckDB uses duckdb_indexes() system function.
        """
        where_clauses = [f"index_name = '{index_name}'"]
        if table_name:
            where_clauses.append(f"table_name = '{table_name}'")
        if database:
            where_clauses.append(f"database_name = '{database}'")

        where_sql = " AND ".join(where_clauses)
        return f"SELECT COUNT(*) as count FROM duckdb_indexes() WHERE {where_sql}"

    @classmethod
    def _get_list_indexes_sql(
        cls,
        table_name: str,
        database: str | None
    ) -> str:
        """
        DuckDB uses duckdb_indexes() system function.
        """
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
