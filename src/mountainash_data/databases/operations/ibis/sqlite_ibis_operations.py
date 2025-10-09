



from .base_ibis_operations import BaseIbisOperations
from ._duckdb_family_mixin import _DuckDBFamilyOperationsMixin
from ...constants import CONST_DB_BACKEND


class SQLite_IbisOperations(_DuckDBFamilyOperationsMixin, BaseIbisOperations):

    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.SQLITE

    @classmethod
    def _rename_table(cls, old_name: str, new_name: str) -> None:
        """Rename table (not yet implemented for SQLite)."""
        raise NotImplementedError(f"{CONST_DB_BACKEND.SQLITE}: rename_table not yet implemented")

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
        SQLite uses sqlite_master system table.
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

    @classmethod
    def _get_list_indexes_sql(
        cls,
        table_name: str,
        database: str | None
    ) -> str:
        """
        SQLite uses sqlite_master system table.
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
