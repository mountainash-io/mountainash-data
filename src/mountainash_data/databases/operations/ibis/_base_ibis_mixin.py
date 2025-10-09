
from abc import ABC

# from mountainash_data.databases.base_db_connection import BaseDBConnection

# from abc import abstractmethod

# from mountainash_dataframes import BaseDataFrame, IbisDataFrame


class _BaseIbisMixin(ABC):

    # ===========================
    # HELPER METHODS
    # ===========================

    @classmethod
    def _generate_index_name(
        cls,
        table_name: str,
        columns: list[str],
        *,
        unique: bool = False,
        suffix: str | None = None
    ) -> str:
        """
        Generate standardized index name.

        Args:
            table_name: Target table name
            columns: Columns in the index
            unique: Whether this is a unique index
            suffix: Optional suffix to add

        Returns:
            Generated index name
        """
        sorted_cols = sorted(columns)
        prefix = "uidx" if unique else "idx"
        col_part = "_".join(sorted_cols)
        parts = [prefix, table_name, col_part]
        if suffix:
            parts.append(suffix)
        return "_".join(parts)

    @classmethod
    def _format_qualified_table(
        cls,
        table_name: str,
        *,
        database: str | None = None,
        schema: str | None = None
    ) -> str:
        """
        Format fully qualified table name.

        Args:
            table_name: Base table name
            database: Optional database name
            schema: Optional schema name

        Returns:
            Qualified table name (e.g., "database.schema.table" or "database.table")
        """
        parts = []
        if database:
            parts.append(database)
        if schema:
            parts.append(schema)
        parts.append(table_name)
        return ".".join(parts)

    @classmethod
    def _normalize_columns(
        cls,
        columns: list[str] | str
    ) -> list[str]:
        """
        Normalize column input to list.

        Args:
            columns: Column name(s) as string or list

        Returns:
            List of column names

        Raises:
            ValueError: If columns is empty
        """
        if isinstance(columns, str):
            return [columns]
        if not columns:
            raise ValueError("At least one column must be specified")
        return list(columns)


    ###########################
    # t.Optionally Implemented Functions
