import typing as t
import ibis.backends.duckdb as ir_backend
import contextlib
import warnings
from pydantic_settings import BaseSettings
import ibis.expr.types.relations as ir
import uuid

import ibis

from mountainash_settings import SettingsParameters
from mountainash_dataframes import DataFrameUtils

from .base_ibis_operations import BaseIbisOperations

from ...constants import IBIS_DB_CONNECTION_MODE, CONST_DB_BACKEND
from ...settings import MotherDuckAuthSettings

# from mountainash_dataframes.utils.dataframe_filters import FilterCondition as fc

class MotherDuck_IbisOperations(BaseIbisOperations):


    #From BaseDBConnection
    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.MOTHERDUCK

    @classmethod
    def _list_tables(cls,
                ibis_backend: ir_backend.Backend,
                like: str | None = None,
                database: str | None = None,
                **kwargs
                ) -> t.List[str]:

        return ibis_backend.list_tables(like=like, database=database) if ibis_backend is not None else []



    @classmethod
    def _upsert(
        cls,
        ibis_backend: ir_backend.Backend,
        table_name: str,
        df: ir.Table|t.Any,
        natural_key_columns: list[str]|str,
        data_columns: list[str]|str,
        database: str | None = None,
        schema: str | None = None,
        ) -> None:

        if isinstance(natural_key_columns, str):
            natural_key_columns = [natural_key_columns]

        if len(natural_key_columns) == 0:
            raise ValueError("Natural Keys must be provided")

        if isinstance(data_columns, str):
            data_columns = [data_columns]

        if len(data_columns) == 0:
            raise ValueError("Data Columns must be provided")


        if not cls.table_exists(ibis_backend, table_name=table_name, database=database):
            raise ValueError(f"Target Upsert table '{table_name}' does not exist")


        #Prepare temporary staging table
        # schema = DataFrameUtils.get_table_schema(df)

        table_suffix: str = str(uuid.uuid4()).replace("-", "")
        staging_table_name = f"temp_upsert_{table_suffix}"

        #SQL upsert parts!
        list_all_columns = natural_key_columns + data_columns
        sql_all_columns = ", ".join(list_all_columns) if list_all_columns else ""
        sql_natural_keys = ", ".join(natural_key_columns) if natural_key_columns else ""
        sql_value_fields = ", ".join([f"{col} = excluded.{col}" for col in data_columns]) if data_columns else ""

        upsert_sql = f"INSERT INTO {database}.{table_name}({sql_all_columns}) SELECT {sql_all_columns} FROM {staging_table_name} ON CONFLICT ({sql_natural_keys}) DO UPDATE SET {sql_value_fields}"

        if ibis_backend is not None:

            with contextlib.closing(ibis_backend.con.cursor()) as cur:

                cur.execute("BEGIN TRANSACTION;")
                cur.register(staging_table_name, df)
                cur.execute(f"{upsert_sql}")
                cur.unregister(staging_table_name)
                cur.execute("COMMIT;")


    @classmethod
    def unique_index_exists(
        cls,
        ibis_backend: ir_backend.Backend,
        table_name: str,
        natural_key_columns: list[str],
        database: str | None = None
    ) -> bool|None:
        """
        Ensures that an index exists on the specified natural key columns for a table in DuckDB.

        Args:
            table_name: Name of the target table
            natural_key_columns: List of column names that form the natural key
            database: Optional database name
            schema: Optional schema name
        """
        if not natural_key_columns:
            return None

        # Format the fully qualified table name
        qualified_table = table_name
        if database:
            qualified_table = f"{database}.{qualified_table}"

        # Create a standardized index name
        index_name = cls.create_unique_index_name(table_name, natural_key_columns)

        # In DuckDB, we can check for indexes using the information_schema
        check_index_sql = f"""
        SELECT COUNT(*) as index_exists
        FROM pg_catalog.pg_indexes
        WHERE indexname = '{index_name}'
        AND tablename = '{table_name}'
        """

        # Check if the index exists
        index_exists_table: ir.Table|None = cls.run_sql(ibis_backend, check_index_sql)
        index_exists = DataFrameUtils.get_column_as_list(index_exists_table, "index_exists")[0] > 0 if index_exists_table is not None else False

        return index_exists

    @classmethod
    def create_unique_index(
        cls,
        ibis_backend: ir_backend.Backend,
        table_name: str,
        natural_key_columns: list[str],
        database: str | None = None
    ) -> bool|None:


        if isinstance(natural_key_columns, str):
            natural_key_columns = [natural_key_columns]

        if len(natural_key_columns) == 0:
            raise ValueError("Natural Keys must be provided")

        index_exists = cls.unique_index_exists(ibis_backend, table_name=table_name, natural_key_columns=natural_key_columns, database=database)

        if not index_exists:

            qualified_table = table_name
            if database:
                qualified_table = f"{database}.{qualified_table}"

            # Create a standardized index name
            index_name = cls.create_unique_index_name(table_name, natural_key_columns)

            sql_natural_keys = ", ".join(natural_key_columns)
            create_index_sql = f"CREATE UNIQUE INDEX {index_name} ON {qualified_table} ({sql_natural_keys});"

            with contextlib.closing(ibis_backend.con.cursor()) as cur:
                cur.execute(create_index_sql)


    @classmethod
    def create_unique_index_name(cls,
                            table_name: str,
                            natural_key_columns: list[str]) -> str:

        # Create a standardized index name
        natural_key_columns.sort()
        unique_index_name = f"idx_{table_name}_{'_'.join(natural_key_columns)}"

        return unique_index_name
