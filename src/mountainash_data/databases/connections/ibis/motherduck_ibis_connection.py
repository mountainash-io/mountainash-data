import typing as t
import ibis.backends.duckdb as ir_backend
import contextlib
import warnings
from pydantic_settings import BaseSettings


from mountainash_settings import SettingsParameters

from .base_ibis_connection import BaseIbisConnection
from ...constants import IBIS_DB_CONNECTION_MODE, CONST_DB_BACKEND, CONST_DB_PROVIDER_TYPE
from ...settings import MotherDuckAuthSettings

# from mountainash_dataframes.utils.dataframe_filters import FilterCondition as fc

class MotherDuck_IbisConnection(BaseIbisConnection):


    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None
                 ):

        self._ibis_backend: t.Optional[ir_backend.Backend] = None
        self._ibis_connection_mode: str = connection_mode if connection_mode is not None else IBIS_DB_CONNECTION_MODE.CONNECTION_STRING

        self.supports_upsert = True

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)


    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        return CONST_DB_PROVIDER_TYPE.MOTHERDUCK

    #From BaseIbisConnection
    @property
    def ibis_backend(self) -> t.Optional[ir_backend.Backend]:
        return self._ibis_backend

    @property
    def ibis_connection_mode(self) -> str:
        return self._ibis_connection_mode

    #From BaseDBConnection
    @property
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.MOTHERDUCK

    @property
    def connection_string_scheme(self) -> str:
        return "duckdb://md:"

    @property
    def settings_class(self) -> t.Type[BaseSettings]:
        return MotherDuckAuthSettings

    def set_post_connection_options(self, post_connection_options: t.Dict[str, t.Any]):

        if self.ibis_backend is not None:
            with contextlib.closing(self.ibis_backend.con.cursor()) as cur:
                for option_key, option_value in post_connection_options.items():
                    try:
                        cur.execute(f"SET @@session.{option_key} = '{option_value}'")
                    except Exception as e:
                        warnings.warn(f"Unable to set session {option_key} to UTC: {e}")


    # def _upsert(
    #     self,
    #     table_name: str,
    #     df: ir.Table|t.Any,
    #     natural_key_columns: list[str]|str,
    #     data_columns: list[str]|str,
    #     database: str | None = None,
    #     schema: str | None = None,
    #     ) -> None:

    #     if isinstance(natural_key_columns, str):
    #         natural_key_columns = [natural_key_columns]

    #     if len(natural_key_columns) == 0:
    #         raise ValueError("Natural Keys must be provided")

    #     if isinstance(data_columns, str):
    #         data_columns = [data_columns]

    #     if len(data_columns) == 0:
    #         raise ValueError("Data Columns must be provided")


    #     if not self.table_exists(table_name=table_name, database=database):
    #         raise ValueError(f"Target Upsert table '{table_name}' does not exist")


    #     #Prepare temporary staging table
    #     # schema = DataFrameUtils.get_table_schema(df)

    #     table_suffix: str = str(uuid.uuid4()).replace("-", "")
    #     staging_table_name = f"temp_upsert_{table_suffix}"

    #     #SQL upsert parts!
    #     list_all_columns = natural_key_columns + data_columns
    #     sql_all_columns = ", ".join(list_all_columns) if list_all_columns else ""
    #     sql_natural_keys = ", ".join(natural_key_columns) if natural_key_columns else ""
    #     sql_value_fields = ", ".join([f"{col} = excluded.{col}" for col in data_columns]) if data_columns else ""

    #     upsert_sql = f"INSERT INTO {database}.{table_name}({sql_all_columns}) SELECT {sql_all_columns} FROM {staging_table_name} ON CONFLICT ({sql_natural_keys}) DO UPDATE SET {sql_value_fields}"

    #     if self.ibis_backend is not None:

    #         with contextlib.closing(self.ibis_backend.con.cursor()) as cur:

    #             cur.execute("BEGIN TRANSACTION;")
    #             cur.register(staging_table_name, df)
    #             cur.execute(f"{upsert_sql}")
    #             cur.unregister(staging_table_name)
    #             cur.execute("COMMIT;")


    # def unique_index_exists(
    #     self,
    #     table_name: str,
    #     natural_key_columns: list[str],
    #     database: str | None = None
    # ) -> bool|None:
    #     """
    #     Ensures that an index exists on the specified natural key columns for a table in DuckDB.

    #     Args:
    #         table_name: Name of the target table
    #         natural_key_columns: List of column names that form the natural key
    #         database: Optional database name
    #         schema: Optional schema name
    #     """
    #     if not natural_key_columns:
    #         return None

    #     # Format the fully qualified table name
    #     qualified_table = table_name
    #     if database:
    #         qualified_table = f"{database}.{qualified_table}"

    #     # Create a standardized index name
    #     index_name = self.create_unique_index_name(table_name, natural_key_columns)

    #     # In DuckDB, we can check for indexes using the information_schema
    #     check_index_sql = f"""
    #     SELECT COUNT(*) as index_exists
    #     FROM pg_catalog.pg_indexes
    #     WHERE indexname = '{index_name}'
    #     AND tablename = '{table_name}'
    #     """

    #     # Check if the index exists
    #     index_exists_table = self.run_sql_as_ibis_dataframe(check_index_sql)
    #     index_exists = index_exists_table.get_column_as_list("index_exists")[0] > 0 if index_exists_table is not None else False

    #     return index_exists

    # def create_unique_index(
    #     self,
    #     table_name: str,
    #     natural_key_columns: list[str],
    #     database: str | None = None
    # ) -> bool|None:


    #     if isinstance(natural_key_columns, str):
    #         natural_key_columns = [natural_key_columns]

    #     if len(natural_key_columns) == 0:
    #         raise ValueError("Natural Keys must be provided")

    #     index_exists = self.unique_index_exists(table_name=table_name, natural_key_columns=natural_key_columns, database=database)

    #     if not index_exists:

    #         qualified_table = table_name
    #         if database:
    #             qualified_table = f"{database}.{qualified_table}"

    #         # Create a standardized index name
    #         index_name = self.create_unique_index_name(table_name, natural_key_columns)

    #         sql_natural_keys = ", ".join(natural_key_columns)
    #         create_index_sql = f"CREATE UNIQUE INDEX {index_name} ON {qualified_table} ({sql_natural_keys});"

    #         with contextlib.closing(self.ibis_backend.con.cursor()) as cur:
    #             cur.execute(create_index_sql)


    # def create_unique_index_name(self,
    #                         table_name: str,
    #                         natural_key_columns: list[str]) -> str:

    #     # Create a standardized index name
    #     natural_key_columns.sort()
    #     unique_index_name = f"idx_{table_name}_{'_'.join(natural_key_columns)}"

    #     return unique_index_name
