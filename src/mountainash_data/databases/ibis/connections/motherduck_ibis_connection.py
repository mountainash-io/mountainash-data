import typing as t
import ibis.backends.duckdb as ir_backend
import contextlib
import warnings
from pydantic_settings import BaseSettings
import ibis.expr.types.relations as ir 
import uuid

from ..base_ibis_connection import BaseIbisConnection
from ..constants import IBIS_DB_connection_mode

from mountainash_constants import CONST_DB_BACKEND
from mountainash_settings import SettingsParameters
from mountainash_settings.settings.auth.database import MotherDuckAuthSettings
from mountainash_data.dataframes.utils.dataframe_filters import FilterCondition as fc
from mountainash_data import DataFrameUtils, DataFrameFactory

class MotherDuck_IbisConnection(BaseIbisConnection):


    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 connection_mode: t.Optional[str] = None
                 ):

        self._ibis_backend: t.Optional[ir_backend.Backend] = None
        self._ibis_connection_mode: str = connection_mode if connection_mode is not None else IBIS_DB_connection_mode.CONNECTION_STRING

        self.supports_upsert = True

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)
        
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
        return CONST_DB_BACKEND.MOTHERDUCK.value

    @property
    def connection_string_scheme(self) -> str:
        return "duckdb://md:"

    @property
    def settings_class(self) -> t.Type[BaseSettings]:
        return MotherDuckAuthSettings



    def _list_tables(self,                
                like: str | None = None,
                database: tuple[str, str] | str | None = None,
                schema: str | None = None
                    ) -> t.List[str]:

        return self.ibis_backend.list_tables(like=like, database=database, schema=schema) if self.ibis_backend is not None else []        
    

    def set_post_connection_options(self, post_connection_options: t.Dict[str, t.Any]):

        if self.ibis_backend is not None:
            with contextlib.closing(self.ibis_backend.con.cursor()) as cur:
                for option_key, option_value in post_connection_options.items():
                    try:
                        cur.execute(f"SET @@session.{option_key} = '{option_value}'")
                    except Exception as e:
                        warnings.warn(f"Unable to set session {option_key} to UTC: {e}")


    def _upsert_prepare_staging_table(
        self,
        table_name: str,
        df: ir.Table|t.Any,
        database: str | None = None,
        schema: str | None = None,
    ) -> str:


        ### Temp Table setup
        # target table schema
        # temp_df = self.table_as_ibis_dataframe(table_name).head(0).to_pyarrow()#.filter(filter_condition=fc.always_false())
        temp_df = DataFrameFactory.create_ibis_dataframe_object_from_dataframe(df=df).head(0).to_pyarrow()


        #create temptable
        staging_table_name = f"temp_upsert_{table_name}_{uuid.uuid4()}"
        self.ibis_backend.create_table(name = staging_table_name, 
                                    obj=temp_df, 
                                    schema=schema,
                                    database=database,
                                    overwrite=True, 
                                    temp=True
                                    )

        return staging_table_name



    def _upsert(
        self,
        table_name: str,
        df: ir.Table|t.Any,
        database: str | None = None,
        schema: str | None = None,
        natural_key_columns: list[str] | None = None,
        data_columns: list[str] | None = None
        ) -> None:

        if not self.table_exists(table_name=table_name, database=database, schema=schema):
            raise ValueError(f"Target Upsert table '{table_name}' does not exist")


        #Prepare temporary staging table
        staging_table_name = self._upsert_prepare_staging_table(table_name=table_name, df=df, database=database, schema=schema)

        #Insert real data into temp table
        self.ibis_backend.insert(table_name=staging_table_name,
                                obj=df,
                                schema=schema,
                                database=database,
                                overwrite=True
                                )


        #Now for the upsert!
        sql_natural_keys = ", ".join(natural_key_columns) if natural_key_columns else ""
        sql_value_fields = ", ".join([f"dest.{col} = stg.{col}" for col in data_columns]) if data_columns else ""

        upsert_sql = f"""INSERT INTO {table_name} dest
select * from {staging_table_name} stg
ON CONFLICT ({sql_natural_keys})
DO UPDATE SET {sql_value_fields};"""

        #Execute it
        self.run_sql(upsert_sql)