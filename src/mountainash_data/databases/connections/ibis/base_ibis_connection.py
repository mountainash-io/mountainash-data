from typing import Any, Dict, Optional
import ibis
import ibis.expr.types.relations as ir 

from ...connections.base_db_connection import BaseDBConnection

from abc import abstractmethod
from mountainash_constants import CONST_DATAFRAME_FRAMEWORK
from mountainash_settings import SettingsParameters
from mountainash_utils_dataframes import DataFrameFactory, BaseDataFrame

class BaseIbisConnection(BaseDBConnection):


    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ssh_auth_settings_parameters: Optional[SettingsParameters] = None,
                 connection_string: Optional[str] = None):

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters, 
                         ssh_auth_settings_parameters=ssh_auth_settings_parameters,
                         connection_string=connection_string)


        self.db_abstraction_layer:  Optional[str] = None
        self.db_connection:         Optional[ibis.BaseBackend] = None


    @abstractmethod
    def connect_ibis(self, connection_string: str) -> ibis.BaseBackend:
        """Connect to the database using the provided connection string."""
        raise NotImplementedError





    def connect(self):  

        connection_string = self.prepare_connection_parameters()
        if connection_string is None:
            raise ValueError("Connection string is required to establish connection")

        self.db_connection = self.connect_ibis(connection_string=connection_string)
       
        if self.db_connection is None:
            raise ValueError("Connection could not be established")


    def close(self):
        """Close the connection to the database."""
        raise NotImplementedError
    

    def sql_ibis(self, sql: str) -> ir.Table:
        """Connect to the database using the provided connection string."""

        if self.db_connection is None:
            self.connect()
                                         
        return self.db_connection.sql(sql)    


    def table_ibis(self, tablename: str) -> ir.Table:
        """Connect to the database using the provided connection string."""

        if self.db_connection is None:
            self.connect()

        return self.db_connection.table(name=tablename)    
    

    def get_table_as_dataframe(self, 
                tablename: str, 
                dataframe_framework: Optional[str] = None) -> BaseDataFrame:
        """Execute the given SQL statement."""

        result = self.table_ibis(tablename=tablename)

        if not dataframe_framework:
            dataframe_framework = CONST_DATAFRAME_FRAMEWORK.IBIS.value

        if dataframe_framework == CONST_DATAFRAME_FRAMEWORK.IBIS.value:
            return DataFrameFactory.create_ibis_dataframe_object(df=result, ibis_backend=self.db_connection, )   

        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PANDAS.value:
            return DataFrameFactory.create_pandas_dataframe_object(df=result)        

        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.POLARS.value:
            return DataFrameFactory.create_polars_dataframe_object(df=result)        

     

        else:
            raise ValueError(f"Dataframe framework {dataframe_framework} not supported")

    def get_sql_as_dataframe(self, 
                sql: str, 
                dataframe_framework: Optional[str] = None) -> BaseDataFrame:
        """Execute the given SQL statement."""

        result = self.sql_ibis(sql)

        if not dataframe_framework:
            dataframe_framework = CONST_DATAFRAME_FRAMEWORK.IBIS.value

        if dataframe_framework == CONST_DATAFRAME_FRAMEWORK.IBIS.value:
            return DataFrameFactory.create_ibis_dataframe_object(df=result, ibis_backend=self.db_connection)        

        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PANDAS.value:
            return DataFrameFactory.create_pandas_dataframe_object(df=result)        

        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.POLARS.value:
            return DataFrameFactory.create_polars_dataframe_object(df=result)        


        else:
            raise ValueError(f"Dataframe framework {dataframe_framework} not supported")


    def call_procedure(self, procedure_name: str, 
                            params: Optional[Dict[str, Any]] = None):
        """Call the specified stored procedure with the given parameters."""
        raise NotImplementedError
    
