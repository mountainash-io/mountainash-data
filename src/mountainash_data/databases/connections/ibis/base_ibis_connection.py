from typing import Any, Dict, Optional
import ibis
import ibis.expr.types.relations as ir 

from mountainash_data.databases.connections.base_db_connection import BaseDBConnection

from abc import abstractmethod
from mountainash_settings import SettingsParameters
from mountainash_data import DataFrameFactory, BaseDataFrame

class BaseIbisConnection(BaseDBConnection):


    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ssh_auth_settings_parameters: Optional[SettingsParameters] = None,
                 connection_string: Optional[str] = None):

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters, 
                         ssh_auth_settings_parameters=ssh_auth_settings_parameters,
                         connection_string=connection_string)


        self.db_abstraction_layer:  Optional[str] = None
        self.ibis_backend:         Optional[ibis.BaseBackend] = None


    @abstractmethod
    def connect_ibis(self, connection_string: str) -> ibis.BaseBackend:
        """Connect to the database using the provided connection string."""
        raise NotImplementedError


    def connect(self):  

        connection_string = self.prepare_connection_parameters()
        if connection_string is None:
            raise ValueError("Connection string is required to establish connection")

        self.ibis_backend = self.connect_ibis(connection_string=connection_string)
       
        if self.ibis_backend is None:
            raise ValueError("Connection could not be established")


    def close(self):
        """Close the connection to the database."""
        raise NotImplementedError
    

    @abstractmethod
    def sql_ibis(self, sql: str) -> ir.Table:
        pass

        # if self.ibis_backend is None:
        #     self.connect()

        # if self.ibis_backend is None:
        #     raise ValueError("Connection could not be established")

        # return self.ibis_backend.sql(sql)    
        

    def get_table(self, name: str, database: Optional[str]= None) -> ir.Table:
        """Connect to the database using the provided connection string."""

        if self.ibis_backend is None:
            self.connect()

        if self.ibis_backend is None:
            raise ValueError("Connection could not be established")

        return self.ibis_backend.table(name=name, database=database)    
    

    def get_table_as_dataframe(self, 
                name: str, database: Optional[str]= None
                ) -> BaseDataFrame:
        """Execute the given SQL statement."""

        result = self.get_table(name=name, database=database)

        return DataFrameFactory.create_ibis_dataframe_object_from_dataframe(df=result, 
                                                                            ibis_backend=self.ibis_backend,  
                                                                            tablename_prefix=name)   


    def get_sql_as_dataframe(self, 
                sql: str, 
                tablename_prefix: Optional[str] = None) -> BaseDataFrame:
        """Execute the given SQL statement."""

        result = self.sql_ibis(sql)

        return DataFrameFactory.create_ibis_dataframe_object_from_dataframe(df=result, 
                                                                            ibis_backend=self.ibis_backend,  
                                                                            tablename_prefix=tablename_prefix)        



    def call_procedure(self, procedure_name: str, 
                            params: Optional[Dict[str, Any]] = None):
        """Call the specified stored procedure with the given parameters."""

        if self.ibis_backend is None:
            self.connect()

        if self.ibis_backend is None:
            raise ValueError("Connection could not be established")


        self.ibis_backend.create_view
        raise NotImplementedError
    

    def drop_table(self, name: str, database: Optional[str]= None) -> None:
        """Connect to the database using the provided connection string."""

        if self.ibis_backend is None:
            self.connect()

        if self.ibis_backend is None:
            raise ValueError("Connection could not be established")

        self.ibis_backend.drop_table(name=name, database=database, force=True)    

    def drop_view(self, name: str, database: Optional[str]= None) -> None:
        """Connect to the database using the provided connection string."""

        if self.ibis_backend is None:
            self.connect()

        if self.ibis_backend is None:
            raise ValueError("Connection could not be established")

        self.ibis_backend.drop_view(name=name, database=database, force=True)    

    def list_tables(self, name: str, database: Optional[str]= None) -> None:
        """Connect to the database using the provided connection string."""

        if self.ibis_backend is None:
            self.connect()

        if self.ibis_backend is None:
            raise ValueError("Connection could not be established")

        self.ibis_backend.list_tables(name=name, database=database)    
