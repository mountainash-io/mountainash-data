from typing import Any, Dict, Optional
import ibis
import ibis.expr.types.relations as ir 
from ibis.expr.schema import SchemaLike
from ibis.backends.sql import SQLBackend
from mountainash_data.databases.connections.base_db_connection import BaseDBConnection

from abc import abstractmethod
from mountainash_settings import SettingsParameters
from mountainash_data import BaseDataFrame, IbisDataFrame

class BaseIbisConnection(BaseDBConnection):


    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ssh_auth_settings_parameters: Optional[SettingsParameters] = None,
                 connection_string: Optional[str] = None):

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters, 
                         ssh_auth_settings_parameters=ssh_auth_settings_parameters,
                         connection_string=connection_string)

        self.database_backend: str 
        self.db_abstraction_layer:  Optional[str] = None
        self.ibis_backend:          Optional[SQLBackend|Any] = None



    ###########################
    # Core Functions

    def connect(self):  

        if self.ibis_backend is None:

            #Prepare Connection String
            connection_string = self.prepare_connection_parameters()
            if connection_string is None:
                raise ValueError(f"{self.database_backend}: Connection string is required to establish connection")

            #TODO: Manage Retries if unsuccessful
            self.ibis_backend = self._connect(connection_string=connection_string)

        if self.ibis_backend is None:
            raise ValueError(f"{self.database_backend}: Connection could not be established")


    def _connect(self, connection_string: str) -> SQLBackend:
        """
        Default Implementation to connect to the database using the provided connection string.
        Over-ride in subclasses if a different implementation is necessary."""

        ibis_backend: Any = ibis.connect(connection_string)
    
        return ibis_backend



    def close(self):
        """Close the connection to the database."""
    
        self.disconnect()

    def disconnect(self):
        """Close the connection to the database."""
    
        if self.ibis_backend is not None:
            self.ibis_backend.disconnect()

    ## SQL Queries

    def run_sql(self, 
            query: str,
            schema: SchemaLike | None = None,
            dialect: str | None = None,            
        ) -> Optional[ir.Table]:

        self.connect()

        return self.ibis_backend.sql(query=query,
                                     schema=schema,
                                     dialect=dialect
                         )  if self.ibis_backend is not None else None  
        

    def run_expr(
        self,
        ibis_expr: ir.Expr,
        params: Dict | None = None,
        limit: str | None = "default",
        **kwargs: Any,
        ) -> Any:

        self.connect()

        return self.ibis_backend.execute(expr=ibis_expr,
                                     params=params,
                                     limit=limit,
                                     **kwargs
                         )  if self.ibis_backend is not None else None  


    def to_sql(
        self,
        expr: ir.Expr,
        params=None,
        limit: str | None = None,
        pretty: bool = False,
        **kwargs: Any,
        ) -> Optional[str]:

        self.connect()

        return self.ibis_backend.compile(expr=expr,
                                     params=params,
                                     limit=limit,
                                     pretty=pretty,
                                     **kwargs
                         )  if self.ibis_backend is not None else None  


    ## Tables
    def table(
        self,
        object_name: str,
        schema: str | None = None,
        database: tuple[str, str] | str | None = None
        ) -> Optional[ir.Table]:

        self.connect()

        return self.ibis_backend.table(name=object_name, 
                                       schema=schema,
                                       database=database
                                       ) if self.ibis_backend is not None else None   
    
    
    def create_table(self, 
                     table_name: str, 
                     df: ir.Table|Any,
                        # | pd.DataFrame
                        # | pa.Table
                        # | pl.DataFrame
                        # | pl.LazyFrame
                        # | None = None,
                     schema: Optional[ibis.Schema] = None,
                     database: str | None = None,
                     temp: bool = False,
                     overwrite: bool = False,
                     ) -> None:
        """Connect to the database using the provided connection string."""

        self.connect()

        self.ibis_backend.create_table(name=table_name, 
                                       obj=df, 
                                       schema=schema, 
                                       database=database, 
                                       temp=temp, 
                                       overwrite=overwrite)  if self.ibis_backend is not None else None    


    def drop_table(
        self,
        table_name: str,
        database: tuple[str, str] | str | None = None,
        force: bool = False,
        ) -> None:

        self.connect()

        self.ibis_backend.drop_table(name=table_name, 
                                     database=database, 
                                     force=force)   if self.ibis_backend is not None else None    

    ## Views
    def create_view(
        self,
        view_name: str,
        ibis_table_expr: ir.Table,
        database: str | None = None,
        schema: str | None = None,
        overwrite: bool = False,
        ) -> Optional[ir.Table]:

        self.connect()

        self.ibis_backend.create_view(name=view_name, 
                                    obj=ibis_table_expr,
                                    database=database, 
                                    schema=schema, 
                                    overwrite=overwrite)  if self.ibis_backend is not None else None  

    def drop_view(
        self,
        view_name: str,
        database: str | None = None,
        schema: str | None = None,
        force: bool = False,
    ) -> None:

        self.connect()

        self.ibis_backend.drop_view(name=view_name, 
                                    database=database, 
                                    schema=schema, 
                                    force=force)  if self.ibis_backend is not None else None     


    # Backend Data Manipulation

    def insert(
        self,
        table_name: str,
        df: ir.Table|Any,
        # | pd.DataFrame
        # | pa.Table
        # | pl.DataFrame
        # | pl.LazyFrame
        # | None = None,
        #TODO: Support more DataFrames, etc
        # obj: pd.DataFrame | ir.Table | list | dict,
        database: str | None = None,
        schema: str | None = None,
        overwrite: bool = False,
    ) -> None:    

        #TODO: Support more DataFrames

        self.connect()

        self.ibis_backend.insert(   table_name=table_name, 
                                    obj=df, 
                                    schema=schema,
                                    database=database,
                                    overwrite=overwrite)  if self.ibis_backend is not None else None  
        



    def truncate(
        self, 
        table_name: str, 
        database: str | None = None, 
        schema: str | None = None
    ) -> None:

        self.connect()

        self.ibis_backend.truncate_table(   
                                    name=table_name, 
                                    schema=schema,
                                    database=database)  if self.ibis_backend is not None else None  


    def upsert(
        self,
        table_name: str,
        df: ir.Table|Any,
        database: str | None = None,
        schema: str | None = None,
        natural_key_columns: list[str] | None = None,
        data_columns: list[str] | None = None
        ) -> None:

        self.connect()

        self._upsert(   table_name=table_name, 
                        df=df, 
                        database=database,
                        schema=schema,
                        natural_key_columns=natural_key_columns,
                        data_columns=data_columns)  if self.ibis_backend is not None else None  

    @abstractmethod
    def _upsert(
        self,
        table_name: str,
        df: ir.Table|Any,
        database: str | None = None,
        schema: str | None = None,
        natural_key_columns: list[str] | None = None,
        data_columns: list[str] | None = None
        ) -> None:

        raise NotImplementedError(f"{self.database_backend}: Upsert is not implemented for this backend")


    ###########################
    # Optionally Implemented Functions

    def list_tables(self, 
                like: str | None = None,
                database: tuple[str, str] | str | None = None,
                schema: str | None = None
                    ) -> None:

        self.connect()
        return self._list_tables(like=like, database=database, schema=schema)  if self.ibis_backend is not None else None     

    def _list_tables(self,                
                like: str | None = None,
                database: tuple[str, str] | str | None = None,
                schema: str | None = None
                    ) -> None:

        raise NotImplementedError


    def rename_table(self, 
                old_name: str,
                new_name: str,
                ) -> None:

        self.connect()
        return self._rename_table(old_name=old_name, new_name=new_name)  if self.ibis_backend is not None else None     


    def _rename_table(self, 
                old_name: str,
                new_name: str,
                ) -> None:
        
        raise NotImplementedError



    # @abstractmethod
    # def list_databases(self, like:  Optional[str]= None, database: Optional[str]= None) -> list[str]:
    #     """Connect to the database using the provided connection string."""

    #     pass

    ###########################
    # Mountain Ash Abstractions
    def table_as_dataframe(self, 
        object_name: str,
        schema: str | None = None,
        database: tuple[str, str] | str | None = None,
        tablename_prefix: Optional[str] = None

        ) -> Optional[IbisDataFrame]:
        
        """Get a table or view as a DataFrame."""

        result: ibis.Table | None = self.table(object_name=object_name, 
                                               schema=schema,
                                               database=database)

        return IbisDataFrame(df=result,
                            ibis_backend=self.ibis_backend,  
                            tablename_prefix=tablename_prefix)   

    def run_sql_as_dataframe(self, 
            query: str,
            schema: SchemaLike | None = None,
            dialect: str | None = None, 
            tablename_prefix: Optional[str] = None
            ) -> Optional[BaseDataFrame]:
        """Execute the given SQL statement."""

        result: Optional[ir.Table] = self.run_sql(query=query,
                                                  schema=schema,
                                                  dialect=dialect
                                                  )

        return IbisDataFrame(df=result,
                            ibis_backend=self.ibis_backend,  
                            tablename_prefix=tablename_prefix)       

    def run_expr_as_dataframe(self, 
            ibis_expr: ir.Expr,
            params: Dict | None = None,
            limit: str | None = "default",
            tablename_prefix: Optional[str] = None,
            **kwargs: Any,
            ) -> Optional[BaseDataFrame]:
        """Execute the given SQL statement."""

        result: Optional[ir.Table] = self.run_expr(ibis_expr=ibis_expr,
                                                  params=params,
                                                  limit=limit,
                                                  **kwargs
                                                  )

        return IbisDataFrame(df=result,
                            ibis_backend=self.ibis_backend,  
                            tablename_prefix=tablename_prefix)     

