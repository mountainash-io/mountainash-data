import typing as t # import t.Any, t.Dict, t.Optional

import ibis
import ibis.expr.types.relations as ir
from ibis.expr.schema import SchemaLike
from ibis.backends.sql import SQLBackend
from mountainash_data.databases.base_db_connection import BaseDBConnection
from abc import abstractmethod

# from abc import abstractmethod
from mountainash_settings import SettingsParameters

from mountainash_dataframes.constants import CONST_DATAFRAME_FRAMEWORK
from ..constants import IBIS_DB_connection_mode,  CONST_DB_ABSTRACTION_LAYER
from mountainash_dataframes import BaseDataFrame, IbisDataFrame
from mountainash_dataframes.utils.dataframe_utils import DataFrameUtils


class BaseIbisConnection(BaseDBConnection):

    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ):

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters,
                         )


    @property
    def db_abstraction_layer(self) -> str:
        return CONST_DB_ABSTRACTION_LAYER.IBIS


    @property
    @abstractmethod
    def ibis_backend(self) -> t.Optional[SQLBackend|t.Any]:
        """Concret Ibis backend connection object."""
        pass

    @property
    @abstractmethod
    def ibis_connection_mode(self) -> str:
        """Connect via a connection string, kwargs or both."""
        pass

    @property
    @abstractmethod
    def connection_string_scheme(self) -> str:
        """Template string for database connection."""
        pass

    # @property
    # @abstractmethod
    # def supports_upsert(self) -> str:
    #     """Whether upserts are supported"""
    #     pass




    ###########################
    # Core Functions


    def connect(self,
                connection_string: t.Optional[str] = None,
                connection_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
                **kwargs) -> SQLBackend:

        """Connect with explicitly provided connection parameters"""


        if self.ibis_backend is None:

            if connection_string is not None:
                if connection_kwargs is not None:
                    self._connect(connection_string=connection_string, connection_kwargs=connection_kwargs, **kwargs)
                else:
                    self._connect(connection_string=connection_string, **kwargs)

            else:
                if connection_kwargs is not None:
                    self._connect(connection_string = self.connection_string_scheme, connection_kwargs=connection_kwargs, **kwargs)
                else:
                    self.connect_default(**kwargs)

        return self.ibis_backend


    def connect_default(self, **kwargs) -> SQLBackend:
        """Connect using default configuration"""
        # Implementation for connecting with default parameters

        if self.ibis_backend is None:

            connection_string_template = self.get_connection_string_template(scheme=self.connection_string_scheme)
            connectionstring_params = self.get_connection_string_params()

            connection_string = self.format_connection_string(template=connection_string_template, params=connectionstring_params)
            connection_kwargs = self.get_connection_kwargs(db_abstraction_layer = self.db_abstraction_layer)

            if self.ibis_connection_mode == IBIS_DB_connection_mode.CONNECTION_STRING:
                self._connect(connection_string=connection_string, **kwargs)

            elif self.ibis_connection_mode == IBIS_DB_connection_mode.KWARGS:
                #combine connectionstring_params and connection_kwargs
                connection_kwargs = {**connectionstring_params, **connection_kwargs}
                self._connect(connection_string=self.connection_string_scheme, connection_kwargs=connection_kwargs, **kwargs)

            elif self.ibis_connection_mode == IBIS_DB_connection_mode.HYBRID:
                self._connect(connection_string=connection_string, connection_kwargs=connection_kwargs, **kwargs)

        return self.ibis_backend



    def _connect(self,
                 connection_string: t.Optional[str] = None,
                 connection_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
                 **kwargs
                 ) -> SQLBackend:
        """
        Default Implementation to connect to the database using the provided connection string.
        By default this relies on the connection_string_scheme to determine the backend dynamically.
        Over-ride in subclasses if a different implementation is necessary, such as using the custom backend directly.
        """

        if connection_kwargs is None:
            connection_kwargs = {}

        #combine connection_kwargs and kwargs
        connection_kwargs = {**connection_kwargs, **kwargs}

        if connection_string is None:
            raise ValueError(f"{self.db_backend_name}: Connection string is required to establish connection")

        self._ibis_backend : t.Any = ibis.connect(connection_string, **connection_kwargs)

        return self.ibis_backend



    def close(self):
        """Close the connection to the database."""

        self.disconnect()

    def disconnect(self):
        """Close the connection to the database."""

        if self.ibis_backend is not None:
            self.ibis_backend.disconnect()
            self._ibis_backend  = None

    def is_connected(self) -> bool:
        """ Is the connection open?"""
        if self.ibis_backend is None:
            return False
        else:
            return True




    ## SQL Queries

    def run_sql(self,
            query: str,
            schema: SchemaLike | None = None,
            dialect: str | None = None,
        ) -> t.Optional[ir.Table]:

        self.connect()

        return self.ibis_backend.sql(query=query,
                                     schema=schema,
                                     dialect=dialect
                         )  if self.ibis_backend is not None else None


    def run_expr(
        self,
        ibis_expr: ir.Expr,
        params: t.Dict | None = None,
        limit: str | None = "default",
        **kwargs: t.Any,
        ) -> t.Any:

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
        **kwargs: t.Any,
        ) -> t.Optional[str]:

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
        ) -> t.Optional[ir.Table]:

        self.connect()

        return self.ibis_backend.table(object_name,
                                    #    schema=schema,
                                       database=database
                                       ) if self.ibis_backend is not None else None


    def create_table(self,
                     table_name: str,
                     df: ir.Table|t.Any,
                        # | pd.DataFrame
                        # | pa.Table
                        # | pl.DataFrame
                        # | pl.LazyFrame
                        # | None = None,
                     schema: t.Optional[ibis.Schema] = None,
                     database: str | None = None,
                     temp: bool = False,
                     overwrite: bool = False,
                     ) -> None:
        """Connect to the database using the provided connection string."""

        self.connect()

        #TODO: set a flag to load data. Default is true, if not use the schema.
        # if schema is None:
        #     schema = DataFrameUtils.get_table_schema(df)


        self.ibis_backend.create_table(table_name,
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
        ) -> bool:

        self.connect()

        try:
            self.ibis_backend.drop_table(table_name,
                                        database=database,
                                        force=force)   if self.ibis_backend is not None else None
            return True
        except Exception:
            return False



    ## Views
    def create_view(
        self,
        view_name: str,
        ibis_table_expr: ir.Table,
        database: str | None = None,
        schema: str | None = None,
        overwrite: bool = False,
        ) -> t.Optional[ir.Table]:

        self.connect()

        return self.ibis_backend.create_view(view_name,
                                obj=ibis_table_expr,
                                database=database,
                                # schema=schema,
                                overwrite=overwrite)  if self.ibis_backend is not None else None

    def drop_view(
        self,
        view_name: str,
        database: str | None = None,
        schema: str | None = None,
        force: bool = False,
    ) -> bool:

        self.connect()

        try:

            self.ibis_backend.drop_view(view_name,
                                    database=database,
                                    # schema=schema,
                                    force=force)  if self.ibis_backend is not None else None
            return True
        except Exception:
            return False


    # Backend Data Manipulation

    def insert(
        self,
        table_name: str,
        df: ir.Table|t.Any,
        # | pd.DataFrame
        # | pa.Table
        # | pl.DataFrame
        # | pl.LazyFrame
        # | None = None,
        #TODO: Support more DataFrames, etc
        # obj: pd.DataFrame | ir.Table | list | t.Dict,
        database: str | None = None,
        schema: str | None = None,
        overwrite: bool = False,
    ) -> bool:

        #TODO: Support more DataFrames

        self.connect()

        try:
            self.ibis_backend.insert(   table_name,
                                    obj=df,
                                    schema=schema,
                                    database=database,
                                    overwrite=overwrite)  if self.ibis_backend is not None else None
            return True
        except Exception:
            return False



    def truncate(
        self,
        table_name: str,
        database: str | None = None,
        schema: str | None = None
    ) -> None:

        self.connect()

        self.ibis_backend.truncate_table(
                                    table_name,
                                    schema=schema,
                                    database=database)  if self.ibis_backend is not None else None


    def upsert(
        self,
        table_name: str,
        df: ir.Table|t.Any,
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

    # @abstractmethod
    def _upsert(
        self,
        table_name: str,
        df: ir.Table|t.Any,
        database: str | None = None,
        schema: str | None = None,
        natural_key_columns: list[str] | None = None,
        data_columns: list[str] | None = None
        ) -> None:

        raise NotImplementedError(f"{self.db_backend_name}: Upsert is not implemented for this backend")





    ###########################
    # t.Optionally Implemented Functions

    def list_tables(self,
                table_name: str | None = None,
                database: tuple[str, str] | str | None = None,
                schema: str | None = None
                    ) -> t.List[str]:

        self.connect()
        return self._list_tables(like=table_name, database=database, schema=schema)

    def _list_tables(self,
                like: str | None = None,
                database: t.Any = None,
                schema: str | None = None
                    ) -> t.List[str]:

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


    def table_exists(self,
                table_name: str | None = None,
                database: tuple[str, str] | str | None = None,
                schema: str | None = None
                    ) -> bool:

        tables = self.list_tables(table_name=table_name, database=database, schema=schema)

        return True if table_name in tables else False


    # @abstractmethod
    # def list_databases(self, like:  t.Optional[str]= None, database: t.Optional[str]= None) -> list[str]:
    #     """Connect to the database using the provided connection string."""

    #     pass

    ###########################
    # Mountain Ash Abstractions
    def table_as_ibis_dataframe(self,
        object_name: str,
        schema: str | None = None,
        database: tuple[str, str] | str | None = None,
        tablename_prefix: t.Optional[str] = None

        ) -> t.Optional[IbisDataFrame]:

        """Get a table or view as a DataFrame."""

        result: ibis.Table | None = self.table(object_name=object_name,
                                               schema=schema,
                                               database=database)

        return IbisDataFrame(df=result,
                            ibis_backend=self.ibis_backend,
                            tablename_prefix=tablename_prefix)

    def run_sql_as_ibis_dataframe(self,
            query: str,
            schema: SchemaLike | None = None,
            dialect: str | None = None,
            tablename_prefix: t.Optional[str] = None
            ) -> t.Optional[BaseDataFrame]:
        """Execute the given SQL statement."""

        result: t.Optional[ir.Table] = self.run_sql(query=query,
                                                  schema=schema,
                                                  dialect=dialect
                                                  )

        return IbisDataFrame(df=result,
                            ibis_backend=self.ibis_backend,
                            tablename_prefix=tablename_prefix)


    def run_expr_as_ibis_dataframe(self,
            ibis_expr: ir.Expr,
            params: t.Dict | None = None,
            limit: str | None = "default",
            tablename_prefix: t.Optional[str] = None,
            **kwargs: t.Any,
            ) -> t.Optional[BaseDataFrame]:
        """Execute the given ibis expression statement."""

        result: t.Optional[ir.Table] = self.run_expr(ibis_expr=ibis_expr,
                                                  params=params,
                                                  limit=limit,
                                                  **kwargs
                                                  )

        return IbisDataFrame(df=result,
                            ibis_backend=self.ibis_backend,
                            tablename_prefix=tablename_prefix)

    #### Native Dataframe

    def table_as_native_dataframe(self,
        object_name: str,
        schema: str | None = None,
        database: tuple[str, str] | str | None = None,
        dataframe_framework: t.Optional[str] = CONST_DATAFRAME_FRAMEWORK.POLARS

        ) -> t.Optional[IbisDataFrame]:

        """Get a table or view as a DataFrame."""

        result: ibis.Table | None = self.table(object_name=object_name,
                                               schema=schema,
                                               database=database)

        return DataFrameUtils.cast_dataframe(df=result, dataframe_framework=dataframe_framework)


    def run_sql_as_native_dataframe(self,
            query: str,
            schema: SchemaLike | None = None,
            dialect: str | None = None,
            dataframe_framework: t.Optional[str] = CONST_DATAFRAME_FRAMEWORK.POLARS
            ) -> t.Optional[BaseDataFrame]:
        """Execute the given SQL statement."""

        result: t.Optional[ir.Table] = self.run_sql(query=query,
                                                  schema=schema,
                                                  dialect=dialect
                                                  )

        return DataFrameUtils.cast_dataframe(df=result, dataframe_framework=dataframe_framework)


    def run_expr_as_materialised_dataframe(self,
            ibis_expr: ir.Expr,
            params: t.Dict | None = None,
            limit: str | None = "default",
            dataframe_framework: t.Optional[str] = CONST_DATAFRAME_FRAMEWORK.POLARS,
            **kwargs: t.Any,
            ) -> t.Optional[BaseDataFrame]:
        """Execute the given ibis expression statement."""

        result: t.Optional[ir.Table] = self.run_expr(ibis_expr=ibis_expr,
                                                  params=params,
                                                  limit=limit,
                                                  **kwargs
                                                  )

        return DataFrameUtils.cast_dataframe(df=result, dataframe_framework=dataframe_framework)
