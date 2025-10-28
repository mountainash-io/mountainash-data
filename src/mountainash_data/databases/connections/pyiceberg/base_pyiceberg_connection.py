import typing as t # import t.Any, t.Dict, t.Optional

import ibis.expr.types.relations as ir
# from catalog.expr.schema import SchemaLike
# from catalog.backends.sql import SQLBackend
from ..base_db_connection import BaseDBConnection
from abc import abstractmethod
from time import sleep

# from abc import abstractmethod
from mountainash_settings import SettingsParameters
from mountainash_dataframes.constants import CONST_DATAFRAME_FRAMEWORK
from ...constants import  CONST_DB_ABSTRACTION_LAYER, CONST_DB_PROVIDER_TYPE

from pyiceberg.table import Table
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog

from mountainash_dataframes import SupportedDataFrames, DataFrameUtils
# from mountainash_dataframes.utils.dataframe_utils import DataFrameUtils

from pyiceberg.schema import Schema

from pyiceberg.types import (
    BooleanType, IntegerType, LongType, FloatType, DoubleType,
    DateType, TimeType, TimestampType, StringType, UUIDType,
    BinaryType, DecimalType, FixedType, ListType, MapType, StructType
)

from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table.sorting import SortOrder



import pyarrow as pa
from datetime import date, datetime


class BasePyIcebergConnection(BaseDBConnection):

    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ):

        super().__init__(db_auth_settings_parameters=db_auth_settings_parameters)

        self._schema_cache = {}

    @property
    def db_abstraction_layer(self) -> CONST_DB_ABSTRACTION_LAYER:
        return CONST_DB_ABSTRACTION_LAYER.PYICEBERG


    @property
    def db_provider_type(self) -> CONST_DB_PROVIDER_TYPE:
        """Database provider identifier."""
        return CONST_DB_PROVIDER_TYPE.PYICEBERG_REST

    @property
    @abstractmethod
    def catalog_backend(self) -> t.Optional[Catalog|t.Any]:
        """Concret Ibis backend connection object."""
        pass

    # @property
    # @abstractmethod
    # def catalog_connection_mode(self) -> str:
    #     """Connect via a connection string, kwargs or both."""
    #     pass

    # @property
    # @abstractmethod
    # def supports_upsert(self) -> str:
    #     """Whether upserts are supported"""
    #     pass


    def get_schema(self, table_name: str|t.Tuple[str], refresh: bool = False) -> t.Optional[Schema]:
        """
        Get schema for a table with caching.

        Args:
            table_name: Name of the table
            refresh: Whether to force refresh the cached schema

        Returns:
            Cached or fresh Schema object, or None if not available
        """
        # Use cached schema if available and refresh not requested
        if not refresh and table_name in self._schema_cache:
            return self._schema_cache[table_name]

        # Get a table reference
        table_ref = self.table(table_name)
        if table_ref is None:
            return None

        # Cache the schema
        try:
            schema = table_ref.schema()
            self._schema_cache[table_name] = schema
            return schema
        except Exception as e:
            print(f"Error getting schema for {table_name}: {e}")
            return None

    def clear_schema_cache(self, table_name: t.Optional[str|t.Tuple[str]] = None):
        """
        Clear schema cache for a specific table or all tables.

        Args:
            table_name: Specific table to clear cache for, or None for all tables
        """
        if table_name:
            if table_name in self._schema_cache:
                del self._schema_cache[table_name]
        else:
            self._schema_cache.clear()


    ###########################
    # Core Functions


    def connect(self,
                connection_string: t.Optional[str] = None,
                connection_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
                **kwargs) -> Catalog:

        """Connect with explicitly provided connection parameters"""

        # if not (connection_string or connection_kwargs):
        #     raise ValueError("Either connection_string or connection_kwargs must be provided")

        if self.catalog_backend is None:

            # self._connect(connection_string=connection_string, connection_kwargs=connection_kwargs, **kwargs)
            # else:
            self.connect_default(**kwargs)

        return self.catalog_backend


    def connect_default(self, **kwargs) -> Catalog:
        """Connect using default configuration"""
        # Implementation for connecting with default parameters

        if self.catalog_backend is None:

            # WAREHOUSE = f"{get_settings(r2_bronze_storage_settings_parameters).ACCOUNT_ID}_{get_settings(r2_bronze_storage_settings_parameters).BUCKET}"
            # CATALOG_URI = f"https://catalog.cloudflarestorage.com/{get_settings(r2_bronze_storage_settings_parameters).ACCOUNT_ID}/{get_settings(r2_bronze_storage_settings_parameters).BUCKET}"
            # CREDENTIAL = f"{get_settings(r2_bronze_storage_settings_parameters).ACCESS_KEY_ID}:{get_settings(r2_bronze_storage_settings_parameters).SECRET_ACCESS_KEY}"
            # TOKEN = get_settings(r2_bronze_storage_settings_parameters).TOKEN

            connection_kwargs = self.get_connection_kwargs()

            self._catalog_backend: RestCatalog = RestCatalog(**connection_kwargs)
            # self._connect(connection_kwargs=connection_kwargs, **kwargs)

        return self.catalog_backend



    def _connect(self,
                 connection_kwargs: t.Optional[t.Dict[str,str]]
                 ) -> Catalog:
        """
        Default Implementation to connect to the database using the provided connection string.
        By default this relies on the connection_string_scheme to determine the backend dynamically.
        Over-ride in subclasses if a different implementation is necessary, such as using the custom backend directly.
        """
        raise NotImplementedError
        # Connect to R2 Data Catalog
        # self._catalog_backend: RestCatalog = RestCatalog(**connection_kwargs)

        # return self.catalog_backend



    def close(self):
        """Close the connection to the database."""

        self.disconnect()

    def disconnect(self):
        """Close the connection to the database."""

        if self.catalog_backend is not None:
            # self.catalog_backend.disconnect()
            self._catalog_backend  = None

    def is_connected(self) -> bool:
        """ Is the connection open?"""
        if self.catalog_backend is None:
            return False
        else:
            return True




    ## SQL Queries

    def run_sql(self,
            query: str,
            schema: Schema | None = None,
            dialect: str | None = None,
        ) -> t.Optional[Table]:

        raise NotImplementedError

        # self.connect()

        # # table = self.table

        # return self.catalog_backend.sql(query=query,
        #                              schema=schema,
        #                              dialect=dialect
        #                  )  if self.catalog_backend is not None else None


    # def run_expr(
    #     self,
    #     catalog_expr: ir.Expr,
    #     params: t.Dict | None = None,
    #     limit: str | None = "default",
    #     **kwargs: t.Any,
    #     ) -> t.Any:

    #     self.connect()

    #     return self.catalog_backend.execute(expr=catalog_expr,
    #                                  params=params,
    #                                  limit=limit,
    #                                  **kwargs
    #                      )  if self.catalog_backend is not None else None


    # def to_sql(
    #     self,
    #     expr: ir.Expr,
    #     params=None,
    #     limit: str | None = None,
    #     pretty: bool = False,
    #     **kwargs: t.Any,
    #     ) -> t.Optional[str]:

    #     self.connect()

    #     return self.catalog_backend.compile(expr=expr,
    #                                  params=params,
    #                                  limit=limit,
    #                                  pretty=pretty,
    #                                  **kwargs
    #                      )  if self.catalog_backend is not None else None



    ## Tables
    def list_namespaces(
        self,
        table_name: t.Optional[str|t.Tuple[str]] = None,
        # schema: str | None = None,
        # database: tuple[str, str] | str | None = None
        ) -> t.Optional[Table]:

        self.connect()

        return self.catalog_backend.list_namespaces(table_name) if self.catalog_backend is not None else None



    # ## Tables
    # def table(
    #     self,
    #     table_name: str|t.Tuple[str],
    #     # schema: str | None = None,
    #     # database: tuple[str, str] | str | None = None
    #     ) -> t.Optional[Table]:

    #     self.connect()

    #     return self.catalog_backend.load_table(table_name) if self.catalog_backend is not None else None


    def table(
        self,
        table_name: str|t.Tuple[str],
        max_attempts: int = 3,
        retry_delay: float = 0.5
    ) -> t.Optional[Table]:
        """
        Get a table reference with built-in retry logic.

        Args:
            table_name: Name of the table to load
            max_attempts: Maximum number of attempts to load the table
            retry_delay: Delay between retry attempts in seconds

        Returns:
            Table reference or None if the table couldn't be loaded after retries
        """
        self.connect()

        for attempt in range(max_attempts):
            table_ref = self.catalog_backend.load_table(table_name)

            # snapshot = table_ref.current_snapshot()

            if table_ref is not None:
                # Successfully got a table reference
                return table_ref

            if attempt < max_attempts - 1:
                # Not the last attempt, so wait and retry
                print(f"Table reference for {table_name} returned None (attempt {attempt+1}/{max_attempts}), retrying...")
                sleep(retry_delay)

        # Failed to get a valid table reference after all attempts
        print(f"Failed to get table reference for {table_name} after {max_attempts} attempts")
        return None


    def create_table(
        self,
        table_name: str|t.Tuple[str],
        schema: Schema,
        df: t.Optional[ir.Table|t.Any] = None,
        # | pd.DataFrame
        # | pa.Table
        # | pl.DataFrame
        # | pl.LazyFrame
        # | None = None,
        location: str | None = None,
        partition_spec: t.Optional[PartitionSpec] = None,
        sort_order: t.Optional[SortOrder] = None,
        overwrite: t.Optional[bool] = False
        ) -> None:
        """Connect to the database using the provided connection string."""

        self.connect()

        #Clean up params, preseving underlying defaults
        params = {
            "identifier": table_name,
            "schema": schema
        }

        # Add optional parameters only if they're not None
        if location is not None:
            params["location"] = location
        if partition_spec is not None:
            params["partition_spec"] = partition_spec
        if sort_order is not None:
            params["sort_order"] = sort_order



        if not overwrite and self.table_exists(table_name):
            print(f"Cannot create table - table already exists: {table_name}. Set overwrite= True.")
            return False


        if overwrite:
            self.catalog_backend.drop_table(table_name)

        obj_table = self.catalog_backend.create_table(**params)  if self.catalog_backend is not None else None

        #Refesh the schema after recreating the table
        self.get_schema(table_name, refresh=True)

        if df is not None:
            pa_df = self.prepare_dataframe_for_iceberg(df, target_schema=schema)
            obj_table.append(pa_df)

        return obj_table


    def drop_table(
        self,
        table_name: str|t.Tuple[str],
        purge: t.Optional[bool] = False
        ) -> bool:

        self.connect()

        try:
            if self.table_exists(table_name):
                if purge:
                    self.catalog_backend.purge_table(table_name) if self.catalog_backend is not None else None
                else:
                    self.catalog_backend.drop_table(table_name) if self.catalog_backend is not None else None
                return True
            return False

        except Exception:
            return False


    # Backend Data Manipulation
    def insert(
        self,
        table_name: str|t.Tuple[str],
        df: ir.Table|t.Any,
        prevent_duplicates: t.Optional[bool] = False
        # overwrite: bool = False,
    ) -> bool:

        #TODO: Support more DataFrames

        self.connect()

        try:

            #Prepare Data
            schema = self.get_schema(table_name)
            pa_df = self.prepare_dataframe_for_iceberg(df, target_schema=schema)

            # Insert
            table = self.table(table_name)

            if prevent_duplicates:
                print("NOTE: insert operation on {table_name} may have prevented duplicates being added to the table. Records matching the existing keys were dropped. If you wish to perfom a full insert set 'prevent_duplicates=False'. If you wish to also update existing rows use upsert()")
                table.upsert(df=pa_df, when_matched_update_all=False, when_not_matched_insert_all=True)  if self.catalog_backend is not None else None
            else:
                print("NOTE: insert operation on {table_name} performing a straight append, whch may cause duplicates to be added to the table. To avoid duplicates 'prevent_duplicates=False'. To update rows that match existing natural keys use upsert()")
                table.append(df=pa_df)  if self.catalog_backend is not None else None

            return True

        except Exception:
            return False



    def truncate(
        self,
        table_name: str|t.Tuple[str],
    ) -> None:

        raise NotImplementedError
        # self.connect()

        # obj_table = self.table(table_name)

        # obj_table.(
        #                             name=table_name,
        #                             schema=schema,
        #                             database=database)  if self.catalog_backend is not None else None


    def upsert(
        self,
        table_name: str|t.Tuple[str],
        df: ir.Table|t.Any,
        natural_key_columns: list[str] | None = None,
        when_matched_update_all: bool= True,
        when_not_matched_insert_all: bool= True,
        case_sensitive: bool= True,
        ) -> None:

        self.connect()

        #Prepare Data
        schema = self.get_schema(table_name)
        pa_df = self.prepare_dataframe_for_iceberg(df, target_schema=schema)

        #Clean up params, preseving underlying defaults
        params = {
            "df": pa_df,
            "when_matched_update_all" : when_matched_update_all,
            "when_not_matched_insert_all" : when_not_matched_insert_all,
            "case_sensitive": case_sensitive
        }

        # Add optional parameters only if they're not None
        if natural_key_columns is not None:
            params["join_cols"] = natural_key_columns


        table = self.table(table_name)
        table.upsert(**params)  if self.catalog_backend is not None else None


        # def do_upsert():

        #     # table = self.table(table_name)
        #     # print(f"upsert. Table: {table.current_snapshot().snapshot_id}")

        #     # table.upsert(**params)  if table is not None else None

        #     table_post = self.table(table_name)

        #     print(f"upsert. Post Table: {table_post.current_snapshot().snapshot_id}")

        # return self.retry_operation(do_upsert)


    def retry_operation(self, operation_func, max_attempts=3):
        """Retry a PyIceberg operation that might fail due to commit conflicts."""

        attempts = 0
        last_error = None

        while attempts < max_attempts:
            try:

                return operation_func()
            except Exception as e:
                attempts += 1
                print(e)
                # last_error = e
                if attempts >= max_attempts:
                    break
                sleep(0.2)  # Add a small delay between attempts

        raise last_error if last_error else RuntimeError("Operation failed for unknown reason")

    # @abstractmethod
    # def _upsert(
    #     self,
    #     table_name: str,
    #     df: ir.Table|t.Any,
    #     database: str | None = None,
    #     schema: str | None = None,
    #     natural_key_columns: list[str] | None = None,
    #     data_columns: list[str] | None = None
    #     ) -> None:

    #     raise NotImplementedError(f"{self.db_backend_name}: Upsert is not implemented for this backend")




    def prepare_dataframe_for_iceberg(self,
                                        df: SupportedDataFrames,
                                        target_schema: t.Optional[Schema] = None,
                                        target_table: t.Optional[Table] = None):
        """
        Preprocess a PyArrow dataframe to match Iceberg table schema requirements

        Args:
            df: PyArrow table to be processed
            table: Iceberg table to extract schema from

        Returns:
            PyArrow table with schema matching the Iceberg table
        """

        if target_schema is None and target_table is None:
            return DataFrameUtils.to_pyarrow(df=df)

        if target_schema is None and target_table is not None:
            target_schema = target_table.schema()

        df = DataFrameUtils.to_pyarrow(df=df)

        pa_schema = pa.schema([])

        # Build a compatible PyArrow schema based on Iceberg schema
        for field in target_schema.fields:
            iceberg_type = field.field_type
            iceberg_nullable = not field.required

            # Map Iceberg types to PyArrow types
            if isinstance(iceberg_type, BooleanType):
                pa_type = pa.bool_()
            elif isinstance(iceberg_type, IntegerType):
                pa_type = pa.int32()
            elif isinstance(iceberg_type, LongType):
                pa_type = pa.int64()
            elif isinstance(iceberg_type, FloatType):
                pa_type = pa.float32()
            elif isinstance(iceberg_type, DoubleType):
                pa_type = pa.float64()
            elif isinstance(iceberg_type, DateType):
                pa_type = pa.date32()
            elif isinstance(iceberg_type, TimeType):
                pa_type = pa.time64('us')
            elif isinstance(iceberg_type, TimestampType):
                if iceberg_type.with_timezone:
                    pa_type = pa.timestamp('us', tz='UTC')
                else:
                    pa_type = pa.timestamp('us')
            elif isinstance(iceberg_type, StringType):
                pa_type = pa.string()
            elif isinstance(iceberg_type, UUIDType):
                pa_type = pa.string()  # UUIDs are usually handled as strings in PyArrow
            elif isinstance(iceberg_type, BinaryType):
                pa_type = pa.binary()
            elif isinstance(iceberg_type, DecimalType):
                pa_type = pa.decimal128(iceberg_type.precision, iceberg_type.scale)
            elif isinstance(iceberg_type, FixedType):
                pa_type = pa.binary(iceberg_type.length)
            elif isinstance(iceberg_type, ListType):
                # Recursive call for list element type
                inner_type = iceberg_type.element_type
                if isinstance(inner_type, BooleanType):
                    pa_element_type = pa.bool_()
                elif isinstance(inner_type, IntegerType):
                    pa_element_type = pa.int32()
                elif isinstance(inner_type, LongType):
                    pa_element_type = pa.int64()
                elif isinstance(inner_type, FloatType):
                    pa_element_type = pa.float32()
                elif isinstance(inner_type, DoubleType):
                    pa_element_type = pa.float64()
                elif isinstance(inner_type, StringType):
                    pa_element_type = pa.string()
                else:
                    pa_element_type = pa.string()  # Default fallback for complex types
                pa_type = pa.list_(pa_element_type)
            elif isinstance(iceberg_type, MapType):
                # For maps, we default to string keys and values
                # This could be expanded to handle specific key-value type mappings
                pa_type = pa.map_(pa.string(), pa.string())
            elif isinstance(iceberg_type, StructType):
                # For structs, we create a nested field structure
                struct_fields = []
                for nested_field in iceberg_type.fields:
                    # Simplified handling - you might want to expand this
                    struct_fields.append(pa.field(nested_field.name, pa.string(),
                                                nullable=not nested_field.required))
                pa_type = pa.struct(struct_fields)
            else:
                pa_type = pa.string()  # Default fallback

            pa_schema = pa_schema.append(pa.field(field.name, pa_type, nullable=iceberg_nullable))

        # Cast the dataframe to the new schema
        cast_arrays = []
        for i, field in enumerate(pa_schema):
            # Handle potential missing columns by adding nulls
            if field.name in df.column_names:
                col_index = df.column_names.index(field.name)

                # Special handling for data type conversions that might need preprocessing
                if pa.types.is_timestamp(field.type) and not pa.types.is_timestamp(df.column(col_index).type):
                    # Convert string to timestamp if needed
                    try:
                        # This is a simplified approach - may need more robust datetime parsing
                        values = df.column(col_index).to_pylist()
                        converted = [datetime.fromisoformat(str(v)) if v is not None else None for v in values]
                        cast_arrays.append(pa.array(converted, type=field.type))
                    except Exception:
                        # Fallback to regular casting
                        cast_arrays.append(df.column(col_index).cast(field.type, safe=True))
                elif pa.types.is_date(field.type) and not pa.types.is_date(df.column(col_index).type):
                    # Convert string to date if needed
                    try:
                        values = df.column(col_index).to_pylist()
                        converted = [date.fromisoformat(str(v)) if v is not None else None for v in values]
                        cast_arrays.append(pa.array(converted, type=field.type))
                    except Exception:
                        cast_arrays.append(df.column(col_index).cast(field.type, safe=True))
                elif pa.types.is_decimal(field.type):
                    # Decimal requires special handling
                    try:
                        values = df.column(col_index).to_pylist()
                        from decimal import Decimal
                        converted = [Decimal(str(v)) if v is not None else None for v in values]
                        cast_arrays.append(pa.array(converted, type=field.type))
                    except Exception:
                        cast_arrays.append(df.column(col_index).cast(field.type, safe=True))
                else:
                    # Standard casting for other types
                    cast_arrays.append(df.column(col_index).cast(field.type, safe=True))
            else:
                # Create empty column for missing fields
                cast_arrays.append(pa.array([None] * len(df), type=field.type))

        return pa.Table.from_arrays(cast_arrays, schema=pa_schema)



    ## Views
    def create_view(
        self,
        view_name: str|t.Tuple[str],
        ) -> t.Optional[Table]:

        raise NotImplementedError


    def drop_view(
        self,
        view_name: str|t.Tuple[str],
    ) -> bool:

        self.connect()

        try:
            if self.view_exists(view_name):
                self.catalog_backend.drop_view(view_name)  if self.catalog_backend is not None else None
                return True
            return False

        except Exception:
            return False

    def view_exists(self,
                    view_name: str|t.Tuple[str] = None,
                    ) -> bool:

        self.connect()

        return self.catalog_backend.view_exists(view_name)  if self.catalog_backend is not None else None


    ###########################
    # t.Optionally Implemented Functions

    def list_tables(self,
                # table_name: str | None = None,
                namespace: tuple[str, str] | str | None = None,
                    ) -> t.List[str]:

        self.connect()
        return self._list_tables(namespace=namespace)

    def _list_tables(self,
                namespace: str | None = None,
                # database: t.Any = None,
                # schema: str | None = None
                    ) -> t.List[str]:

        raise NotImplementedError


    def rename_table(self,
                old_name: str,
                new_name: str,
                ) -> None:

        self.connect()
        return self._rename_table(old_name=old_name, new_name=new_name)  if self.catalog_backend is not None else None


    def _rename_table(self,
                old_name: str,
                new_name: str,
                ) -> None:

        raise NotImplementedError


    def table_exists(self,
                    table_name: str|t.Tuple[str] = None,
                    ) -> bool:

        self.connect()

        return self.catalog_backend.table_exists(table_name)  if self.catalog_backend is not None else None





    # @abstractmethod
    # def list_databases(self, like:  t.Optional[str]= None, database: t.Optional[str]= None) -> list[str]:
    #     """Connect to the database using the provided connection string."""

    #     pass

    ###########################
    # Mountain Ash Abstractions
    def table_as_ibis_dataframe(self,
        table_name: str,
        # schema: str | None = None,
        # database: tuple[str, str] | str | None = None,
        tablename_prefix: t.Optional[str] = None

        ) -> t.Optional[SupportedDataFrames]:

        """Get a table or view as a DataFrame."""

        result: Table | None = self.table(table_name=table_name )

        return DataFrameUtils.to_ibis(result)


    def table_as_polars_dataframe(self,
        table_name: str,
        # schema: str | None = None,
        # database: tuple[str, str] | str | None = None,
        tablename_prefix: t.Optional[str] = None

        ) -> t.Optional[SupportedDataFrames]:

        """Get a table or view as a DataFrame."""

        result: Table | None = self.table(table_name=table_name )

        return DataFrameUtils.to_polars(result)

    # def run_sql_as_ibis_dataframe(self,
    #         query: str,
    #         schema: Schema | None = None,
    #         dialect: str | None = None,
    #         tablename_prefix: t.Optional[str] = None
    #         ) -> t.Optional[BaseDataFrame]:
    #     """Execute the given SQL statement."""

    #     result: t.Optional[ir.Table] = self.run_sql(query=query,
    #                                               schema=schema,
    #                                               dialect=dialect
    #                                               )

    #     return IbisDataFrame(df=result,
    #                         catalog_backend=self.catalog_backend,
    #                         tablename_prefix=tablename_prefix)


    # def run_expr_as_ibis_dataframe(self,
    #         catalog_expr: ir.Expr,
    #         params: t.Dict | None = None,
    #         limit: str | None = "default",
    #         tablename_prefix: t.Optional[str] = None,
    #         **kwargs: t.Any,
    #         ) -> t.Optional[BaseDataFrame]:
    #     """Execute the given catalog expression statement."""

    #     result: t.Optional[ir.Table] = self.run_expr(catalog_expr=catalog_expr,
    #                                               params=params,
    #                                               limit=limit,
    #                                               **kwargs
    #                                               )
    #### Native Dataframe

    def table_as_native_dataframe(self,
        object_name: str,
        schema: str | None = None,
        database: tuple[str, str] | str | None = None,
        dataframe_framework: t.Optional[str] = CONST_DATAFRAME_FRAMEWORK.POLARS

        ) -> t.Optional[SupportedDataFrames]:

        """Get a table or view as a DataFrame."""

        result: Table | None = self.table(object_name,
                                            #    schema=schema,
                                            #    database=database
                                               )
        if result is None:
            return None

        if dataframe_framework is None:
            dataframe_framework = CONST_DATAFRAME_FRAMEWORK.POLARS

        return DataFrameUtils.cast_dataframe(result, dataframe_framework=dataframe_framework)


    # def run_sql_as_native_dataframe(self,
    #         query: str,
    #         schema: Schema | None = None,
    #         dialect: str | None = None,
    #         dataframe_framework: t.Optional[str] = CONST_DATAFRAME_FRAMEWORK.POLARS
    #         ) -> t.Optional[BaseDataFrame]:
    #     """Execute the given SQL statement."""

    #     result: t.Optional[ir.Table] = self.run_sql(query=query,
    #                                               schema=schema,
    #                                               dialect=dialect
    #                                               )

    #     return DataFrameUtils.cast_dataframe(df=result, dataframe_framework=dataframe_framework)


    # def run_expr_as_materialised_dataframe(self,
    #         catalog_expr: ir.Expr,
    #         params: t.Dict | None = None,
    #         limit: str | None = "default",
    #         dataframe_framework: t.Optional[str] = CONST_DATAFRAME_FRAMEWORK.POLARS,
    #         **kwargs: t.Any,
    #         ) -> t.Optional[BaseDataFrame]:
    #     """Execute the given catalog expression statement."""

    #     result: t.Optional[ir.Table] = self.run_expr(catalog_expr=catalog_expr,
    #                                               params=params,
    #                                               limit=limit,
    #                                               **kwargs
    #                                               )

    #     return DataFrameUtils.cast_dataframe(df=result, dataframe_framework=dataframe_framework)
