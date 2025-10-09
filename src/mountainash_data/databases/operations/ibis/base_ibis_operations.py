import typing as t # import t.Any, t.Dict, t.Optional

from abc import abstractmethod, ABC

import ibis
import ibis.expr.types.relations as ir
from ibis.expr.schema import SchemaLike
from ibis.backends.sql import SQLBackend
# from mountainash_data.databases.base_db_connection import BaseDBConnection

# from abc import abstractmethod
from mountainash_settings import SettingsParameters

from ...constants import (
    CONST_DB_BACKEND,
    CONST_CONFLICT_ACTION
)
# from mountainash_dataframes import BaseDataFrame, IbisDataFrame


class BaseIbisOperations(ABC):

    def __init__(self,
                 db_auth_settings_parameters: SettingsParameters,
                 ):
        ...


    ## SQL Queries

    @property
    @abstractmethod
    def db_backend_name(self) -> str:
        return CONST_DB_BACKEND.DUCKDB


    @classmethod
    def run_sql(cls,
            ibis_backend: SQLBackend,
            query: str,
            /,
            *,
            schema: SchemaLike | None = None,
            dialect: str | None = None,
        ) -> t.Optional[ir.Table]:

        try:

            return ibis_backend.sql(query,
                                        schema=schema,
                                        dialect=dialect
                            )

        except Exception as e:
            print(f"Error executing SQL: {e}")
            return None



    @classmethod
    def run_expr(
        cls,
        ibis_backend: ibis.BaseBackend,
        ibis_expr: ir.Expr,
        /,
        params: t.Dict | None = None,
        limit: str | None = "default",
        **kwargs: t.Any,
        ) -> t.Any:

        try:
            return ibis_backend.execute(ibis_expr,
                                        params=params,
                                        limit=limit,
                                        **kwargs
                            )  if ibis_backend is not None else None
        except Exception as e:
            print(f"Error executing expression: {e}")
            return None

    @classmethod
    def to_sql(
        cls,
        ibis_backend: ibis.BaseBackend,
        expr: ir.Expr,
        /,
        params=None,
        limit: str | None = None,
        pretty: bool = False,
        **kwargs: t.Any,
        ) -> t.Optional[str]:

        try:

            return ibis_backend.compile(expr,
                                        params=params,
                                        limit=limit,
                                        pretty=pretty,
                                        **kwargs
                            )  if ibis_backend is not None else None
        except Exception as e:
            print(f"Error compiling expression to SQL: {e}")
            return None

    ## Tables
    @classmethod
    def table(
        cls,
        ibis_backend: ibis.BaseBackend,
        object_name: str,
        /,
        schema: str | None = None,
        database: tuple[str, str] | str | None = None
        ) -> t.Optional[ir.Table]:

        try:

            return ibis_backend.table(object_name,
                                        #    schema=schema,
                                        database=database
                                        ) if ibis_backend is not None else None
        except Exception as e:
            print(f"Error getting table {object_name}: {e}")
            return None

    @classmethod
    def create_table(cls,
                    ibis_backend: ibis.BaseBackend,
                     table_name: str,
                     df: ir.Table|t.Any,
                     /,
                     schema: t.Optional[ibis.Schema] = None,
                     database: str | None = None,
                     temp: bool = False,
                     overwrite: bool = False,
                     ) -> None:
        """Connect to the database using the provided connection string."""

        #TODO: set a flag to load data. Default is true, if not use the schema.
        # if schema is None:
        #     schema = DataFrameUtils.get_table_schema(df)
        #TODO: Use DataFrameUtils
        try:

            ibis_backend.create_table(table_name,
                                        obj=df,
                                        schema=schema,
                                        database=database,
                                        temp=temp,
                                        overwrite=overwrite)  if ibis_backend is not None else None
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")
            return None

    @classmethod
    def drop_table(
        cls,
        ibis_backend: ibis.BaseBackend,
        table_name: str,
        /,
        database:  str | None = None, #t.Tuple[str, str]
        force: bool = False,
        ) -> bool:

        try:
            ibis_backend.drop_table(table_name,
                                        database=database,
                                        force=force)   if ibis_backend is not None else None
            return True
        except Exception:
            return False



    ## Views
    @classmethod
    def create_view(
        cls,
        ibis_backend: ibis.BaseBackend,
        view_name: str,
        ibis_table_expr: ir.Table,
        /,
        database: str | None = None,
        schema: str | None = None,
        overwrite: bool = False,
        ) -> t.Optional[ir.Table]:

        try:
            return ibis_backend.create_view(view_name,
                                    obj=ibis_table_expr,
                                    database=database,
                                    # schema=schema,
                                    overwrite=overwrite)  if ibis_backend is not None else None

        except Exception as e:
            print(f"Error creating view {view_name}: {e}")
            return None

    @classmethod
    def drop_view(
        cls,
        ibis_backend: ibis.BaseBackend,
        view_name: str,
        /,
        database: str | None = None,
        schema: str | None = None,
        force: bool = False,
    ) -> bool:


        try:

            ibis_backend.drop_view(view_name,
                                    database=database,
                                    # schema=schema,
                                    force=force)  if ibis_backend is not None else None
            return True
        except Exception:
            print(f"Error dropping view {view_name}")
            return False


    # Backend Data Manipulation

    @classmethod
    def insert(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        /,
        df: ir.Table|t.Any,
        database: str | None = None,
        schema: str | None = None,
        overwrite: bool = False,
    ) -> bool:

        #TODO: Support more DataFrames

        try:
            ibis_backend.insert(   table_name,
                                    obj=df,
                                    database=database,
                                    overwrite=overwrite)  if ibis_backend is not None else None
            return True
        except Exception as e:
            print(f"Error inserting into table {table_name}: {e}")
            return False



    @classmethod
    def truncate(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        /,
        database: str | None = None,
        schema: str | None = None
    ) -> None:

        try:
            ibis_backend.truncate_table(
                                        table_name,
                                        schema=schema,
                                        database=database)  if ibis_backend is not None else None

        except Exception as e:
            print(f"Error truncating table {table_name}: {e}")
            return None

    @classmethod
    def upsert(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        df: ir.Table|t.Any,
        /,
        conflict_columns: list[str] | str,
        update_columns: list[str] | str | None = None,
        conflict_action: str = CONST_CONFLICT_ACTION.UPDATE,
        update_condition: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        ) -> None:
        """
        Perform an upsert (INSERT ON CONFLICT) operation.

        Args:
            ibis_backend: The Ibis backend connection
            table_name: Target table name
            df: Data to upsert (Ibis table or compatible dataframe)
            conflict_columns: Columns to check for conflicts (ON CONFLICT)
            update_columns: Columns to update on conflict (None = all except conflict_columns)
            conflict_action: Action on conflict - UPDATE or NOTHING
            update_condition: Optional WHERE clause for conditional updates
            database: Optional database name
            schema: Optional schema name
        """

        try:

            cls._upsert(
                ibis_backend,
                table_name,
                df,
                conflict_columns=conflict_columns,
                update_columns=update_columns,
                conflict_action=conflict_action,
                update_condition=update_condition,
                database=database,
                schema=schema)  if ibis_backend is not None else None

        except Exception as e:
            print(f"Error upserting into table {table_name}: {e}")
            raise


    @classmethod
    @abstractmethod
    def _upsert(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        df: ir.Table|t.Any,
        *,
        conflict_columns: list[str] | str,
        update_columns: list[str] | str | None = None,
        conflict_action: str = CONST_CONFLICT_ACTION.UPDATE,
        update_condition: str | None = None,
        database: str | None = None,
        schema: str | None = None,
    ) -> None:
        """
        Provider-specific upsert implementation.

        Scenarios supported:
        1. Simple upsert: conflict_columns + update_columns
        2. Insert-ignore: conflict_action="NOTHING"
        3. Conditional update: update_condition specified
        4. Auto-detect columns: update_columns=None
        """

        raise NotImplementedError(f"{cls.db_backend_name}: Upsert is not implemented for this backend")


    # ===========================
    # INDEX MANAGEMENT
    # ===========================

    @classmethod
    @abstractmethod
    def create_index(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        columns: list[str] | str,
        *,
        index_name: str | None = None,
        unique: bool = False,
        index_type: str | None = None,
        where_condition: str | None = None,
        database: str | None = None,
        if_not_exists: bool = True
    ) -> bool:
        """
        Create an index on specified columns.

        Args:
            ibis_backend: The Ibis backend connection
            table_name: Target table name
            columns: Column(s) to index
            index_name: Optional custom index name (auto-generated if None)
            unique: Create unique index
            index_type: Index type (BTREE, HASH, etc.) - support varies by database
            where_condition: WHERE clause for partial indexes
            database: Optional database name
            if_not_exists: Use IF NOT EXISTS clause

        Returns:
            True if successful, False otherwise
        """
        raise NotImplementedError(f"{cls.db_backend_name}: create_index is not implemented for this backend")

    @classmethod
    def create_unique_index(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        columns: list[str] | str,
        *,
        index_name: str | None = None,
        where_condition: str | None = None,
        database: str | None = None
    ) -> bool:
        """
        Convenience method for creating unique indexes.

        Args:
            ibis_backend: The Ibis backend connection
            table_name: Target table name
            columns: Column(s) to index
            index_name: Optional custom index name
            where_condition: WHERE clause for partial indexes
            database: Optional database name

        Returns:
            True if successful, False otherwise
        """
        return cls.create_index(
            ibis_backend, table_name, columns,
            index_name=index_name, unique=True,
            where_condition=where_condition, database=database
        )

    @classmethod
    @abstractmethod
    def drop_index(
        cls,
        ibis_backend: SQLBackend,
        index_name: str,
        *,
        table_name: str | None = None,
        database: str | None = None,
        if_exists: bool = True
    ) -> bool:
        """
        Drop an index.

        Args:
            ibis_backend: The Ibis backend connection
            index_name: Name of index to drop
            table_name: Optional table name (required by some databases)
            database: Optional database name
            if_exists: Use IF EXISTS clause

        Returns:
            True if successful, False otherwise
        """
        raise NotImplementedError(f"{cls.db_backend_name}: drop_index is not implemented for this backend")

    @classmethod
    @abstractmethod
    def index_exists(
        cls,
        ibis_backend: SQLBackend,
        index_name: str,
        *,
        table_name: str | None = None,
        database: str | None = None
    ) -> bool:
        """
        Check if an index exists.

        Args:
            ibis_backend: The Ibis backend connection
            index_name: Name of index to check
            table_name: Optional table name for narrower search
            database: Optional database name

        Returns:
            True if index exists, False otherwise
        """
        raise NotImplementedError(f"{cls.db_backend_name}: index_exists is not implemented for this backend")

    @classmethod
    @abstractmethod
    def list_indexes(
        cls,
        ibis_backend: SQLBackend,
        table_name: str,
        *,
        database: str | None = None
    ) -> list[dict]:
        """
        List all indexes for a table.

        Args:
            ibis_backend: The Ibis backend connection
            table_name: Target table name
            database: Optional database name

        Returns:
            List of dicts with keys: name, columns, unique, type
        """
        raise NotImplementedError(f"{cls.db_backend_name}: list_indexes is not implemented for this backend")


    ###########################
    # t.Optionally Implemented Functions

    @classmethod
    def list_tables(cls,
                ibis_backend: ibis.BaseBackend,
                table_name: str | None = None,
                database: tuple[str, str] | str | None = None
                    ) -> t.List[str]:

        try:
            return ibis_backend.list_tables(like=table_name, database=database) if ibis_backend is not None else []
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    # @classmethod
    # @abstractmethod
    # def _list_tables(cls,
    #             ibis_backend: ibis.BaseBackend,
    #             like: str | None = None,
    #             database: t.Any = None
    #             # schema: str | None = None
    #                 ) -> t.List[str]:

    #     try:
    #         return ibis_backend.list_tables(like=like, database=database) if ibis_backend is not None else []
    #     except Exception as e:
    #         print(f"Error listing tables: {e}")
    #         return []

    @classmethod
    def rename_table(cls,
                ibis_backend: ibis.BaseBackend,
                old_name: str,
                new_name: str,
                ) -> None:

        try:

            return cls._rename_table(old_name=old_name, new_name=new_name)  if ibis_backend is not None else None
        except Exception as e:
            print(f"Error renaming table {old_name} to {new_name}: {e}")
            return None

    @classmethod
    @abstractmethod
    def _rename_table(cls,
                old_name: str,
                new_name: str,
                ) -> None:

        raise NotImplementedError


    @classmethod
    def table_exists(cls,
                ibis_backend: ibis.BaseBackend,
                table_name: str | None = None,
                database: tuple[str, str] | str | None = None
                    ) -> bool:

        tables = cls.list_tables(ibis_backend, table_name=table_name, database=database)

        return True if table_name in tables else False


    # @abstractmethod
    # def list_databases(self, like:  t.Optional[str]= None, database: t.Optional[str]= None) -> list[str]:
    #     """Connect to the database using the provided connection string."""

    #     pass

    ###########################
    # Mountain Ash Abstractions
    # @classmethod
    # def table_to_ibis(cls,
    #     ibis_backend: ibis.BaseBackend,
    #     object_name: str,
    #     schema: str | None = None,
    #     database: tuple[str, str] | str | None = None,
    #     tablename_prefix: t.Optional[str] = None

    #     ) -> t.Optional[ir.Table]:

    #     """Get a table or view as a DataFrame."""

    #     result: ibis.Table | None = self.table(ibis_backend,
    #                                            object_name=object_name,
    #                                            schema=schema,
    #                                            database=database)

    #     return result

    # @classmethod
    # def run_sql_as_ibis_dataframe(self,
    #         ibis_backend: SQLBackend,
    #         query: str,
    #         schema: SchemaLike | None = None,
    #         dialect: str | None = None,
    #         tablename_prefix: t.Optional[str] = None
    #         ) -> t.Optional[ir.Table]:
    #     """Execute the given SQL statement."""

    #     result: t.Optional[ir.Table] = self.run_sql(ibis_backend,
    #                                                query,
    #                                               schema=schema,
    #                                               dialect=dialect
    #                                               )

    #     return result

    # @classmethod
    # def run_expr_as_ibis_dataframe(self,
    #         ibis_backend: SQLBackend,
    #         ibis_expr: ir.Expr,
    #         params: t.Dict | None = None,
    #         limit: str | None = "default",
    #         tablename_prefix: t.Optional[str] = None,
    #         **kwargs: t.Any,
    #         ) -> t.Optional[ir.Table]:
    #     """Execute the given ibis expression statement."""

    #     result: t.Optional[ir.Table] = self.run_expr(ibis_backend,
    #                                               ibis_expr=ibis_expr,
    #                                               params=params,
    #                                               limit=limit,
    #                                               **kwargs
    #                                               )

    #     return result

    #### Native Dataframe

    # @classmethod
    # def table_as_native_dataframe(self,
    #     ibis_backend: SQLBackend,
    #     object_name: str,
    #     schema: str | None = None,
    #     database: tuple[str, str] | str | None = None,
    #     dataframe_framework: t.Optional[str] = CONST_DATAFRAME_FRAMEWORK.POLARS

    #     ) -> t.Optional[IbisDataFrame]:

    #     """Get a table or view as a DataFrame."""

    #     result: ibis.Table | None = ibis_backend.table(object_name=object_name,
    #                                            schema=schema,
    #                                            database=database)

    #     return DataFrameUtils.cast_dataframe(result, dataframe_framework=dataframe_framework)


    # @classmethod
    # def run_sql_as_native_dataframe(self,
    #         ibis_backend: SQLBackend,
    #         query: str,
    #         schema: SchemaLike | None = None,
    #         dialect: str | None = None,
    #         dataframe_framework: t.Optional[str] = CONST_DATAFRAME_FRAMEWORK.POLARS
    #         ) -> t.Optional[ir.Table]:
    #     """Execute the given SQL statement."""

    #     result: t.Optional[ir.Table] = self.run_sql(query=query,
    #                                               schema=schema,
    #                                               dialect=dialect
    #                                               )

    #     return DataFrameUtils.cast_dataframe(df=result, dataframe_framework=dataframe_framework)


    # @classmethod
    # def run_expr_as_materialised_dataframe(self,
    #         ibis_backend: SQLBackend,
    #         ibis_expr: ir.Expr,
    #         params: t.Dict | None = None,
    #         limit: str | None = "default",
    #         dataframe_framework: t.Optional[str] = CONST_DATAFRAME_FRAMEWORK.POLARS,
    #         **kwargs: t.Any,
    #         ) -> t.Optional[ir.Table]:
    #     """Execute the given ibis expression statement."""

    #     result: t.Optional[ir.Table] = self.run_expr(ibis_expr=ibis_expr,
    #                                               params=params,
    #                                               limit=limit,
    #                                               **kwargs
    #                                               )

    #     return DataFrameUtils.cast_dataframe(result, dataframe_framework=dataframe_framework)
