from typing import Union, List, Dict, Any, Optional, Tuple
import typing as t

import pandas as pd
from mountainash_data.dataframes.utils.dataframe_utils import DataFrameUtils
import polars as pl
import pyarrow as pa
import ibis as ibis
import ibis.expr.types as ir
import ibis.expr.schema as ibis_schema
from ibis.expr.schema import Schema

from ibis.expr.types.groupby import GroupedTable, GroupedArray, GroupedNumbers
from ibis.expr.types.temporal_windows import WindowedTable


import uuid

from ibis.formats.pyarrow import PyArrowSchema
from ibis.formats.polars import PolarsSchema
from ibis.formats.pandas import PandasSchema

from mountainash_data.dataframes.utils.dataframe_filters import FilterCondition
from mountainash_constants import CONST_DATAFRAME_FRAMEWORK

# import uuid
from .base_dataframe import BaseDataFrame
from ..dataframes.utils.ibis_utils import init_ibis_connection, get_default_ibis_backend_schema

# @lru_cache(maxsize=None)
# def init_ibis_connection(ibis_schema: Optional[str] = None) -> ibis.BaseBackend:
#     return ibis.connect(resource=f"{ibis_schema}://")


class IbisDataFrame(BaseDataFrame):
    """Unified dataframe interface supporting multiple backends via Ibis.

    IbisDataFrame provides a consistent API for dataframe operations across
    different backends (SQLite, DuckDB, PostgreSQL, SQL Server, etc.) while maintaining
    compatibility with pandas, polars, and pyarrow data structures.

    This class wraps Ibis tables and provides automatic backend resolution,
    type conversion, and cross-backend join operations. It abstracts the complexity
    of working with different data processing backends through a unified interface.

    Args:
        df: Input dataframe in pandas, polars, pyarrow, or Ibis format.
        ibis_backend: Optional Ibis backend connection. If not provided,
            uses the default backend based on configuration.
        ibis_backend_schema: Backend type ('duckdb', 'sqlite', 'postgres', etc.).
            Defaults to the system default if not specified.
        tablename_prefix: Optional prefix for generated table names.
        create_as_view: Whether to create tables as views (default: False).
        df_grouped: Optional grouped table state from previous operations.
        df_windowed: Optional windowed table state from previous operations.

    Attributes:
        ibis_df: The underlying Ibis table.
        ibis_backend: The active Ibis backend connection.
        ibis_backend_schema: The backend type string.
        default_ibis_backend_schema: System default backend type.
        ibis_df_grouped: Grouped table state if applicable.
        ibis_df_windowed: Windowed table state if applicable.

    Examples:
        >>> # Create from pandas DataFrame
        >>> import pandas as pd
        >>> df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        >>> ibis_df = IbisDataFrame(df)

        >>> # Cross-backend join
        >>> left = IbisDataFrame(df1, ibis_backend_schema='duckdb')
        >>> right = IbisDataFrame(df2, ibis_backend_schema='sqlite')
        >>> result = left.inner_join(right, predicates=['id'])

        >>> # Convert between formats
        >>> polars_df = ibis_df.materialise('polars')
        >>> pandas_df = ibis_df.to_pandas()
    """

    def __init__(self,
                 df:                    Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
                 ibis_backend:          Optional[ibis.BaseBackend] = None,
                 ibis_backend_schema:   Optional[str] = None,
                 tablename_prefix:      Optional[str] = None,
                 create_as_view:        Optional[bool] = False,
                 df_grouped:            Optional[GroupedTable] = None,
                 df_windowed:           Optional[WindowedTable|GroupedArray|GroupedNumbers] = None,
                #lineage_history: Optional[List[Any]] = None,
                 ) -> None:

        super().__init__(df=df)


        #default ibis schema. Schema is the type of backend, eg "polars", "duckdb", "postgres"
        self.default_ibis_backend_schema: Optional[str] = None
        self.init_default_ibis_backend_schema()

        #Init the dataframe. What Am I doing with a pyarrow table here?
        self.ibis_df: ir.Table = self.init_ibis_table(df=df,
                                                      tablename_prefix=tablename_prefix,
                                                      create_as_view=create_as_view,
                                                      ibis_backend=ibis_backend,
                                                      ibis_backend_schema=ibis_backend_schema)



        self.ibis_df_grouped: Optional[GroupedTable] = df_grouped
        self.ibis_df_windowed: Optional[WindowedTable|GroupedArray|GroupedNumbers] = df_windowed


        #Get the column names
        self._column_names = set(self.get_column_names())

    # =======
    # NEP-0018 properties

    # @property
    # def columns(self):
    #     return self.ibis_df.columns

    # @property
    # def shape(self):
    #     return self.ibis_df.shape


    # =======
    # PEP-3118 properties
    # @property
    # def __array__(self, dtype=None):
    #     """NumPy array interface"""
    #     return self.ibis_df.__array__(dtype)

    # @property
    # def __array_interface__(self):
    #     """Low-level numpy array interface"""
    #     return self.ibis_df.__array_interface__

    # =======
    # REduction Operations -maybe...
    # def sum(self, axis=None, skipna=True):
    #     return self.ibis_df.sum(axis=axis, skipna=skipna)

    # def mean(self, axis=None, skipna=True):
    #     return self.ibis_df.mean(axis=axis, skipna=skipna)

    # # Other common reductions
    # def min(self): pass
    # def max(self): pass
    # def std(self): pass
    # def var(self): pass


    #========
    # Newer Python Features (PEP 584 - Dict Union)
    # def __or__(self, other):
    #     """Support for | operator for combining dataframes"""
    #     if not isinstance(other, (MountainAshDataFrame, type(self.ibis_df))):
    #         return NotImplemented
    #     other_df = other.ibis_df if isinstance(other, MountainAshDataFrame) else other
    #     return type(self)(self.ibis_df | other_df)


    # =======
    # # Pandas interface
    # def iloc(self):  # integer-location based indexing
    #     return self.ibis_df.iloc

    # def loc(self):   # label-location based indexing
    #     return self.ibis_df.loc

    # Boolean indexing
    # def __bool__(self):
    #     raise ValueError(
    #         "The truth value of a DataFrame is ambiguous. Use bool(), all() or any()"
    #     )

    # Iterator protocol
    def __iter__(self):
        """Enable iteration over the dataframe rows.

        Returns:
            Iterator over the rows of the underlying Ibis table.
        """
        return iter(self.ibis_df)

    # Length protocol
    def __len__(self):
        """Return the number of rows in the dataframe.

        Returns:
            int: Number of rows in the dataframe.
        """
        return len(self.ibis_df)

    # String representation
    def __repr__(self):
        """Return a detailed string representation of the dataframe.

        Returns:
            str: String representation including the underlying Ibis table.
        """
        return f"MountainAshDataFrame({repr(self.ibis_df)})"

    def __str__(self):
        """Return a human-readable string representation of the dataframe.

        Returns:
            str: String representation of the underlying Ibis table.
        """
        return str(self.ibis_df)



    # =========
    # Column access - Dot notation and array access

    def __getattr__(self, name):
        """Handle dot notation access for columns and methods.

        Provides access to columns via dot notation (e.g., df.column_name) and
        delegates method calls to the underlying Ibis table. For methods that
        return tables, automatically wraps the result in a new IbisDataFrame.

        Args:
            name: The attribute or column name to access.

        Returns:
            Column data if name matches a column, wrapped method result if
            name matches a method, or the attribute value otherwise.

        Raises:
            AttributeError: If the attribute is not found on the dataframe,
                grouped table, windowed table, or underlying Ibis table.
        """
        # First check if the attribute exists on self
        if name in self.__dict__:
            return self.__dict__[name]

        # If not, try to access it on the ibis dataframe

        # If the name matches a column, return that column
        if name in self._column_names:
            return self.ibis_df[name]

        #TODO: Handle GroupedTables and WindowedTables
        # Possibly at this level. Will also need to be able to pass them through to the constructor
        # If these exists, and not on these, should it fall back to the ibis dataframe? Or raise an error?

        if self.ibis_df_grouped is not None and hasattr(self.ibis_df_grouped, name):
            #handle grouped tables actions
            # pass
            #Get the attribute/method from the ibis dataframe
            attr = getattr(self.ibis_df_grouped, name)

        elif self.ibis_df_windowed is not None and hasattr(self.ibis_df_windowed, name):
            #handle windowed tables actions
            # pass
            #Get the attribute/method from the ibis dataframe
            attr = getattr(self.ibis_df_windowed, name)

        #if the name matches an attribute of the ibis dataframe
        elif hasattr(self.ibis_df, name):

            if self.ibis_df_grouped is not None:
                print(f"Grouped table '{self.ibis_df_grouped}' does not have attribute or method '{name}'. Applying to the underlying ibis table. The result may not be what you expected.")

            if self.ibis_df_windowed is not None:
                print(f"Windowed table '{self.ibis_df_windowed}' does not have attribute or method '{name}'. Applying to the underlying ibis table. The result may not be what you expected.")

            #Get the attribute/method from the ibis dataframe
            attr = getattr(self.ibis_df, name)

        else:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

        # If it's a method, wrap it to return MountainAshDataFrame
        if callable(attr):

            def wrapped(*args, **kwargs):

                #Apply the function and generate a new alias based on the function
                result = attr(*args, **kwargs)

                #TODO: Handle GroupedTables and WindowedTables
                if isinstance(result, (GroupedTable)):

                    # result= result.alias(self.generate_tablename(prefix=name))
                    df = result.table.to_expr().alias(self.generate_tablename(prefix=name))
                    return type(self)(df=df, ibis_backend=self.ibis_backend, df_grouped=result)

                elif isinstance(result, (GroupedArray,GroupedNumbers)):
                    print("We have a GroupedArray or GroupedNumbers table!")
                    # result= result.alias(self.generate_tablename(prefix=name))
                    df = result.parent.alias(self.generate_tablename(prefix=name))
                    return type(self)(df=df, ibis_backend=self.ibis_backend, df_windowed=result)


                elif isinstance(result, (WindowedTable)):
                    print("We have a windowed table!")
                    # result= result.alias(self.generate_tablename(prefix=name))
                    df = result.parent.alias(self.generate_tablename(prefix=name))
                    return type(self)(df=df, ibis_backend=self.ibis_backend, df_windowed=result)

                # If result is an ibis DataFrame, wrap it in this class, otherwise return the result
                elif isinstance(result, ir.Table):
                    result= result.alias(self.generate_tablename(prefix=name))
                    return type(self)(df=result, ibis_backend=self.ibis_backend)

                return result

            return wrapped

        return attr


    # ========
    # Column access - Dictionary-style access
    def __getitem__(self, key):
        """Handle dictionary-style column access.

        Provides access to columns using dictionary-style notation
        (e.g., df['column_name']).

        Args:
            key: The column name to access.

        Returns:
            The column data from the underlying Ibis table.

        Raises:
            KeyError: If the column name is not found in the dataframe.
        """

        if isinstance(key, str) and key not in self._column_names:
            raise KeyError(f"Column '{key}' not found")

        return self.ibis_df[key]


    def _get_dataframe(self) -> ir.Table:
        """Get the underlying Ibis table.

        Internal method to access the raw Ibis table expression.

        Returns:
            ir.Table: The underlying Ibis table.
        """
        return self.ibis_df


    # =========
    # Initialisation

    def init_ibis_table(self,
                        df : Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
                        tablename_prefix:Optional[str]=None,
                        ibis_backend:          Optional[ibis.BaseBackend] = None,
                        ibis_backend_schema:   Optional[str] = None,
                        create_as_view:        Optional[bool] = False,
                 ) -> ir.Table:
        """Initialize an Ibis table from various dataframe formats.

        Converts input dataframes to Ibis tables and sets up the backend
        connection. Handles multiple input formats and backend configurations.

        Args:
            df: Input dataframe in pandas, polars, pyarrow, or Ibis format.
            tablename_prefix: Optional prefix for the generated table name.
            ibis_backend: Optional pre-configured Ibis backend connection.
            ibis_backend_schema: Backend type if creating a new connection.
            create_as_view: Whether to create as a view instead of table.

        Returns:
            ir.Table: Initialized Ibis table expression.
        """


        if isinstance(df, ir.Table):
            #Already in ibis. Use what we have.

            #init the backend
            existing_backend = df._find_backend()

            if existing_backend:
                self.ibis_backend = existing_backend
                self.ibis_backend_schema = existing_backend.name
            else:
                if ibis_backend:
                    self.ibis_backend = ibis_backend
                    self.ibis_backend_schema = ibis_backend.name
                else:
                    if not ibis_backend_schema:
                        ibis_backend_schema = self.default_ibis_backend_schema
                    self.init_default_ibis_backend(default_ibis_schema=ibis_backend_schema)


            # We already have ibis, but if generated manually, we may lack a name
            # if df.get_name() is not None and not tablename_prefix:
            #     ibis_df = df
            # else:
            tablename = self.generate_tablename(prefix=tablename_prefix)
            ibis_df = df.alias(tablename)

        else:
            # We are entering ibis-land for this data.
            if ibis_backend:
                self.ibis_backend = ibis_backend
                self.ibis_backend_schema = ibis_backend.name
            else:
                if not ibis_backend_schema:
                    ibis_backend_schema = self.default_ibis_backend_schema
                self.init_default_ibis_backend(default_ibis_schema=ibis_backend_schema)

            #we have a new table from pandas or polars
            ibis_df = self.create_temp_table_ibis(df=df, tablename_prefix=tablename_prefix, current_ibis_backend=self.ibis_backend,  target_ibis_backend=self.ibis_backend, create_as_view=create_as_view)

        return ibis_df



    def create_temp_table_ibis(self,
                          df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
                          tablename_prefix: Optional[str] = None,
                          current_ibis_backend: Optional[ibis.BaseBackend] = None,
                          target_ibis_backend: Optional[ibis.BaseBackend] = None,
                          overwrite: Optional[bool] = True,
                          create_as_view: Optional[bool] = False
            ) -> ir.Table:

        if current_ibis_backend is None:
            current_ibis_backend = self.ibis_backend

        if target_ibis_backend is None:
            target_ibis_backend = self.ibis_backend


        tablename: str = self.generate_tablename(prefix=tablename_prefix)

        if current_ibis_backend is target_ibis_backend:

            if create_as_view and isinstance(df, ir.Table):
                ibis_df =  target_ibis_backend.create_view(name = tablename, obj=df, overwrite=overwrite)
                return ibis_df

        else:
            #When moving between backends, we need materialise to move to the new backend

            if isinstance(df, ir.Table):

                print(f"Note: Data {df.get_name()} materialised as copy from {current_ibis_backend.name} to {target_ibis_backend.name}")

                if df._find_backend().name in ["snowflake", "duckdb"]:
                    df = self._get_dataframe().to_polars()
                else:
                    df = df.to_pyarrow()
            else:
                print(f"Note: Data materialised as copy from {current_ibis_backend.name} to {target_ibis_backend.name}")


        # if target_ibis_backend.supports_in_memory_tables:
        #     ibis_df =  target_ibis_backend.create_table(name = tablename, obj=df, overwrite=overwrite, temp=True)
        # else:
        ibis_df = target_ibis_backend.create_table(tablename, obj=df, overwrite=bool(overwrite))
        # ibis_df =  target_ibis_backend.create_table(obj=df, overwrite=overwrite)

        if ibis_df is None:
            raise ValueError("Failed to create ibis_df in create_temp_table_ibis")

        return ibis_df



    # Call a method to set the backend schema after super().__init__
    def init_default_ibis_backend_schema(self):
        self.default_ibis_backend_schema = get_default_ibis_backend_schema()

    def init_default_ibis_connection(self, ibis_backend_schema: str) -> ibis.BaseBackend:
        self.ibis_backend_schema = ibis_backend_schema
        return init_ibis_connection(ibis_schema=ibis_backend_schema)

    def init_native_table(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]
                          ) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]:

        return df


    def convert_backend_schema(self, new_backend_schema:str) -> "IbisDataFrame":

        #TODO: verify that the new backend schema is valid

        return IbisDataFrame(df=self.ibis_df, ibis_backend_schema=new_backend_schema)


    def init_default_ibis_backend(self,
                                  default_ibis_schema: Optional[str] = None):

        if not default_ibis_schema:
            default_ibis_schema = self.default_ibis_backend_schema

        #TODO: Check that we have a valid schema
        if default_ibis_schema not in ["duckdb", "polars", "sqlite"]:
            raise ValueError(f"Invalid default ibis schema: {default_ibis_schema}")

        #Using the function to cache the connection.
        self.ibis_backend = self.init_default_ibis_connection(ibis_backend_schema=default_ibis_schema)

        if not self.ibis_backend:
            raise Exception("Ibis client connection not established")






    def generate_tablename(self, prefix: Optional[str] = None) -> str:
        """Generate a unique table name for temporary tables.

        Creates a unique table name using UUID to avoid naming conflicts
        when creating temporary tables in the backend.

        Args:
            prefix: Optional prefix to prepend to the generated name.
                Useful for identifying the operation that created the table.

        Returns:
            str: A unique table name in the format '{prefix}_{uuid}' or
                just '{uuid}' if no prefix is provided.
        """

        if prefix:
            temp_tablename = f"{prefix}_{str(object=uuid.uuid4())}"
        else:
            temp_tablename = str(object=uuid.uuid4())

        return temp_tablename








#Materialisation Status
    def is_materialised(self) -> bool:
        """Check if the dataframe is materialized.

        Returns:
            bool: True if the dataframe is materialized as an Ibis table.
        """
        return isinstance(self.ibis_df, ir.Table)


    def materialise(self, dataframe_framework: Optional[str]=None) -> Any:
        """Convert the Ibis dataframe to a specific framework format.

        Materializes the lazy Ibis expression into a concrete dataframe
        in the requested format. Handles type conversions and compatibility
        checks automatically.

        Args:
            dataframe_framework: Target framework ('polars', 'pandas', 'pyarrow',
                'pyarrow_recordbatch', 'ibis'). Defaults to 'polars'.
                Also accepts 'pyarrow' as shorthand for 'pyarrow_table'.

        Returns:
            Materialized dataframe in the requested format:
            - polars.DataFrame for 'polars'
            - pandas.DataFrame for 'pandas'
            - pyarrow.Table for 'pyarrow' or 'pyarrow_table'
            - List[pyarrow.RecordBatch] for 'pyarrow_recordbatch'
            - ibis.expr.types.Table for 'ibis'

        Raises:
            ValueError: If dataframe_framework is not a valid option.
            TypeError: If conversion to the requested format fails.

        Examples:
            >>> # Convert to polars (default)
            >>> df_polars = ibis_df.materialise()

            >>> # Convert to pandas
            >>> df_pandas = ibis_df.materialise('pandas')

            >>> # Convert to pyarrow
            >>> df_arrow = ibis_df.materialise('pyarrow')
        """

        if dataframe_framework is None:
            dataframe_framework = CONST_DATAFRAME_FRAMEWORK.POLARS

        # Handle shorthand aliases for better user experience
        if dataframe_framework == "pyarrow":
            dataframe_framework = CONST_DATAFRAME_FRAMEWORK.PYARROW_TABLE

        if dataframe_framework == CONST_DATAFRAME_FRAMEWORK.POLARS:
            return self.to_polars()

        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PANDAS:
            return self.to_pandas()

        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PYARROW_TABLE:
            return self.to_pyarrow()

        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PYARROW_RECORDBATCH:
            return self.to_pyarrow_recordbatch()

        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.IBIS:
            return self._get_dataframe()

        else:
            valid_formats = [
                CONST_DATAFRAME_FRAMEWORK.POLARS,
                CONST_DATAFRAME_FRAMEWORK.PANDAS,
                CONST_DATAFRAME_FRAMEWORK.PYARROW_TABLE,
                CONST_DATAFRAME_FRAMEWORK.PYARROW_RECORDBATCH,
                CONST_DATAFRAME_FRAMEWORK.IBIS,
                "pyarrow"  # shorthand alias
            ]
            raise ValueError(f"Invalid dataframe_framework '{dataframe_framework}'. Valid options are: {valid_formats}")

    def to_pyarrow(self) -> pa.Table:
        """Convert the dataframe to a PyArrow Table.

        Converts the Ibis dataframe to PyArrow format, handling type compatibility
        and casting when necessary. If direct conversion is not possible, uses
        intermediate conversion through polars or pandas.

        Returns:
            pyarrow.Table: The dataframe converted to PyArrow format.

        Raises:
            TypeError: If the dataframe cannot be converted to PyArrow format.

        Note:
            Type casting warnings will be printed when schema changes are required
            during conversion.
        """

        ibis_df = self._get_dataframe()

        #check compatibility
        original_schema = ibis_df.schema()
        polars_schema, polars_compatibility, polars_changed = self.check_polars_schema_compatibility()
        pyarrow_schema, pyarrow_compatibility, pyarrow_changed = self.check_pyarrow_schema_compatibility()
        pandas_schema, pandas_compatibility, pandas_changed = self.check_pandas_schema_compatibility()

        if pyarrow_compatibility:
            if pyarrow_changed:

                schema_changes = {k: {"original":original_schema[k],"new":pyarrow_schema[k]}  for k in original_schema.keys() if original_schema[k] != pyarrow_schema[k]}
                for field,schema_change in schema_changes.items():
                    print(f"Warning. The type of {field} has been cast from {schema_change['original']} to {schema_change['new']} during intermediate dataframe conversion to pyarrow")

                ibis_df = ibis_df.cast(pyarrow_schema)
            return ibis_df.to_pyarrow()

        elif polars_compatibility:
            if polars_changed:

                schema_changes = {k: {"original":original_schema[k],"new":polars_schema[k]}  for k in original_schema.keys() if original_schema[k] != polars_schema[k]}
                for field,schema_change in schema_changes.items():
                    print(f"Warning. The type of {field} has been cast from {schema_change['original']} to {schema_change['new']} during intermediate dataframe conversion to polars")

                ibis_df = ibis_df.cast(polars_schema)

            temp_ibis_df = ibis_df.to_polars()
            return temp_ibis_df.to_arrow()


        elif pandas_compatibility:
            if pandas_changed:

                schema_changes = {k: {"original":original_schema[k],"new":pandas_schema[k]}  for k in original_schema.keys() if original_schema[k] != pandas_schema[k]}
                for field,schema_change in schema_changes.items():
                    print(f"Warning. The type of {field} has been cast from {schema_change['original']} to {schema_change['new']} during intermediate dataframe conversion to pandas")

                ibis_df = ibis_df.cast(pandas_schema)
            temp_ibis_df = ibis_df.to_pandas()
            return pa.Table(temp_ibis_df)


        else:
            raise TypeError("Unable to convert dataframe to pyarrow")


    def to_pyarrow_recordbatch(self, batchsize: int = 1) -> List[pa.RecordBatch]:
        """Convert the dataframe to PyArrow RecordBatches.

        Converts the dataframe to a list of PyArrow RecordBatches, useful for
        streaming or processing data in chunks.

        Args:
            batchsize: Maximum number of rows per batch. Defaults to 1.

        Returns:
            List[pa.RecordBatch]: List of PyArrow RecordBatches.
        """
        temp = self.to_pyarrow()
        return temp.to_batches(max_chunksize=batchsize)

    def to_pandas(self, index: Optional[List] = None) -> pd.DataFrame:
        """Convert the dataframe to a pandas DataFrame.

        Converts the Ibis dataframe to pandas format, handling type compatibility
        and casting when necessary. If direct conversion is not possible, uses
        intermediate conversion through polars or pyarrow.

        Args:
            index: Optional list of column names to set as the pandas index.
                If provided, these columns will be used as the DataFrame index.

        Returns:
            pandas.DataFrame: The dataframe converted to pandas format.

        Raises:
            TypeError: If the dataframe cannot be converted to pandas format.

        Note:
            Type casting warnings will be printed when schema changes are required
            during conversion.

        Examples:
            >>> # Simple conversion
            >>> df_pandas = ibis_df.to_pandas()

            >>> # With custom index
            >>> df_pandas = ibis_df.to_pandas(index=['user_id', 'timestamp'])
        """

        ibis_df = self._get_dataframe()

        #check compatibility
        original_schema = ibis_df.schema()
        polars_schema, polars_compatibility, polars_changed = self.check_polars_schema_compatibility()
        pyarrow_schema, pyarrow_compatibility, pyarrow_changed = self.check_pyarrow_schema_compatibility()
        pandas_schema, pandas_compatibility, pandas_changed = self.check_pandas_schema_compatibility()


        if pandas_compatibility:
            if pandas_changed:

                schema_changes = {k: {"original":original_schema[k],"new":pandas_schema[k]}  for k in original_schema.keys() if original_schema[k] != pandas_schema[k]}
                for field,schema_change in schema_changes.items():
                    print(f"Warning. The type of {field} has been cast from {schema_change['original']} to {schema_change['new']} during intermediate dataframe conversion to pandas")

                ibis_df = ibis_df.cast(pandas_schema)
            return_df = ibis_df.to_pandas()

        elif polars_compatibility:
            if polars_changed:

                schema_changes = {k: {"original":original_schema[k],"new":polars_schema[k]}  for k in original_schema.keys() if original_schema[k] != polars_schema[k]}
                for field,schema_change in schema_changes.items():
                    print(f"Warning. The type of {field} has been cast from {schema_change['original']} to {schema_change['new']} during intermediate dataframe conversion to polars")

                ibis_df = ibis_df.cast(polars_schema)

            temp_ibis_df = ibis_df.to_polars()
            return_df =  temp_ibis_df.to_pandas()

        elif pyarrow_compatibility:
            if pyarrow_changed:

                schema_changes = {k: {"original":original_schema[k],"new":pyarrow_schema[k]}  for k in original_schema.keys() if original_schema[k] != pyarrow_schema[k]}
                for field,schema_change in schema_changes.items():
                    print(f"Warning. The type of {field} has been cast from {schema_change['original']} to {schema_change['new']} during intermediate dataframe conversion to pyarrow")

                ibis_df = ibis_df.cast(pyarrow_schema)
            temp_ibis_df = ibis_df.to_pyarrow()
            return_df = temp_ibis_df.to_pandas()


        else:
            raise TypeError("Unable to convert dataframe to pyarrow")


        if index is not None and len(index) > 0:
            return_df.set_index(keys=index, inplace=True)
        return return_df



    def to_polars(self) -> pl.DataFrame:
        """Convert the dataframe to a Polars DataFrame.

        Converts the Ibis dataframe to Polars format, handling type compatibility
        and casting when necessary. If direct conversion is not possible, uses
        intermediate conversion through pyarrow or pandas.

        Returns:
            polars.DataFrame: The dataframe converted to Polars format.

        Raises:
            TypeError: If the dataframe cannot be converted to Polars format.

        Note:
            Type casting warnings will be printed when schema changes are required
            during conversion.
        """

        ibis_df = self._get_dataframe()

        #check compatibility
        original_schema = ibis_df.schema()
        polars_schema, polars_compatibility, polars_changed = self.check_polars_schema_compatibility()
        pyarrow_schema, pyarrow_compatibility, pyarrow_changed = self.check_pyarrow_schema_compatibility()
        pandas_schema, pandas_compatibility, pandas_changed = self.check_pandas_schema_compatibility()

        if polars_compatibility:
            if polars_changed:

                schema_changes = {k: {"original":original_schema[k],"new":polars_schema[k]}  for k in original_schema.keys() if original_schema[k] != polars_schema[k]}
                for field,schema_change in schema_changes.items():
                    print(f"Warning. The type of {field} has been cast from {schema_change['original']} to {schema_change['new']} during intermediate dataframe conversion to polars")

                ibis_df = ibis_df.cast(polars_schema)
            return ibis_df.to_polars()

        elif pyarrow_compatibility:
            if pyarrow_changed:

                schema_changes = {k: {"original":original_schema[k],"new":pyarrow_schema[k]}  for k in original_schema.keys() if original_schema[k] != pyarrow_schema[k]}
                for field,schema_change in schema_changes.items():
                    print(f"Warning. The type of {field} has been cast from {schema_change['original']} to {schema_change['new']} during intermediate dataframe conversion to pyarrow")

                ibis_df = ibis_df.cast(pyarrow_schema)
            temp_ibis_df = ibis_df.to_pyarrow()
            return pl.DataFrame(temp_ibis_df)

        elif pandas_compatibility:
            if pandas_changed:

                schema_changes = {k: {"original":original_schema[k],"new":pandas_schema[k]}  for k in original_schema.keys() if original_schema[k] != pandas_schema[k]}
                for field,schema_change in schema_changes.items():
                    print(f"Warning. The type of {field} has been cast from {schema_change['original']} to {schema_change['new']} during intermediate dataframe conversion to pandas")

                ibis_df = ibis_df.cast(pandas_schema)
            temp_ibis_df = ibis_df.to_pandas()
            return pl.DataFrame(temp_ibis_df)


        else:
            raise TypeError("Unable to convert dataframe to polars")



    def to_ibis(self) -> ir.Table:
        """Return the underlying Ibis table.

        Returns:
            ibis.expr.types.Table: The underlying Ibis table expression.
        """
        return self._get_dataframe()

    def to_xarray(self, index: Optional[List] = None) -> ir.Table:
        """Convert the dataframe to an xarray Dataset.

        Converts the dataframe to xarray format via pandas intermediate.

        Args:
            index: Optional list of column names to use as coordinates
                in the xarray Dataset.

        Returns:
            xarray.Dataset: The dataframe converted to xarray format.
        """
        return self.to_pandas(index=index).to_xarray()

    #check schema compatibility

    def check_pandas_schema_compatibility(self) -> Tuple[Schema|None, bool, bool]:
        """Check if the dataframe schema is compatible with pandas.

        Determines whether the current schema can be converted to pandas
        format and identifies any type changes that would be required.

        Returns:
            Tuple containing:
            - new_schema: The schema after pandas compatibility adjustments
            - compatible (bool): True if conversion is possible
            - changed (bool): True if type casting is required
        """

        original_schema = self._get_dataframe().schema()
        changed = False
        try:
            new_schema = PandasSchema.to_ibis(PandasSchema.from_ibis(original_schema))
            compatible = True
            if original_schema != new_schema:
                changed = True
        except Exception:
            new_schema = None
            compatible = False

        return  new_schema, compatible, changed

    def check_polars_schema_compatibility(self) -> Tuple[Schema|None, bool, bool]:
        """Check if the dataframe schema is compatible with polars.

        Determines whether the current schema can be converted to polars
        format and identifies any type changes that would be required.

        Returns:
            Tuple containing:
            - new_schema: The schema after polars compatibility adjustments
            - compatible (bool): True if conversion is possible
            - changed (bool): True if type casting is required
        """

        original_schema = self._get_dataframe().schema()
        changed = False
        try:
            new_schema = PolarsSchema.to_ibis(PolarsSchema.from_ibis(original_schema))
            compatible = True
            if original_schema != new_schema:
                changed = True
        except Exception:
            new_schema = None
            compatible = False

        return  new_schema, compatible, changed

    def check_pyarrow_schema_compatibility(self) -> Tuple[Schema|None, bool, bool]:
        """Check if the dataframe schema is compatible with pyarrow.

        Determines whether the current schema can be converted to pyarrow
        format and identifies any type changes that would be required.

        Returns:
            Tuple containing:
            - new_schema: The schema after pyarrow compatibility adjustments
            - compatible (bool): True if conversion is possible
            - changed (bool): True if type casting is required
        """

        original_schema = self._get_dataframe().schema()
        changed = False
        try:
            new_schema = PyArrowSchema.to_ibis(PyArrowSchema.from_ibis(original_schema))
            compatible = True
            if original_schema != new_schema:
                changed = True
        except Exception:
            new_schema = None
            compatible = False

        return new_schema, compatible, changed


    def cast_dataframe_to_pyarrow(self) -> pa.Table:
        return self.to_pyarrow()

    def cast_dataframe_to_pyarrow_recordbatch(self, batchsize: int = 1) -> List[pa.RecordBatch]:
        return self.to_pyarrow_recordbatch()

    def cast_dataframe_to_pandas(self, index: Optional[List] = None) -> pd.DataFrame:
        return self.to_pandas(index=index)

    def cast_dataframe_to_polars(self) -> pl.DataFrame:
        return self.to_polars()

    def cast_dataframe_to_ibis(self) -> ir.Table:
        return self._get_dataframe()

    def cast_dataframe_to_xarray(self) -> ir.Table:
        return self.to_xarray()


# Column Selection
    # def select(self, ibis_expr: Any) -> "IbisDataFrame":

    #     new_df: ir.Table = self._select_ibis(ibis_expr=ibis_expr).alias(self.generate_tablename(prefix="select"))
    #     # add a lineage record - name and transformations

    #     return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)


    # def _select_ibis(self, ibis_expr: Any) -> Any:

    #     #Do not add a parameter name here for columns. That will be interpreted as a column name
    #     df_cols: Any = self.ibis_df.select(ibis_expr)
    #     return df_cols


    def drop(self, columns: Any) -> "IbisDataFrame":

        new_df: ir.Table = self._drop_ibis(columns).alias(self.generate_tablename(prefix="drop"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _drop_ibis(self, columns: Any) -> ir.Table:

        #Only drop columns if they exist in the dataframe
        if isinstance(columns, str):
            columns = [columns]

        existing_columns = self.get_column_names()
        columns = [x for x in columns if x in existing_columns]

        if not columns or len(columns) == 0:
            return self.ibis_df

        new_df: Any = self.ibis_df.drop(columns)
        return new_df

#     def distinct(self) -> "IbisDataFrame":

#         new_df: ir.Table = self._distinct_ibis().alias(self.generate_tablename(prefix="distinct"))

#         return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _distinct_ibis(self) -> ir.Table:
#         new_df: Any = self.ibis_df.distinct()
#         return new_df


#     def rename(self, **kwargs) -> "IbisDataFrame":

#         new_df: ir.Table = self._rename_ibis(**kwargs).alias(self.generate_tablename(prefix="rename"))

#         return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _rename_ibis(self,  **kwargs) -> ir.Table:

#         #Will need to be addresses in https://github.com/mountainash-io/mountainash-data/issues/22
#         # Needs to pass test_rename_method()
#         new_df: Any = self.ibis_df.rename( **kwargs)
#         return new_df


#     def try_cast(self, **kwargs) -> "IbisDataFrame":

#         new_df: ir.Table = self._try_cast_ibis(**kwargs).alias(self.generate_tablename(prefix="try_cast"))

#         return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _try_cast_ibis(self,  **kwargs) -> ir.Table:
#         new_df: Any = self.ibis_df.try_cast( **kwargs)
#         return new_df

# # Add Columns
#     def mutate(self, **kwargs) -> "IbisDataFrame":

#         new_df: ir.Table = self._mutate_ibis( **kwargs).alias(self.generate_tablename(prefix="mutate"))
#         return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _mutate_ibis(self,  **kwargs) -> ir.Table:
#         df_cols: Any = self.ibis_df.mutate( **kwargs)
#         return df_cols

# # Reshape
#     def aggregate(self,  **kwargs) -> "IbisDataFrame":

#         new_df: ir.Table = self._aggregate_ibis( **kwargs).alias(self.generate_tablename(prefix="aggregate"))
#         return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _aggregate_ibis(self, **kwargs) -> ir.Table:

#         df_cols: Any = self.ibis_df.aggregate(**kwargs)
#         return df_cols

#     def pivot_wider(self,  **kwargs) -> "IbisDataFrame":

#         new_df: ir.Table = self._pivot_wider_ibis( **kwargs).alias(self.generate_tablename(prefix="pivot_wider"))
#         return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _pivot_wider_ibis(self, **kwargs) -> ir.Table:
#         df_cols: Any = self.ibis_df.pivot_wider(**kwargs)
#         return df_cols

#     def pivot_longer(self, **kwargs) -> "IbisDataFrame":

#         new_df: ir.Table = self._pivot_longer_ibis( **kwargs).alias(self.generate_tablename(prefix="pivot_longer"))
#         return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _pivot_longer_ibis(self, **kwargs) -> ir.Table:
#         df_cols: Any = self.ibis_df.pivot_longer(**kwargs)
#         return df_cols


    def _cast_types_ibis(self,
                         df_ibis: ir.Table,
                         target_ibis_backend_schema: Optional[str],
                         fields_diff_types: List[str],
                         target_fields_types: Dict[str, Any]) -> ir.Table:

        if self.ibis_backend_schema in set("duckdb"):
            cast_dict = {field: target_fields_types[field] for field in fields_diff_types}
            # print(f"CAST DICT: {cast_dict}")
            df_ibis = df_ibis.try_cast(cast_dict)
        else:

            for field in fields_diff_types:
                target_type = target_fields_types[field]
                # print(f"Casting field: {field} to {target_type}")
                df_ibis = ( df_ibis
                                .mutate(field = ibis._[field].cast(target_type ) )
                                .drop(field)
                                .rename({field: "field"})
                )
        return df_ibis


    #Join Resolution!

    def _resolve_join_backend(self,
                                   right: "BaseDataFrame",
                                   execute_on: Optional[str] = None
                                   ) -> Tuple[ir.Table, ir.Table]:

        return self._resolve_join_backend_ibis(right=right, execute_on=execute_on)


    def _resolve_join_backend_ibis(self,
                                   right: "BaseDataFrame",
                                   execute_on: Optional[str] = None
                                   ) -> Tuple[ir.Table, ir.Table]:

        #TODO: Get schemas, and compare
        #IF different and diff backend, cast to target backend
        #IF different and same backend, cast to left backend
        # Finally, never use Pandas! Always use Ibis or Polars


        if self.ibis_backend is None:
            raise Exception("Ibis client connection not established")

        if execute_on and execute_on not in ["left", "right"]:
            raise ValueError("execute_on must be one of 'left', 'right' or None")

        #Test if we have the same backend object
        same_backend = False

        #Resolve table schema
        left_table_schema:  Optional[ibis_schema.Schema] = self.ibis_df.schema()
        right_table_schema: Optional[ibis_schema.Schema] = right._get_dataframe().schema()

        #find common keys in schemas:
        common_fields = list(set(left_table_schema.fields.keys()).intersection(set(right_table_schema.fields.keys())))

        #For common keys, check if the types are the same
        fields_diff_types: List[str] = [field for field in common_fields if left_table_schema.fields[field] != right_table_schema.fields[field]]
        fields_types_left: Dict[str, Any] = {field: left_table_schema[field] for field in fields_diff_types}
        fields_types_right: Dict[str, Any] = {field: right_table_schema[field] for field in fields_diff_types}


        #For in-memory dbs don't use equals (==) here as it just compares the connection string!
        if self.ibis_backend_schema in ["duckdb", "sqlite"] and self.ibis_backend is right._get_dataframe()._find_backend():
            same_backend = True
        #For database connections (like databases) it is OK to use == as it compares the connection string, which will be the same database, even if they are distinct connections
        if self.ibis_backend_schema not in ["duckdb", "sqlite"] and self.ibis_backend == right.ibis_backend:
            same_backend = True


        if isinstance(right, IbisDataFrame) and same_backend:

            left_table = self.ibis_df
            right_table = right.ibis_df

            if len(fields_diff_types) > 0:

                right_table = self._cast_types_ibis(df_ibis=right_table,
                                                    target_ibis_backend_schema =self.ibis_backend_schema,
                                                    fields_diff_types=fields_diff_types,
                                                    target_fields_types=fields_types_left)


        #right a different backend or not ibis
        else:

            if execute_on == "left":

                left_table = self.ibis_df
                right_table = right.create_temp_table_ibis(df=right._get_dataframe(),
                                                          tablename_prefix="right_table",
                                                          current_ibis_backend=right.ibis_backend,
                                                          target_ibis_backend=self.ibis_backend,
                                                          overwrite=True)



                if len(fields_diff_types) > 0:
                    right_table = self._cast_types_ibis(df_ibis=right_table,
                                                        target_ibis_backend_schema =right.ibis_backend_schema,
                                                        fields_diff_types=fields_diff_types,
                                                        target_fields_types=fields_types_left)

            elif execute_on == "right":

                if not isinstance(right, IbisDataFrame):
                    raise ValueError("Right must be an IbisDataFrame to execute on right")

                left_table = self.create_temp_table_ibis(df=self.ibis_df,
                                                          tablename_prefix="left_table",
                                                          current_ibis_backend=self.ibis_backend,
                                                          target_ibis_backend=right.ibis_backend,
                                                          overwrite=True)
                right_table = right.ibis_df

                if len(fields_diff_types) > 0:
                    left_table = self._cast_types_ibis(df_ibis=left_table,
                                                       target_ibis_backend_schema =self.ibis_backend_schema,
                                                       fields_diff_types=fields_diff_types,
                                                       target_fields_types=fields_types_right)


            else:

                default_ibis_backend = init_ibis_connection(self.default_ibis_backend_schema)

                #We have differing backends, and we are running locally. Bring both into local memory
                left_table =  self.create_temp_table_ibis(df=self.ibis_df,
                                                          tablename_prefix="left_table",
                                                          current_ibis_backend=self.ibis_backend,
                                                          target_ibis_backend=default_ibis_backend,
                                                          overwrite=True)

                right_table = right.create_temp_table_ibis(df=right._get_dataframe(),
                                                           tablename_prefix="right_table",
                                                             current_ibis_backend=right.ibis_backend,
                                                             target_ibis_backend=default_ibis_backend,
                                                             overwrite=True)

                if len(fields_diff_types) > 0:
                    right_table = self._cast_types_ibis(df_ibis=right_table,
                                                        target_ibis_backend_schema =self.ibis_backend_schema,
                                                        fields_diff_types=fields_diff_types,
                                                        target_fields_types=fields_types_left)

        return (left_table, right_table)



    def inner_join(self,
                   right: "BaseDataFrame",
                   predicates: Any,
                   execute_on: Optional[str] = None,
                   **kwargs) -> "IbisDataFrame":
        """Perform an inner join with another dataframe.

        Joins this dataframe with another dataframe using inner join semantics,
        returning only rows that have matching values in both dataframes.
        Supports cross-backend joins by automatically resolving backend compatibility.

        Args:
            right: The right dataframe to join with. Can be any BaseDataFrame
                or a format that can be converted to IbisDataFrame.
            predicates: Join condition(s). Can be:
                - A list of column names (for equality joins on same-named columns)
                - A boolean expression
                - A list of tuples for joining on different column names
            execute_on: Optional backend to execute the join on. If not specified,
                automatically determines the appropriate backend.
            **kwargs: Additional arguments passed to the underlying Ibis join method.

        Returns:
            IbisDataFrame: A new dataframe containing the inner join result.

        Examples:
            >>> # Join on single column
            >>> result = df1.inner_join(df2, predicates=['user_id'])

            >>> # Join on multiple columns
            >>> result = df1.inner_join(df2, predicates=['user_id', 'date'])

            >>> # Join with different column names
            >>> result = df1.inner_join(df2, predicates=[('id', 'user_id')])
        """

        if not isinstance(right, BaseDataFrame):
            right = IbisDataFrame(df = right)

        new_df: ir.Table = self._inner_join_ibis(right=right,
                                                 predicates=predicates,
                                                 execute_on=execute_on,
                                                 **kwargs).alias(self.generate_tablename(prefix="inner_join"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _inner_join_ibis(self,
             right: "BaseDataFrame",
             predicates: Any,
             execute_on: Optional["str"] = None,
             **kwargs
            ) -> ir.Table:

        left_table, right_table = self._resolve_join_backend_ibis(right=right,
                                                                  execute_on=execute_on)

        return left_table.inner_join(right_table, predicates=predicates, **kwargs)

    def left_join(self,
                   right: "BaseDataFrame",
                   predicates: Any,
                   execute_on: Optional[str] = None,
                   **kwargs) -> "IbisDataFrame":
        """Perform a left join with another dataframe.

        Joins this dataframe with another dataframe using left join semantics,
        returning all rows from the left dataframe and matched rows from the right.
        Unmatched rows from the right will have NULL values.
        Supports cross-backend joins by automatically resolving backend compatibility.

        Args:
            right: The right dataframe to join with. Can be any BaseDataFrame
                or a format that can be converted to IbisDataFrame.
            predicates: Join condition(s). Can be:
                - A list of column names (for equality joins on same-named columns)
                - A boolean expression
                - A list of tuples for joining on different column names
            execute_on: Optional backend to execute the join on. If not specified,
                automatically determines the appropriate backend.
            **kwargs: Additional arguments passed to the underlying Ibis join method.

        Returns:
            IbisDataFrame: A new dataframe containing the left join result.

        Examples:
            >>> # Left join preserving all rows from df1
            >>> result = df1.left_join(df2, predicates=['user_id'])

            >>> # Left join with custom backend
            >>> result = df1.left_join(df2, predicates=['id'], execute_on='duckdb')
        """

        if not isinstance(right, BaseDataFrame):
            right = IbisDataFrame(df = right)


        new_df: ir.Table = self._left_join_ibis(right=right,
                                                predicates=predicates,
                                                execute_on=execute_on,
                                                **kwargs).alias(self.generate_tablename(prefix="left_join"))


        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _left_join_ibis(self,
             right: "BaseDataFrame",
             predicates: Any,
             execute_on: Optional["str"] = None,
             **kwargs
            ) -> ir.Table:

        left_table, right_table = self._resolve_join_backend_ibis(right=right, execute_on=execute_on)

        return left_table.left_join(right_table, predicates=predicates, **kwargs)


    def outer_join(self,
                   right: "BaseDataFrame",
                   predicates: Any,
                   execute_on: Optional[str] = None,
                   **kwargs) -> "IbisDataFrame":
        """Perform an outer join with another dataframe.

        Joins this dataframe with another dataframe using outer join semantics,
        returning all rows from both dataframes. Unmatched rows will have NULL
        values for columns from the other dataframe.
        Supports cross-backend joins by automatically resolving backend compatibility.

        Args:
            right: The right dataframe to join with. Can be any BaseDataFrame
                or a format that can be converted to IbisDataFrame.
            predicates: Join condition(s). Can be:
                - A list of column names (for equality joins on same-named columns)
                - A boolean expression
                - A list of tuples for joining on different column names
            execute_on: Optional backend to execute the join on. If not specified,
                automatically determines the appropriate backend.
            **kwargs: Additional arguments passed to the underlying Ibis join method.

        Returns:
            IbisDataFrame: A new dataframe containing the outer join result.

        Examples:
            >>> # Outer join keeping all rows from both dataframes
            >>> result = df1.outer_join(df2, predicates=['user_id'])

            >>> # Outer join with multiple conditions
            >>> result = df1.outer_join(df2, predicates=['user_id', 'product_id'])
        """

        if not isinstance(right, BaseDataFrame):
            right = IbisDataFrame(df = right)


        new_df: ir.Table = self._outer_join_ibis(right=right,
                                                 predicates=predicates,
                                                 execute_on=execute_on,
                                                 **kwargs).alias(self.generate_tablename(prefix="outer_join"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _outer_join_ibis(self,
             right: "BaseDataFrame",
             predicates: Any,
             execute_on: Optional["str"] = None,
             **kwargs
            ) -> ir.Table:

        left_table, right_table = self._resolve_join_backend_ibis(right=right, execute_on=execute_on)

        return left_table.outer_join(right_table, predicates=predicates, **kwargs)

# Row Selection

    def filter(self,
               ibis_expr: Optional[Any] = None,
               filter_condition: Optional[FilterCondition] = None
               ) -> "IbisDataFrame":
        """Filter rows based on the given expression."""

        new_df: ir.Table = self._filter_ibis(ibis_expr, filter_condition).alias(self.generate_tablename(prefix="filter"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _filter_ibis(self,
                     ibis_expr: Optional[Any] = None,
                     filter_condition: Optional[FilterCondition] = None) -> ir.Table:

        if ibis_expr is not None and filter_condition is not None:
            raise ValueError("Only one of ibis_expr and filter_condition can be used")

        if ibis_expr is not None:
            return self.ibis_df.filter(ibis_expr)
        if filter_condition is not None:
            return DataFrameUtils.filter(self.ibis_df, filter_condition)

        return self._get_dataframe()

    # def distinct(self) -> "IbisDataFrame":
    #     """Take the first n rows."""

    #     new_df = self._distinct_ibis().alias(self.generate_tablename(prefix="distinct"))

    #     return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    # def _distinct_ibis(self) -> ir.Table:

    #     return self.ibis_df.distinct()




    # def head(self, n: int=10) -> "IbisDataFrame":
    #     """Take the first n rows."""

    #     new_df = self._head_ibis(n).alias(self.generate_tablename(prefix="head"))

    #     return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    # def _head_ibis(self, n: int) -> ir.Table:

    #     if n < 0:
    #         raise ValueError("n must be greater than or equal to 0")

    #     return self.ibis_df.head(n)

    #Table Comparisons
    def union(self, **kwargs) -> "IbisDataFrame":
        new_df = self._union_ibis(**kwargs).alias(self.generate_tablename(prefix="union"))
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _union_ibis(self, **kwargs) -> ir.Table:

        #There may be multiple tables to union...
        return ibis.union(self.ibis_df, **kwargs)


    def difference(self, comparison_df: "IbisDataFrame") -> "IbisDataFrame":

        new_df = self._difference_ibis(comparison_df=comparison_df).alias(self.generate_tablename(prefix="difference"))
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _difference_ibis(self, comparison_df: "IbisDataFrame") -> ir.Table:

        left_table, comparison_df = self._resolve_join_backend_ibis(right=comparison_df, execute_on="right")

        return ibis.difference(left_table, comparison_df)


    def intersect(self, comparison_df: "IbisDataFrame") -> "IbisDataFrame":
        new_df = self._intersect_ibis(comparison_df=comparison_df).alias(self.generate_tablename(prefix="intersect"))
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _intersect_ibis(self, comparison_df: "IbisDataFrame") -> ir.Table:

        left_table, comparison_df = self._resolve_join_backend_ibis(right=comparison_df, execute_on="right")

        return ibis.intersect(left_table, comparison_df)



    # def order_by(self, by=Any) -> "IbisDataFrame":

    #     new_df = self._order_by_ibis(by=by).alias(self.generate_tablename(prefix="order_by"))
    #     return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    # def _order_by_ibis(self, by=Any) -> ir.Table:
    #     return self.ibis_df.order_by(by)

#Column Metadata
    def get_column_names(self) -> List[str]:
        """Get the list of column names in the dataframe.

        Returns:
            List[str]: List of column names in the order they appear in the dataframe.
        """
        return self._get_column_names_ibis()

    def _get_column_names_ibis(self) -> List[str]:
        """Internal method to get column names from Ibis.

        Returns:
            List[str]: List of column names.
        """
        #force to list as ibis returns a tuple > v1.10
        return list(self.ibis_df.columns)

### Aggregates

    def count(self) -> int:
        """Get the number of rows in the dataframe.

        Executes a count query on the backend to determine the number of rows.

        Returns:
            int: The number of rows in the dataframe.
        """
        return self._count_ibis()

    def _count_ibis(self) -> int:

        return self.ibis_df.count().execute()


### Query

    def sql(self, query:str) -> "IbisDataFrame":
        """Execute a SQL query on the dataframe.

        Allows executing SQL queries directly on the dataframe. The dataframe
        can be referenced in the query using '{_}' as a placeholder.

        Args:
            query: SQL query string. Use '{_}' to reference this dataframe
                in the query.

        Returns:
            IbisDataFrame: A new dataframe containing the query results.

        Examples:
            >>> # Select specific columns
            >>> result = df.sql("SELECT user_id, amount FROM {_} WHERE amount > 100")

            >>> # Aggregate query
            >>> result = df.sql("SELECT user_id, SUM(amount) FROM {_} GROUP BY user_id")
        """
        new_df = self._sql_ibis(query=query).alias(self.generate_tablename(prefix="sql"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _sql_ibis(self, query:str) -> ir.Table:

        if self.ibis_df.get_name() is not None:
            query = query.format(_=self.ibis_df.get_name())
        else:
            query = query.format(_="df")

        return self.ibis_df.sql(query=query)

    def to_pydict(self) -> Dict[str, List[Any]] | Any:
        """Convert the dataframe to a Python dictionary.

        Converts the dataframe to a dictionary where keys are column names
        and values are lists of column values.

        Returns:
            Dict[str, List[Any]]: Dictionary with column names as keys and
                column data as lists.

        Examples:
            >>> df_dict = ibis_df.to_pydict()
            >>> # {'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']}
        """
        return self._as_dict_ibis()

    def as_dict(self) -> Dict[str, List[Any]] | Any:
        """Alias for to_pydict().

        See to_pydict() for full documentation.
        """
        return self._as_dict_ibis()

    def _as_dict_ibis(self) -> Dict[str, List[Any]] | Any:
        df: pa.Table = self.ibis_df.to_polars()
        return df.to_dict(as_series=False)


    def to_pylist(self) -> List[Dict[Any,Any]] | Any:
        """Convert the dataframe to a list of dictionaries.

        Converts the dataframe to a list where each element is a dictionary
        representing a row, with column names as keys.

        Returns:
            List[Dict[Any, Any]]: List of dictionaries, one per row.

        Examples:
            >>> df_list = ibis_df.to_pylist()
            >>> # [{'col1': 1, 'col2': 'a'}, {'col1': 2, 'col2': 'b'}]
        """
        return self._as_list_ibis()

    def as_list(self) -> List[Dict[Any,Any]] | Any:
        """Alias for to_pylist().

        See to_pylist() for full documentation.
        """
        return self._as_list_ibis()

    def _as_list_ibis(self) -> List[Dict[Any,Any]] | Any:
        df: pa.Table = self.ibis_df.to_polars()
        return df.to_dicts()



    def get_first_row_as_dict(self,
        ) -> Dict[Any,Any]:
        """Get the first row of the dataframe as a dictionary.

        Returns the first row with column names as keys and cell values
        as values. Returns an empty dictionary if the dataframe is empty.

        Returns:
            Dict[Any, Any]: Dictionary representation of the first row,
                or empty dict if no rows exist.

        Examples:
            >>> first_row = ibis_df.get_first_row_as_dict()
            >>> # {'user_id': 123, 'name': 'John', 'age': 30}
        """
        return self._get_first_row_as_dict_ibis()

    def _get_first_row_as_dict_ibis(
            self,
        ) -> Dict[Any,Any]:

        obj_df = self.head(1)
        obj_list = obj_df.ibis_df.to_pyarrow().to_pylist()
        if len(obj_list) > 0:
            return obj_list[0]
        else:
            return {}

    def get_column_as_list(
            self,
            column:str
        ) -> List[Any]:
        """Extract a single column as a Python list.

        Args:
            column: Name of the column to extract.

        Returns:
            List[Any]: List containing all values from the specified column.

        Raises:
            KeyError: If the column does not exist in the dataframe.

        Examples:
            >>> user_ids = ibis_df.get_column_as_list('user_id')
            >>> # [123, 456, 789]
        """
        obj_dict = self.select(column).to_pyarrow().to_pydict()

        return obj_dict[column]


    def get_column_as_set(
            self,
            column: str
        ) -> t.Set[Any]:
        """Extract unique values from a column as a Python set.

        Gets all distinct values from the specified column and returns
        them as a set.

        Args:
            column: Name of the column to extract unique values from.

        Returns:
            Set[Any]: Set containing all unique values from the column.

        Raises:
            KeyError: If the column does not exist in the dataframe.

        Examples:
            >>> unique_categories = ibis_df.get_column_as_set('category')
            >>> # {'electronics', 'clothing', 'food'}
        """
        obj_dict = self.select(column).ibis_df.distinct().to_pyarrow().to_pydict()

        return obj_dict[column]
