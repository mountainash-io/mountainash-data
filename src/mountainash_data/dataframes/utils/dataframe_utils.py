from typing import Union, Any,  Dict, List, Optional, Sequence

import collections

import pandas as pd
import polars as pl
import pyarrow as pa

import ibis
from mountainash_constants import CONST_DATAFRAME_FRAMEWORK
import ibis.expr.types as ir
import ibis.expr.schema as ibis_schema

from . import BaseDataFrameStrategy, PandasDataFrameUtils, PolarsDataFrameUtils, PolarsLazyFrameUtils, PyArrowTableUtils, IbisDataFrameUtils, PyArrowRecordBatchUtils

from .filter import FilterVisitor, ColumnCondition, LogicalCondition, FilterNode, PandasFilterVisitor
# from pyarrow import Table as irTable  # assuming you are using pyarrow's Table for ir.Table


class DataFrameUtils:


    @classmethod
    def create_dataframe(
            cls,
            dataframe_framework: str, 
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None,
            column_types: Optional[Dict[str, str]]=None
         ) -> Union[pd.DataFrame, pl.DataFrame, ir.Table, pa.Table, pa.RecordBatch]:

        if not dataframe_framework:
            raise ValueError("dataframe_framework must be specified")
        

        if dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PANDAS.value:
            return cls.create_pandas_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.POLARS.value:
            return cls.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.IBIS.value:
            return cls.create_ibis_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PYARROW_TABLE.value:
            return cls.create_pyarrow_table(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PYARROW_RECORDBATCH.value:
            return cls.create_pyarrow_recordbatch(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        else:
            raise ValueError(f"Unsupported dataframe framework: {dataframe_framework}")
        
    @classmethod
    def create_pandas_dataframe(
            cls,
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None,
            column_types: Optional[Dict[str, str]]=None) -> pd.DataFrame:

        pl_df = cls.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        return cls.cast_dataframe_to_pandas(df=pl_df)


    @classmethod
    def create_polars_dataframe(
            cls,
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None,
            column_types: Optional[Dict[str, str]]=None) -> pl.DataFrame:

        if column_dict and cls.validate_column_mapping(column_dict=column_dict):

            df = pl.DataFrame(data_dict, strict=False)
            df_cols = DataFrameUtils.get_column_names(df)
            new_colnames = {col: column_dict.get(col, col) for col in df_cols}

            return df.rename(mapping=new_colnames)

        else:
            return pl.DataFrame(data=data_dict, strict=False)
            
    @classmethod
    def create_pyarrow_recordbatch(
        cls,
        data_dict: Dict[str, Union[Sequence, List]] | List[Dict[str, Any]],
        column_dict: Optional[Dict[str, str]] = None,
        column_types: Optional[Dict[str, str]] = None,
        batchsize: int = 1
    ) -> List[pa.RecordBatch]:
        
        pl_df = cls.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        return cls.cast_dataframe_to_pyarrow_recordbatch(df=pl_df, batchsize=batchsize)


    @classmethod
    def create_pyarrow_table(
        cls,
        data_dict: Dict[str, Union[Sequence, List]] | List[Dict[str, Any]],
        column_dict: Optional[Dict[str, str]] = None,
        column_types: Optional[Dict[str, str]] = None
    ) -> pa.Table:
        
        pl_df = cls.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        return cls.cast_dataframe_to_arrow(df=pl_df)


        # if isinstance(data_dict, dict):
        #     # Convert dict of lists/sequences to PyArrow table
        #     table = pa.Table.from_pydict(data_dict)
        # elif isinstance(data_dict, list):
        #     # Convert list of dicts to PyArrow table
        #     table = pa.Table.from_pylist(data_dict)
        # else:
        #     raise ValueError("Input must be a dictionary of sequences or a list of dictionaries")

        # if column_dict and cls.validate_column_mapping(column_dict=column_dict):

        #     # Rename columns if column_dict is provided
        #     # new_names = [column_dict.get(col, col) for col in table.column_names]
        #     df_cols = cls.get_column_names(table)
        #     new_colnames = {col: column_dict.get(col, col) for col in df_cols}

        #     table = table.rename_columns(new_colnames)

        # return table

    @classmethod
    def create_ibis_dataframe(
            cls,
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]] = None,
            column_types: Optional[Dict[str, str]]=None ) -> ir.Table:
        
        pl_df = cls.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)

        return cls.cast_dataframe_to_ibis(df=pl_df)

    @classmethod
    def validate_column_mapping(cls,
                                column_dict: Optional[Dict[str, str]]=None) -> bool:
        """
        Validates the column mapping for a given data dictionary and column dictionary.

        Args:
            data_dict (Dict[str, Union[Sequence,List]] | List[Dict[str, Any]]): The data dictionary to validate.
            column_dict (Optional[Dict[str, str]]): The column dictionary to validate. Defaults to None.

        Raises:
            ValueError: If source and target column names are not specified or if duplicate column names are found.
            TypeError: If column names are not strings.
        """

        # Column Validation
        if column_dict is not None:

            try:
                # Cannot have duplicate column names in the keys or the values
                if len(column_dict) != len(set(column_dict.values())):
                    duplicate_values = [item for item, count in collections.Counter(column_dict.values()).items() if count > 1]    
                    raise ValueError(f"Source and target column names must be unique. Duplicate column names: {duplicate_values}")

                if len(column_dict) != len(set(column_dict.keys())):
                    duplicate_values = [item for item, count in collections.Counter(column_dict.keys()).items() if count > 1]    
                    raise ValueError(f"Source and target column names must be unique. Duplicate column names: {duplicate_values}")

                if None in column_dict.values():
                    raise ValueError("Source and target column names must be specified")

                column_types = [type(colname) for colname in column_dict.values()]
                if any(coltype != str for coltype in column_types):
                    raise ValueError("Column names must be strings")
                
            except ValueError as e:
                print(f"Error validating column mapping: {e}")
                return False
            except TypeError as e:
                print(f"Error validating column mapping: {e}")
                return False
        return True

    @classmethod
    def _is_recordbatch(cls, df: Any) -> bool:

        if isinstance(df, list):
            return isinstance(df[0], pa.RecordBatch)
        else:
            return isinstance(df, pa.RecordBatch)


    ################################

    @classmethod
    def _get_strategy(cls, 
                      df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> BaseDataFrameStrategy:
        
        
        if isinstance(df, ir.Table):
            return IbisDataFrameUtils()
        elif cls._is_recordbatch(df=df):
            return PyArrowRecordBatchUtils()
        elif isinstance(df, pa.Table):
            return PyArrowTableUtils()
        elif isinstance(df, pl.DataFrame ):
            return PolarsDataFrameUtils()
        elif isinstance(df, pl.LazyFrame ):
            return PolarsLazyFrameUtils()
        elif isinstance(df, pd.DataFrame):
            return PandasDataFrameUtils()
        else:
            raise TypeError(f"Unsupported dataframe type. Received {type(df)}")


    @classmethod
    def cast_dataframe_to_pandas(cls, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> pd.DataFrame:
        strategy = cls._get_strategy(df=df)
        return strategy.cast_to_pandas(df=df)

    @classmethod
    def cast_dataframe_to_polars(cls, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> pl.DataFrame:
        strategy = cls._get_strategy(df=df)
        return strategy.cast_to_polars(df=df)

    @classmethod
    def cast_dataframe_to_arrow(cls, 
                                df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> pa.Table:
        strategy = cls._get_strategy(df=df)
        return strategy._cast_to_pyarrow_table(df)

    @classmethod
    def cast_dataframe_to_pyarrow_recordbatch(cls, 
                                              df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
                                              batchsize: int = 1) -> List[pa.RecordBatch]:
        strategy = cls._get_strategy(df=df)
        return strategy.cast_to_pyarrow_recordbatch(df=df, batchsize=batchsize)

    # @classmethod
    # def cast_dataframe_to_pyarrow_reader(cls, 
    #                                           df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]
    #                                           ) -> pa.RecordBatchReader:
    #     strategy = cls._get_strategy(df=df)
    #     return strategy.cast_to_pyarrow_reader(df=df)


    @classmethod
    def cast_dataframe_to_ibis(cls, 
                               df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
                               ibis_backend=None, 
                               tablename_prefix=None) -> ir.Table:
        strategy = cls._get_strategy(df=df)
        return strategy.cast_to_ibis(df=df, ibis_backend=ibis_backend, tablename_prefix=tablename_prefix)

    @classmethod
    def create_temp_table_ibis(cls, 
                            df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
                            tablename_prefix: Optional[str] = None,
                            current_ibis_backend: Optional[ibis.BaseBackend] = None,
                            target_ibis_backend: Optional[ibis.BaseBackend] = None,
                            overwrite: Optional[bool] = True,
                            create_as_view: Optional[bool] = False
            ) -> ir.Table:                               
        strategy = cls._get_strategy(df=df)
        return strategy.create_temp_table_ibis(df=df, 
                                     tablename_prefix= tablename_prefix,
                                     current_ibis_backend=current_ibis_backend,
                                     target_ibis_backend=target_ibis_backend,
                                     overwrite=overwrite,
                                     create_as_view=create_as_view
                                     )

    # Cast to Lists and Dicts!

    @classmethod
    def cast_dataframe_to_dictonary_of_lists(cls, 
                                             df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> Dict[Any, List[Any]]:
        strategy = cls._get_strategy(df)
        return strategy.cast_to_dictonary_of_lists(df)

    @classmethod
    def cast_dataframe_to_dictonary_of_series(cls, 
                                              df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> Dict[str, pl.Series]:
        strategy = cls._get_strategy(df)
        return strategy.cast_to_dictonary_of_series(df)

    @classmethod
    def cast_dataframe_to_list_of_dictionaries(cls, 
                                               df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> List[Dict[Any, Any]]:
        strategy = cls._get_strategy(df)
        return strategy.cast_to_list_of_dictionaries(df)


    @classmethod
    def get_column_names(cls, 
                         df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> List[str]:
        strategy = cls._get_strategy(df)
        return strategy.get_column_names(df)

    @classmethod
    def get_table_schema(cls, 
                         df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> ibis_schema.Schema:
        strategy = cls._get_strategy(df)
        return strategy.get_table_schema(df)


    @classmethod
    def drop(cls, 
             df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
             columns: List[str]|str):
        strategy = cls._get_strategy(df)
        return strategy.drop(df, columns)

    @classmethod
    def select(cls, 
               df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
               columns: List[str]|str):
        strategy = cls._get_strategy(df)
        return strategy.select(df, columns)

    @classmethod
    def head(cls, 
             df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
             n: int):
        strategy = cls._get_strategy(df)
        return strategy.head(df, n)

    @classmethod
    def count(cls, 
              df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> int:
        strategy = cls._get_strategy(df)
        return strategy.count(df)
    
    @classmethod
    def generate_tablename(cls, 
                           prefix: Optional[str] = None) -> str:
        return BaseDataFrameStrategy.generate_tablename(prefix=prefix)

 


    @classmethod
    def filter(cls, 
               df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
               condition: FilterNode) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]:
        strategy = cls._get_strategy(df)
        return strategy.filter(df, condition)

    @classmethod
    def create_filter_condition(cls):
        return FilterCondition


    
