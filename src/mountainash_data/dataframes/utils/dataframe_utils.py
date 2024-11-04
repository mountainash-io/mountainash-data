# path: src/mountainash_data/dataframes/utils/dataframe_utils.py

from typing import Union, Any,  Dict, List, Optional, Sequence, Set

import collections

import pandas as pd
from mountainash_data.dataframes.utils.column_mapper.column_config import ColumnMapConfig
import polars as pl
import pyarrow as pa

import ibis
import ibis.expr.types as ir
import ibis.expr.schema as ibis_schema


from mountainash_constants import CONST_DATAFRAME_FRAMEWORK

from .dataframe_handlers import BaseDataFrameStrategy #, PandasDataFrameUtils, PolarsDataFrameUtils, PolarsLazyFrameUtils, PyArrowTableUtils, IbisDataFrameUtils, PyArrowRecordBatchUtils

from .dataframe_filters import FilterCondition, FilterNode
from ..base_dataframe import BaseDataFrame
from .dataframe_handlers.dataframe_strategy_factory import DataFrameStrategyFactory
from .pydata_converter import PyDataConverterFactory
from .column_mapper.column_mapper import ColumnMapper

# from .data_structures.data_structure_adapter import DataStructureAdapter, InputType
# from pyarrow import Table as irTable  # assuming you are using pyarrow's Table for ir.Table


class DataFrameUtils:


    ################################
    # Dataframe Creation Methods




    @classmethod
    def create_dataframe(
            cls,
            dataframe_framework: str, 
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str|Dict[str,str]]]=None,
         ) -> Union[pd.DataFrame, pl.DataFrame, ir.Table, pa.Table, pa.RecordBatch]:

        if not dataframe_framework:
            raise ValueError("dataframe_framework must be specified")
        

        if dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PANDAS.value:
            return cls.create_pandas_dataframe(data_dict=data_dict, column_dict=column_dict)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.POLARS.value:
            return cls.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.IBIS.value:
            return cls.create_ibis_dataframe(data_dict=data_dict, column_dict=column_dict)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PYARROW_TABLE.value:
            return cls.create_pyarrow_table(data_dict=data_dict, column_dict=column_dict)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PYARROW_RECORDBATCH.value:
            return cls.create_pyarrow_recordbatch(data_dict=data_dict, column_dict=column_dict)
        else:
            raise ValueError(f"Unsupported dataframe framework: {dataframe_framework}")
        

    # @classmethod
    # def _apply_column_mapping(cls, df: pl.DataFrame, column_dict: Dict[str, str|Dict[str,str]]) -> pl.DataFrame:
    #     df_cols = DataFrameUtils.get_column_names(df)
    #     new_colnames = {col: column_dict.get(col, col) for col in df_cols}
    #     return df.rename(mapping=new_colnames)
    

    @classmethod
    def create_pandas_dataframe(
            cls,
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str|Dict[str,str]]]=None,
            filter_unmapped: Optional[bool]=False
            ) -> Optional[pd.DataFrame]:

        if data_dict is None:
            return pd.DataFrame()    

        #Convert data to Polars DataFrame as an intermediate format
        pydata_strategy = PyDataConverterFactory.get_strategy(data_dict)
        pl_df = pydata_strategy.convert(data_dict)


        #Apply column mappings and type enforcement if specified
        col_mapping: ColumnMapConfig|None = ColumnMapper.create_config(column_dict, filter_unmapped)
        
        if col_mapping:
            pl_df =  ColumnMapper.apply_mapping(pl_df,col_mapping)

        #Finalise the cast to the target dataframe
        df_strategy = DataFrameStrategyFactory._get_strategy(df=pl_df)
        return df_strategy.cast_to_pandas(df=pl_df)



    @classmethod
    def create_polars_dataframe(
            cls,
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str|Dict[str,str]]]=None,
            filter_unmapped: Optional[bool]=False

            ) -> pl.DataFrame:

        if data_dict is None:
            return pl.DataFrame()    

        #Convert data to Polars DataFrame as an intermediate format
        pydata_strategy = PyDataConverterFactory.get_strategy(data_dict)
        pl_df = pydata_strategy.convert(data_dict)

        col_mapping: ColumnMapConfig|None = ColumnMapper.create_config(column_dict, filter_unmapped)
        
        if col_mapping:
            pl_df =  ColumnMapper.apply_mapping(pl_df, col_mapping )

        return pl_df
            
    @classmethod
    def create_pyarrow_recordbatch(
        cls,
        data_dict: Dict[str, Union[Sequence, List]] | List[Dict[str, Any]],
        column_dict: Optional[Dict[str, str|Dict[str,str]]] = None,
        filter_unmapped: Optional[bool]=False,
        batchsize: int = 1
    ) -> List[pa.RecordBatch]:

        if data_dict is None:
            return []    

        #Convert data to Polars DataFrame as an intermediate format
        pydata_strategy = PyDataConverterFactory.get_strategy(data_dict)
        pl_df = pydata_strategy.convert(data_dict)

        col_mapping: ColumnMapConfig|None = ColumnMapper.create_config(column_dict, filter_unmapped)
        
        if col_mapping:
            pl_df =  ColumnMapper.apply_mapping(pl_df, col_mapping )

        #Finalise the cast to the target dataframe
        df_strategy = DataFrameStrategyFactory._get_strategy(df=pl_df)
        return df_strategy.cast_to_pyarrow_recordbatch(df=pl_df, batchsize=batchsize)



    @classmethod
    def create_pyarrow_table(
        cls,
        data_dict: Dict[str, Union[Sequence, List]] | List[Dict[str, Any]],
        column_dict: Optional[Dict[str, str|Dict[str,str]]] = None,
        filter_unmapped: Optional[bool]=False

    ) -> pa.Table:
        
        if data_dict is None:
            return pa.Table()

        #Convert data to Polars DataFrame as an intermediate format
        pydata_strategy = PyDataConverterFactory.get_strategy(data_dict)
        pl_df = pydata_strategy.convert(data_dict)

        col_mapping: ColumnMapConfig|None = ColumnMapper.create_config(column_dict, filter_unmapped)
        
        if col_mapping:
            pl_df =  ColumnMapper.apply_mapping(pl_df, col_mapping )

        return cls.cast_dataframe_to_arrow(df=pl_df)


    @classmethod
    def create_ibis_dataframe(
            cls,
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str|Dict[str,str]]] = None,
            filter_unmapped: Optional[bool]=False
            ) -> Optional[ir.Table]:
        

        #Convert data to Polars DataFrame as an intermediate format
        pydata_strategy = PyDataConverterFactory.get_strategy(data_dict)
        pl_df = pydata_strategy.convert(data_dict)


        col_mapping: ColumnMapConfig|None = ColumnMapper.create_config(column_dict, filter_unmapped)
        
        if col_mapping:
            pl_df =  ColumnMapper.apply_mapping(pl_df, col_mapping )

        return cls.cast_dataframe_to_ibis(df=pl_df)

    ################################
    # Validation Methods

    #TODO: Move!
    @classmethod
    def validate_column_mapping(cls,
                                column_dict: Optional[Dict[str, str|Dict[str,str]]]=None) -> bool:
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

        if isinstance(df, list) and len(df) > 0:
            return isinstance(df[0], pa.RecordBatch)
        else:
            return isinstance(df, pa.RecordBatch)

    @classmethod
    def validate_dataframe_supported(cls, 
              df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> Optional[bool]:
        
        if df is None:
            return df
        
        strategy = DataFrameStrategyFactory._get_strategy(df)
        return strategy.validate_dataframe_input(df)


    ################################
    # Dataframe Conversion Methods

    # @classmethod
    # def _get_strategy(cls, 
    #                   df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> BaseDataFrameStrategy:
        
    #     if isinstance(df, BaseDataFrame):
    #         return IbisDataFrameUtils()
    #     elif isinstance(df, ir.Table):
    #         return IbisDataFrameUtils()
    #     elif cls._is_recordbatch(df=df):
    #         return PyArrowRecordBatchUtils()
    #     elif isinstance(df, pa.Table):
    #         return PyArrowTableUtils()
    #     elif isinstance(df, pl.DataFrame ):
    #         return PolarsDataFrameUtils()
    #     elif isinstance(df, pl.LazyFrame ):
    #         return PolarsLazyFrameUtils()
    #     elif isinstance(df, pd.DataFrame):
    #         return PandasDataFrameUtils()
    #     else:
    #         raise TypeError(f"Unsupported dataframe type. Received {type(df)}")


    @classmethod
    def cast_dataframe_to_pandas(cls, df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> pd.DataFrame:

        if df is None:
            return df

        df_strategy = DataFrameStrategyFactory._get_strategy(df=df)
        return df_strategy.cast_to_pandas(df=df)

    @classmethod
    def cast_dataframe_to_polars(cls, df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> pl.DataFrame:

        if df is None:
            return df

        df_strategy = DataFrameStrategyFactory._get_strategy(df=df)

        return df_strategy.cast_to_polars(df=df)

    @classmethod
    def cast_dataframe_to_arrow(cls, 
                                df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> pa.Table:
        
        if df is None:
            return df
        
        df_strategy = DataFrameStrategyFactory._get_strategy(df=df)
        
        return df_strategy.cast_to_pyarrow_table(df)

    @classmethod
    def cast_dataframe_to_pyarrow_recordbatch(cls, 
                                              df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
                                              batchsize: int = 1) -> List[pa.RecordBatch]:
        
        if df is None:
            return df
        
        df_strategy = DataFrameStrategyFactory._get_strategy(df=df)

        return df_strategy.cast_to_pyarrow_recordbatch(df=df, batchsize=batchsize)




    @classmethod
    def cast_dataframe_to_ibis(cls, 
                               df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
                               ibis_backend=None, 
                               tablename_prefix=None) -> ir.Table:
        
        if df is None:
            return df
        
        df_strategy = DataFrameStrategyFactory._get_strategy(df=df)

        return df_strategy.cast_to_ibis(df=df, ibis_backend=ibis_backend, tablename_prefix=tablename_prefix)


    # @classmethod
    # def cast_dataframe_to_pyarrow_reader(cls, 
    #                                           df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]
    #                                           ) -> pa.RecordBatchReader:
    #     strategy = cls._get_strategy(df=df)
    #     return strategy.cast_to_pyarrow_reader(df=df)

    @classmethod
    def create_temp_table_ibis(cls, 
                            df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
                            tablename_prefix: Optional[str] = None,
                            current_ibis_backend: Optional[ibis.BaseBackend] = None,
                            target_ibis_backend: Optional[ibis.BaseBackend] = None,
                            overwrite: Optional[bool] = True,
                            create_as_view: Optional[bool] = False
            ) -> ir.Table:
        
        if df is None:
            return df
        
        strategy = DataFrameStrategyFactory._get_strategy(df=df)
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
                                             df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> Dict[Any, List[Any]]:
        
        if df is None:
            return {}
        
        strategy = DataFrameStrategyFactory._get_strategy(df)
        return strategy.cast_to_dictonary_of_lists(df)

    @classmethod
    def cast_dataframe_to_dictonary_of_series(cls, 
                                              df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> Dict[str, pl.Series]:
        
        if df is None:
            return {}
        
        strategy = DataFrameStrategyFactory._get_strategy(df)
        return strategy.cast_to_dictonary_of_series(df)

    @classmethod
    def cast_dataframe_to_list_of_dictionaries(cls, 
                                               df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> List[Dict[Any, Any]]:
        
        if df is None:
            return []
        
        strategy = DataFrameStrategyFactory._get_strategy(df)
        return strategy.cast_to_list_of_dictionaries(df)


    ################################
    # Strategy Based Methods - Dataframne Manipulation

    @classmethod
    def get_column_names(cls, 
                         df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> List[str]:
        
        if df is None:
            return []
        
        strategy = DataFrameStrategyFactory._get_strategy(df)
        return strategy.get_column_names(df)

    @classmethod
    def get_table_schema(cls, 
                         df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> Optional[ibis_schema.Schema]:
                         
        if df is None:
            return None
        
        strategy = DataFrameStrategyFactory._get_strategy(df)
        return strategy.get_table_schema(df)


    @classmethod
    def drop(cls, 
             df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
             columns: List[str]|str) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]:
        
        if df is None:
            return df
        
        strategy = DataFrameStrategyFactory._get_strategy(df)
        return strategy.drop(df, columns)

    @classmethod
    def select(cls, 
               df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
               columns: List[str]|str) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]:
        
        if df is None:
            return df
        
        strategy = DataFrameStrategyFactory._get_strategy(df)
        return strategy.select(df, columns)

    @classmethod
    def head(cls, 
             df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
             n: int) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]:
        
        if df is None:
            return df
        
        strategy = DataFrameStrategyFactory._get_strategy(df)
        return strategy.head(df, n)

    @classmethod
    def count(cls, 
              df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> int:
        
        if df is None:
            return 0
        
        strategy = DataFrameStrategyFactory._get_strategy(df)
        return strategy.count(df)
    


    @classmethod
    def generate_tablename(cls, 
                           prefix: Optional[str] = None) -> str:
        return BaseDataFrameStrategy.generate_tablename(prefix=prefix)

 

    @classmethod
    def filter(cls, 
               df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
               condition: FilterNode) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]:
        
        if df is None:
            return df
        
        strategy = DataFrameStrategyFactory._get_strategy(df)
        return strategy.filter(df, condition)

    @classmethod
    def create_filter_condition(cls):
        return FilterCondition


    @classmethod
    def get_column_as_list(
            cls,
            df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
            column:str
        ) -> List[Any]:

        if df is None or column is None:
            return []

        strategy = DataFrameStrategyFactory._get_strategy(df)


        obj_df = strategy.select(df=df, columns=column)
        obj_dict = strategy.cast_to_dictonary_of_lists(df=obj_df)

        if obj_dict is None:
            return []
        if column not in obj_dict:
            return []

        return obj_dict[column]
    
    @classmethod
    def get_column_as_set(
            cls,
            df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
            column:str
        ) -> Set[Any]:
        
        if df is None or column is None:
            return set()  
              
        list_vals: List[Any] = cls.get_column_as_list(df=df, column=column)

        if list_vals is None or len(list_vals) == 0:
            return set()

        return set(list_vals)    


    @classmethod
    def split_dataframe_in_batches(cls, 
            df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
            batch_size: int
            ) -> List[Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]]:

        if df is None:
            return df      
          
        strategy = DataFrameStrategyFactory._get_strategy(df=df)
        return strategy.split_in_batches(df, batch_size)