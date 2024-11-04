from typing import Union, List, Any

import pandas as pd
import polars as pl
import pyarrow as pa

import ibis.expr.types as ir
from ...base_dataframe import BaseDataFrame

from . import BaseDataFrameStrategy, IbisDataFrameUtils, PolarsDataFrameUtils, PandasDataFrameUtils, PolarsLazyFrameUtils, PyArrowTableUtils, PyArrowRecordBatchUtils


class DataFrameStrategyFactory():

    @classmethod
    def _get_strategy(cls, 
                      df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> BaseDataFrameStrategy:
        
        if isinstance(df, BaseDataFrame):
            return IbisDataFrameUtils()
        elif isinstance(df, ir.Table):
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
    def _is_recordbatch(cls, df: Any) -> bool:

        if isinstance(df, list) and len(df) > 0:
            return isinstance(df[0], pa.RecordBatch)
        else:
            return isinstance(df, pa.RecordBatch)        