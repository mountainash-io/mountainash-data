# path: src/mountainash_data/dataframes/utils/dataframe_strategy_polars_lazyframe.py

from typing import Any,  Dict, List, Union, Callable

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis.expr.schema as ibis_schema

from .base_dataframe_strategy import BaseDataFrameStrategy
from ..dataframe_filters import FilterNode, PolarsFilterVisitor



class PolarsLazyFrameUtils(BaseDataFrameStrategy):
    def _cast_to_pandas(self, df: pl.LazyFrame) -> pd.DataFrame:
        return df.collect().to_pandas()  

    def _cast_to_polars(self, df: pl.LazyFrame) -> pl.DataFrame:
        return df.collect()

    def _cast_to_pyarrow_table(self, df: pl.LazyFrame) -> pa.Table:
        return df.collect().to_arrow() 

    def _cast_to_pyarrow_recordbatch(self, df: pl.LazyFrame, batchsize: int = 1) -> List[pa.RecordBatch]:
        temp = self._cast_to_pyarrow_table(df)
        return temp.to_batches(max_chunksize=batchsize) 

    def _cast_to_dictonary_of_lists(self, df: pl.LazyFrame) -> Dict[Any,List[Any]]:
        # return df.collect().to_dict(as_series=False) 
        return df.collect().to_arrow().to_pydict()

    def _cast_to_dictonary_of_series(self, df: pl.LazyFrame) -> Dict[str,pl.Series]:
        return df.collect().to_dict(as_series=True) 

    def _cast_to_list_of_dictionaries(self, df: pl.LazyFrame) -> List[Dict[Any,Any]]:
        # return df.collect().to_dicts()
        return df.collect().to_arrow().to_pylist()


    def _get_column_names(self, df: pl.LazyFrame) -> List[str]:
        return df.columns

    def get_table_schema(self, df: pl.LazyFrame) -> ibis_schema.Schema:
        return self._get_table_schema(df=df)



    def _drop(self, df: pl.LazyFrame, columns: List[str]) -> pl.LazyFrame:
        return df.drop(columns)

    def _select(self, df: pl.LazyFrame, columns: List[str]) -> pl.LazyFrame:
        return df.select(columns)

    def _head(self, df: pl.LazyFrame, n: int) -> pl.LazyFrame:

        if n < 0:
            raise ValueError("n must be greater than or equal to 0")

        return df.head(n=n)

    def _count(self, df: pl.LazyFrame) -> int:
        dict_count =  df.select(pl.len()).collect().rows(named=True) 
        return dict_count[0]["len"]
    
    def _filter(self, df: Union[pl.DataFrame, pl.LazyFrame], condition: FilterNode) -> Union[pl.DataFrame, pl.LazyFrame]:
        
        visitor = PolarsFilterVisitor()
        polars_callable: Callable = condition.accept(visitor)
        polars_condition = polars_callable(df)
        
        return df.filter(polars_condition)
    
    def _split_in_batches(self, df: pl.DataFrame, batch_size: int) -> List[pl.DataFrame]:
        return [df.slice(i, batch_size) for i in range(0, len(df), batch_size)]    