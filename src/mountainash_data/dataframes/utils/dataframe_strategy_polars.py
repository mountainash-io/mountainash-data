from typing import Any,  Dict, List

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis.expr.schema as ibis_schema

from .base_dataframe_strategy import BaseDataFrameStrategy


class PolarsDataFrameUtils(BaseDataFrameStrategy):
    def _cast_to_pandas(self, df: pl.DataFrame) -> pd.DataFrame:
        return df.to_pandas()

    def _cast_to_polars(self, df: pl.DataFrame) -> pl.DataFrame:
        return df

    def _cast_to_pyarrow_table(self, df: pl.DataFrame) -> pa.Table:
        return df.to_arrow() 

    def _cast_to_pyarrow_recordbatch(self, df: pl.DataFrame, batchsize: int = 1) -> List[pa.RecordBatch]:
        return df.to_arrow().to_batches(max_chunksize=batchsize) 

    def _cast_to_dictonary_of_lists(self, df: pl.DataFrame) -> Dict[Any,List[Any]]:
        return df.to_dict(as_series=False) 

    def _cast_to_dictonary_of_series(self, df: pl.DataFrame) -> Dict[str,pl.Series]:
        return df.to_dict(as_series=True) 

    def _cast_to_list_of_dictionaries(self, df: pl.DataFrame) -> List[Dict[Any,Any]]:
        return df.to_dicts() 



    def _get_column_names(self, df: pl.DataFrame) -> List[str]:
        return df.columns

    def get_table_schema(self, df: pl.DataFrame) -> ibis_schema.Schema:
        return self._get_table_schema(df=df)


    def _drop(self, df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        return df.drop(columns)

    def _select(self, df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        return df.select(columns)

    def _head(self, df: pl.DataFrame, n: int) -> pl.DataFrame:

        if n < 0:
            raise ValueError("n must be greater than or equal to 0")

        self.validate_dataframe_input(df=df)
        return df.head(n=n)

    def _count(self, df: pl.DataFrame) -> int:
        dict_count =  df.select(pl.len()).rows(named=True) 
        return dict_count[0]["len"]