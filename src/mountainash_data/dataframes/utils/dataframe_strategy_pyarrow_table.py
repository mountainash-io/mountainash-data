from typing import Any,  Dict, List

import pandas as pd
import polars as pl
import pyarrow as pa

import ibis.expr.schema as ibis_schema

from .base_dataframe_strategy import BaseDataFrameStrategy


class PyArrowTableUtils(BaseDataFrameStrategy):
    def _cast_to_pandas(self, df: pa.Table) -> pd.DataFrame:
        return df.to_pandas() 

    def _cast_to_polars(self, df: pa.Table) -> pl.DataFrame:
        return pl.DataFrame(data=df) 

    def _cast_to_pyarrow_table(self, df: pa.Table) -> pa.Table:
        return df

    def _cast_to_pyarrow_recordbatch(self, df: pa.Table, batchsize: int = 1) -> List[pa.RecordBatch]:
        return df.to_batches(max_chunksize=batchsize) 

    def _cast_to_dictonary_of_lists(self, df: pa.Table) -> Dict[Any,List[Any]]:
        return df.to_pydict()

    def _cast_to_dictonary_of_series(self, df: pa.Table) -> Dict[str,pl.Series]:
        df =  self.cast_to_polars(df=df)
        return df.to_dict(as_series=True) 

    def _cast_to_list_of_dictionaries(self, df: pa.Table) -> List[Dict[Any,Any]]:
        return df.to_pylist()


    def _get_column_names(self, df: pa.Table) -> List[str]:
        return df.column_names

    def get_table_schema(self, df: pa.Table) -> ibis_schema.Schema:
        return self._get_table_schema(df=df)



    def _drop(self, df: pa.Table, columns: List[str]) -> pa.Table:
        # df =  self.cast_to_polars(df=df)
        return df.drop_columns(columns)

    def _select(self, df: pa.Table, columns: List[str]) -> pa.Table:
        # df = self.cast_to_polars(df=df)
        return df.select(columns).to_arrow()

    def _head(self, df: pa.Table, n: int) -> pa.Table:
        if n < 0:
            raise ValueError("n must be greater than or equal to 0")

        return df.slice(length=n)

    def _count(self, df: pa.Table) -> int:
        return df.num_rows