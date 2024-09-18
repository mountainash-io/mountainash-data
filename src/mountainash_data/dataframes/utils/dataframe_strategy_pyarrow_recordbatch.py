from typing import Any,  Dict, List

import pandas as pd
import polars as pl
import pyarrow as pa

import ibis.expr.schema as ibis_schema

from .base_dataframe_strategy import BaseDataFrameStrategy


class PyArrowRecordBatchUtils(BaseDataFrameStrategy):

    def _cast_to_pandas(self, df: pa.RecordBatch) -> pd.DataFrame:
        return df.to_pandas() 

    def _cast_to_polars(self, df: pa.RecordBatch) -> pl.DataFrame:
        return pl.DataFrame(data=df) 

    def _cast_to_pyarrow_table(self, df: pa.RecordBatch) -> pa.Table:
        return pa.Table.from_batches(df)

    def _cast_to_pyarrow_recordbatch(self, df: pa.RecordBatch) -> pa.RecordBatch:
        return df 


    def _cast_to_dictonary_of_lists(self, df: pa.RecordBatch) -> Dict[Any,List[Any]]:
        return df.to_pydict()

    def _cast_to_dictonary_of_series(self, df: pa.RecordBatch) -> Dict[str,pl.Series]:
        df =  self.cast_to_polars(df=df)
        return df.to_dict(as_series=True) 

    def _cast_to_list_of_dictionaries(self, df: pa.RecordBatch) -> List[Dict[Any,Any]]:
        return df.to_pylist()


    def _get_column_names(self, df: pa.RecordBatch) -> List[str]:
        return df.column_names

    def get_table_schema(self, df: pa.RecordBatch) -> ibis_schema.Schema:
        return self._get_table_schema(df=df)



    def _drop(self, df: pa.RecordBatch, columns: List[str]) -> pa.Table:
        return df.drop_columns(columns)

    def _select(self, df: pa.RecordBatch, columns: List[str]) -> pa.Table:
        # df = self.cast_to_polars(df=df)
        return df.select(columns)

    def _head(self, df: pa.RecordBatch, n: int) -> pa.Table:
        if n < 0:
            raise ValueError("n must be greater than or equal to 0")

        df_pl: pl.DataFrame = self.cast_to_polars(df=df)
        return df_pl.head(n=n).to_arrow()

    def _count(self, df: pa.RecordBatch) -> int:
        return df.num_rows