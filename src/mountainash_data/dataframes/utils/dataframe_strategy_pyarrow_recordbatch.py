from typing import Any,  Dict, List

import pandas as pd
import polars as pl
import pyarrow as pa

import ibis.expr.schema as ibis_schema

from .base_dataframe_strategy import BaseDataFrameStrategy
from .filter import FilterNode, PyArrowFilterVisitor


class PyArrowRecordBatchUtils(BaseDataFrameStrategy):

    def _get_existing_batch_size(self, df: pa.RecordBatch|List[pa.RecordBatch]) -> int:

        if isinstance(df, list):
            return df[0].num_rows                    
        elif isinstance(df, pa.RecordBatch):
            return df.num_rows
        else:
            raise ValueError("df must be either a list of RecordBatch or a RecordBatch")


    def _cast_to_pandas(self, df: pa.RecordBatch|List[pa.RecordBatch] ) -> pd.DataFrame:

        df_tbl = self._cast_to_pyarrow_table(df=df)
        return df_tbl.to_pandas() 

    def _cast_to_polars(self, df: pa.RecordBatch|List[pa.RecordBatch]) -> pl.DataFrame:
        df_tbl = self._cast_to_pyarrow_table(df=df)
        return pl.DataFrame(data=df_tbl) 

    def _cast_to_pyarrow_table(self, df: pa.RecordBatch|List[pa.RecordBatch]) -> pa.Table:

        if isinstance(df, pa.RecordBatch):
            df = [df]
        return pa.Table.from_batches(df)
    

    def _cast_to_pyarrow_recordbatch(self, df: pa.RecordBatch|List[pa.RecordBatch], batchsize: int = 1) -> List[pa.RecordBatch]:

        #If the batchsize already matches, don't do anything
        if batchsize == self._get_existing_batch_size(df=df):
            return df
        
        #Otherwise , convert to pyarrow table and then to new batchesize
        df_tbl = self._cast_to_pyarrow_table(df=df)
        return df_tbl.to_batches(max_chunksize=batchsize) 

    def _cast_to_dictonary_of_lists(self, df: pa.RecordBatch|List[pa.RecordBatch]) -> Dict[Any,List[Any]]:
        df_tbl = self._cast_to_pyarrow_table(df=df)
        return df_tbl.to_pydict()

    def _cast_to_dictonary_of_series(self, df: pa.RecordBatch|List[pa.RecordBatch]) -> Dict[str,pl.Series]:
        df_tbl = self._cast_to_pyarrow_table(df=df)
        df_pl: pl.DataFrame =  self.cast_to_polars(df=df_tbl)
        return df_pl.to_dict(as_series=True) 

    def _cast_to_list_of_dictionaries(self, df: pa.RecordBatch|List[pa.RecordBatch]) -> List[Dict[Any,Any]]:
        df_tbl = self._cast_to_pyarrow_table(df=df)
        return df_tbl.to_pylist()


    def _get_column_names(self, df: pa.RecordBatch|List[pa.RecordBatch]) -> List[str]:
        df_tbl = self._cast_to_pyarrow_table(df=df)
        return df_tbl.column_names

    def get_table_schema(self, df: pa.RecordBatch|List[pa.RecordBatch]) -> ibis_schema.Schema:
        df_tbl = self._cast_to_pyarrow_table(df=df)
        return self._get_table_schema(df=df_tbl)



    def _drop(self, df: pa.RecordBatch|List[pa.RecordBatch], columns: List[str]) -> pa.Table:
        df_tbl: pa.Table = self._cast_to_pyarrow_table(df=df)
        batchsize: int = self._get_existing_batch_size(df=df)
        return df_tbl.drop_columns(columns).to_batches(max_chunksize=batchsize)

    def _select(self, df: pa.RecordBatch, columns: List[str]) -> pa.Table:
        df_tbl: pa.Table = self._cast_to_pyarrow_table(df=df)
        batchsize: int = self._get_existing_batch_size(df=df)
        # df = self.cast_to_polars(df=df)
        return df_tbl.select(columns).to_batches(max_chunksize=batchsize)

    def _head(self, df: pa.RecordBatch|List[pa.RecordBatch], n: int) -> pa.Table:

        print(f"df: {df} of type: {type(df)}")

        if n < 0:
            raise ValueError("n must be greater than or equal to 0")

        df_tbl: pa.Table = self._cast_to_pyarrow_table(df=df)
        batchsize: int = self._get_existing_batch_size(df=df)

        return df_tbl.slice(length=n).to_batches(max_chunksize=batchsize)


    def _count(self, df: pa.RecordBatch|List[pa.RecordBatch]) -> int:
        df_tbl: pa.Table = self._cast_to_pyarrow_table(df=df)
        return df_tbl.num_rows
    
    def _filter(self, df: pa.RecordBatch|List[pa.RecordBatch], condition: FilterNode) -> pa.Table:

        df = self._cast_to_pyarrow_table(df=df)
    
        visitor = PyArrowFilterVisitor()
        pyarrow_condition = condition.accept(visitor)
        mask = pyarrow_condition(df)
        return df.filter(mask)
