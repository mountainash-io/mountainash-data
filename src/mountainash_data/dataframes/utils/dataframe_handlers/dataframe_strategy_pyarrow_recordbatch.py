from typing import Any,  Dict, List, Union

import pandas as pd
import polars as pl
import pyarrow as pa

import ibis.expr.schema as ibis_schema

from .base_dataframe_strategy import BaseDataFrameStrategy
from ..dataframe_filters import FilterNode, PyArrowFilterVisitor



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



    def _drop(self, df: pa.RecordBatch|List[pa.RecordBatch], columns: List[str]) -> List[pa.RecordBatch]:
        df_tbl: pa.Table = self._cast_to_pyarrow_table(df=df)
        batchsize: int = self._get_existing_batch_size(df=df)
        return df_tbl.drop_columns(columns).to_batches(max_chunksize=batchsize)

    def _select(self, df: pa.RecordBatch, columns: List[str]) -> List[pa.RecordBatch]:
        df_tbl: pa.Table = self._cast_to_pyarrow_table(df=df)
        batchsize: int = self._get_existing_batch_size(df=df)
        # df = self.cast_to_polars(df=df)
        return df_tbl.select(columns).to_batches(max_chunksize=batchsize)

    def _head(self, df: pa.RecordBatch|List[pa.RecordBatch], n: int) -> List[pa.RecordBatch]:

        if n < 0:
            raise ValueError("n must be greater than or equal to 0")

        df_tbl: pa.Table = self._cast_to_pyarrow_table(df=df)
        batchsize: int = self._get_existing_batch_size(df=df)

        return df_tbl.slice(length=n).to_batches(max_chunksize=batchsize)


    def _count(self, df: pa.RecordBatch|List[pa.RecordBatch]) -> int:
        df_tbl: pa.Table = self._cast_to_pyarrow_table(df=df)
        return df_tbl.num_rows
    
    def _filter(self, df: pa.RecordBatch|List[pa.RecordBatch], condition: FilterNode) -> List[pa.RecordBatch]:

        df_pa: pa.Table = self._cast_to_pyarrow_table(df=df)
    
        visitor = PyArrowFilterVisitor()
        pyarrow_condition = condition.accept(visitor)
        mask = pyarrow_condition(df_pa)
        return df_pa.filter(mask)

    def _split_in_batches(self, df: pa.RecordBatch|List[pa.RecordBatch], batch_size: int) -> List[List[pa.RecordBatch]]:

        df_pa: pa.Table = self._cast_to_pyarrow_table(df=df)
        batchsize = self._get_existing_batch_size(df=df)

        return  [df_pa.slice(i, batch_size).to_batches(batchsize) for i in range(0, df_pa.num_rows, batch_size)]    


    def _rename(self,
            df: Union[pa.RecordBatch, List[pa.RecordBatch]],
            mapping: Dict[str, str],
            **kwargs) -> List[pa.RecordBatch]:
        """Rename columns in PyArrow RecordBatch or list of RecordBatches.
        
        Args:
            df: Input PyArrow RecordBatch or list of RecordBatches
            mapping: Dictionary mapping old column names to new column names
            **kwargs: Additional keyword arguments (not used)
            
        Returns:
            List[pa.RecordBatch]: RecordBatches with renamed columns
        """
        # Convert single RecordBatch to list
        if isinstance(df, pa.RecordBatch):
            df = [df]

        # Validate against first batch's schema
        missing_cols = set(mapping.keys()) - set(df[0].schema.names)
        if missing_cols:
            raise ValueError(f"Columns not found in RecordBatch: {missing_cols}")

        # Validate no duplicate target names
        new_names = set(mapping.values())
        if len(new_names) != len(mapping):
            raise ValueError("Duplicate target column names in mapping")

        # Create new names list, replacing old names with new ones
        new_names = [mapping.get(name, name) for name in df[0].schema.names]

        # Rename each batch
        return [batch.rename_columns(new_names) for batch in df]