
# path: src/mountainash_data/dataframes/utils/dataframe_strategy_ibis.py

import re
from typing import Any,  Dict, List, Optional

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis.expr.types as ir
import xarray as xr

import ibis.expr.schema as ibis_schema

from .base_dataframe_strategy import BaseDataFrameStrategy
from ..dataframe_filters import FilterNode, IbisFilterVisitor


class XArrayDataFrameUtils(BaseDataFrameStrategy):

    def _cast_to_pandas(self, df: xr.DataArray, index: Optional[List[str]]) -> pd.DataFrame:
        pd_df =  df.to_pandas()

        if index:
            pd_df.set_index(index, inplace=True)
        return pd_df

    def _cast_to_polars(self, df: xr.DataArray) -> pl.DataFrame:
        return df.to_polars()

    def _cast_to_pyarrow_table(self, df: xr.DataArray) -> pa.Table:
        return df.to_pyarrow()

    def _cast_to_pyarrow_recordbatch(self, df: xr.DataArray, batchsize: int = 1) -> List[pa.RecordBatch]:
        temp = self._cast_to_pyarrow_table(df)
        return temp.to_batches(max_chunksize=batchsize) 
    

    def _cast_to_dictonary_of_lists(self, df: xr.DataArray) -> Dict[Any,List[Any]]:
        # return df.to_polars().to_dict(as_series=False)
        return df.to_pyarrow().to_pydict()

    def _cast_to_dictonary_of_series(self, df: xr.DataArray) -> Dict[str,pl.Series]:
        return df.to_polars().to_dict(as_series=True)

    def _cast_to_list_of_dictionaries(self, df: xr.DataArray) -> List[Dict[Any,Any]]:
        # return df.to_polars().to_dicts()
        return df.to_pyarrow().to_pylist()

    def _get_column_names(self, df: xr.DataArray) -> List[str]:
        return df.columns

    #Opposite pattern
    def get_table_schema(self, df: xr.DataArray) -> ibis_schema.Schema:
        return df.schema()


    def _drop(self, df: xr.DataArray, columns: List[str]) -> xr.DataArray:
        return df.drop(*columns)

    def _select(self, df: xr.DataArray, columns: List[str]) -> xr.DataArray:
        return df.select(columns)

    def _head(self, df: xr.DataArray, n) -> xr.DataArray:
        return df.head(n)

    def _count(self, df: xr.DataArray) -> int:
        return df.count().execute()

    def _filter(self, df: xr.DataArray, condition: FilterNode) -> xr.DataArray:
        visitor = IbisFilterVisitor()
        ibis_callable = condition.accept(visitor)

        return df.filter(ibis_callable)
    
    def _split_in_batches(self, df: xr.DataArray, batch_size: int) -> List[xr.DataArray]:
        total_rows = df.count().execute()
        return [df.limit(batch_size, offset=i) for i in range(0, total_rows, batch_size)]    
    
    def _rename(self,
            df: xr.DataArray,
            mapping: Dict[str, str],
            **kwargs) -> xr.DataArray:
        """Rename columns in an XArray Table.
        
        Args:
            df: Input Ibis Table
            mapping: Dictionary mapping old column names to new column names
            **kwargs: Additional keyword arguments passed to ibis rename
            
        Returns:
            xr.DataArray: Table with renamed columns
        """
        # Validate column existence
        missing_cols = set(mapping.keys()) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Columns not found in Table: {missing_cols}")

        # Validate no duplicate target names
        new_names = set(mapping.values())
        if len(new_names) != len(mapping):
            raise ValueError("Duplicate target column names in mapping")

        # Use ibis native rename
        return df.rename(mapping)    