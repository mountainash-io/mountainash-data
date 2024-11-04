
# path: src/mountainash_data/dataframes/utils/dataframe_strategy_pandas.py

from typing import Any,  Dict, List

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis.expr.schema as ibis_schema


from .base_dataframe_strategy import BaseDataFrameStrategy
from ..dataframe_filters import FilterNode, PandasFilterVisitor



class PandasDataFrameUtils(BaseDataFrameStrategy):
    def _cast_to_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def _cast_to_polars(self, df: pd.DataFrame) -> pl.DataFrame:
        return pl.from_pandas(data=df)

    def _cast_to_pyarrow_table(self, df: pd.DataFrame) -> pa.Table:
        return pa.Table.from_pandas(df)

    def _cast_to_pyarrow_recordbatch(self, df: pd.DataFrame, batchsize: int = 1) -> List[pa.RecordBatch]:
        temp = self._cast_to_pyarrow_table(df)
        return temp.to_batches(max_chunksize=batchsize) 


    def _cast_to_dictonary_of_lists(self, df: pd.DataFrame) -> Dict[Any,List[Any]]:
        df_pa: pa.Table =  self._cast_to_pyarrow_table(df=df)
        return df_pa.to_pydict() 

    def _cast_to_dictonary_of_series(self, df: pd.DataFrame) -> Dict[str,pl.Series]:
        df_pl: pl.DataFrame =  self.cast_to_polars(df=df)
        return df_pl.to_dict(as_series=True) 

    def _cast_to_list_of_dictionaries(self, df: pd.DataFrame) -> List[Dict[Any,Any]]:
        df_pa: pa.Table =  self._cast_to_pyarrow_table(df=df)        
        return df_pa.to_pylist()


    def _get_column_names(self, df: pd.DataFrame) -> List[str]:
        return df.columns.tolist()

    def get_table_schema(self, df: pd.DataFrame) -> ibis_schema.Schema:
        return self._get_table_schema(df=df)


    def _drop(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        return df.drop(columns=columns, errors='ignore')

    def _select(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        return df[columns]

    def _head(self, df: pd.DataFrame, n: int) -> pd.DataFrame:

        # if n < 0:
        #     raise ValueError("n must be greater than or equal to 0")

        return df.head(n)

    def _count(self, df: pd.DataFrame) -> int:
        return df.shape[0]
    
    def _filter(self, df: pd.DataFrame, condition: FilterNode) -> pd.DataFrame:
        visitor = PandasFilterVisitor()
        mask = condition.accept(visitor)(df)
        return df[mask]

    def _split_in_batches(self, df: pd.DataFrame, batch_size: int) -> List[pd.DataFrame]:
        return [df[i:i + batch_size] for i in range(0, len(df), batch_size)]
