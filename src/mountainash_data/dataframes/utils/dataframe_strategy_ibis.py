
from typing import Any,  Dict, List

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis.expr.types as ir
import ibis.expr.schema as ibis_schema
import ibis

from .base_dataframe_strategy import BaseDataFrameStrategy
from .filter import FilterVisitor, ColumnCondition, LogicalCondition, FilterNode, IbisFilterVisitor


class IbisDataFrameUtils(BaseDataFrameStrategy):

    def _cast_to_pandas(self, df: ir.Table) -> pd.DataFrame:
        return df.execute()

    def _cast_to_polars(self, df: ir.Table) -> pl.DataFrame:
        return df.to_polars()

    def _cast_to_pyarrow_table(self, df: ir.Table) -> pa.Table:
        return df.to_pyarrow()

    def _cast_to_pyarrow_recordbatch(self, df: ir.Table, batchsize: int = 1) -> List[pa.RecordBatch]:
        return df.to_pyarrow().to_batches(max_chunksize=batchsize)
    

    def _cast_to_dictonary_of_lists(self, df: ir.Table) -> Dict[Any,List[Any]]:
        # return df.to_polars().to_dict(as_series=False)
        return df.to_pyarrow().to_pydict()

    def _cast_to_dictonary_of_series(self, df: ir.Table) -> Dict[str,pl.Series]:
        return df.to_polars().to_dict(as_series=True)

    def _cast_to_list_of_dictionaries(self, df: ir.Table) -> List[Dict[Any,Any]]:
        # return df.to_polars().to_dicts()
        return df.to_pyarrow().to_pylist()

    def _get_column_names(self, df: ir.Table) -> List[str]:
        return df.columns

    #Opposite pattern
    def get_table_schema(self, df: ir.Table) -> ibis_schema.Schema:
        return df.schema()


    def _drop(self, df: ir.Table, columns: List[str]) -> ir.Table:
        return df.drop(*columns)

    def _select(self, df: ir.Table, columns: List[str]) -> ir.Table:
        return df.select(columns)

    def _head(self, df: ir.Table, n) -> ir.Table:
        return df.head(n)

    def _count(self, df: ir.Table) -> int:
        return df.count().execute()

    def _filter(self, df: ir.Table, condition: FilterNode) -> ir.Table:
        visitor = IbisFilterVisitor()
        ibis_callable = condition.accept(visitor)

        return df.filter(ibis_callable)