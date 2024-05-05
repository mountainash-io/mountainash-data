from typing import Union, List, Dict, Any, Optional, Sequence

import pandas as pd
import polars as pl
import ibis as ibis
import pyarrow as pa
import ibis.expr.types as ir

from .base_dataframe import BaseDataFrame
from mountainash_utils_dataframes.dataframe_utils import DataFrameUtils
# from multipledispatch import dispatch
# from pyarrow import Table as irTable  # assuming you are using pyarrow's Table for ir.Table

class PolarsDataFrame(BaseDataFrame):

    def __init__(self, 
                 df:            Union[pl.DataFrame, pl.LazyFrame, Any],
                 ibis_backend:  Optional[ibis.BaseBackend] = None, 
                 tablename_prefix=None                 
                 ) -> None:
        
        super().__init__(df=df, 
                         ibis_backend=ibis_backend, 
                         tablename_prefix=tablename_prefix)

    

    def init_default_ibis_backend_schema(self):
        self.default_ibis_backend_schema = "polars"


    def init_native_table(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, pa.Table, ir.Table]) -> Union[pl.DataFrame, pl.LazyFrame]:
        
        if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            native_df = df

        else:
            #we have a new table from pandas or polars
            native_df: Union[pl.DataFrame, pl.LazyFrame] = DataFrameUtils.cast_dataframe_to_polars(df_dataframe=df)
        
        return native_df

#Materialisation Status
    def is_materialised(self) -> bool:
        return isinstance(self.native_df, pl.DataFrame)


    def materialise(self) -> pl.DataFrame:

        if isinstance(self.native_df, pl.LazyFrame):
            return self.native_df.collect()
        
        if isinstance(self.native_df, pl.DataFrame):
            return self.native_df
        
        else:
            raise ValueError("Dataframe could not be not materialised")



# Column Selection
    def select(self, columns: List[str]|str) -> "PolarsDataFrame":

        new_df: pl.DataFrame = self._select_ibis(columns=columns)

        return PolarsDataFrame(df=new_df, ibis_table_lineage=self.get_lineage())

    def drop(self, columns: List[str]|str) -> "PolarsDataFrame":

        new_df: pl.DataFrame = self._drop_ibis(columns=columns)

        return PolarsDataFrame(df=new_df, ibis_table_lineage=self.get_lineage())
       

    def mutate(self, ibis_expr: Sequence[ir.Expr], **kwargs) -> "PolarsDataFrame":

        new_df: pl.DataFrame = self._mutate_ibis(ibis_expr=ibis_expr, **kwargs)

        return PolarsDataFrame(df=new_df, ibis_table_lineage=self.get_lineage())
       

    def aggregate(self,  **kwargs) -> "PolarsDataFrame":

        new_df: pl.DataFrame = self._aggregate_ibis( **kwargs)

        return PolarsDataFrame(df=new_df, ibis_table_lineage=self.get_lineage())
       

    def pivot_wider(self,  **kwargs) -> "PolarsDataFrame":

        new_df: pl.DataFrame = self._pivot_wider_ibis( **kwargs)

        return PolarsDataFrame(df=new_df, ibis_table_lineage=self.get_lineage())
       
    def pivot_longer(self, **kwargs) -> "PolarsDataFrame":

        new_df: pl.DataFrame = self._pivot_longer_ibis( **kwargs)

        return PolarsDataFrame(df=new_df, ibis_table_lineage=self.get_lineage())


#Column Metadata
    def get_column_names(self) -> list:
        return self._get_column_names()      


#Joins
    def inner_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None) -> "PolarsDataFrame":

        new_df: pl.DataFrame = self._inner_join_ibis(right, predicates, execute_on=execute_on)

        return PolarsDataFrame(df=new_df, ibis_table_lineage=self.get_lineage())

    def left_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None) -> "PolarsDataFrame":

        new_df: pl.DataFrame = self._left_join_ibis(right, predicates, execute_on=execute_on)

        return PolarsDataFrame(df=new_df, ibis_table_lineage=self.get_lineage())

    def outer_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None) -> "PolarsDataFrame":

        new_df: pl.DataFrame = self._outer_join_ibis(right, predicates, execute_on=execute_on)

        return PolarsDataFrame(df=new_df, ibis_table_lineage=self.get_lineage())



# Row Selection
    def filter(self, expr) -> "PolarsDataFrame":
        """Filter rows based on the given expression."""

        filtered_df = self._filter_ibis(expr=expr)
        return PolarsDataFrame(filtered_df, ibis_table_lineage=self.get_lineage())
                


    def head(self, n: int) -> "PolarsDataFrame":
        """Take the first n rows."""

        head_df = self._head_ibis(n)
        return PolarsDataFrame(head_df, ibis_table_lineage=self.get_lineage())

        
### Aggregates        
    
    def count(self) -> int:
        return self._count()


### Query        
    
    def sql(self, query:str) -> "PolarsDataFrame":

        query_df = self._sql(query)
        native_df = DataFrameUtils.cast_dataframe_to_polars(df_dataframe=query_df)
        print(f"native_df: {type(native_df)}")

        return PolarsDataFrame(native_df, ibis_table_lineage=self.get_lineage())

#ibis connections:
    def set_ibis_backend(self) -> None:
        ibis.set_backend(backend="polars")

    def init_ibis_connection(self):    
        if self.df is not None:
            self.ibis_backend = get_ibis_connection("polars")

            # self.ibis_backend = ibis.polars.connect()#{self.tablename: self.df})






    # def create_filter_expression(self, column_name: str, operator: str, value: Any):
    #     if operator == '>':
    #         return pl.col(column_name) > value
    #     elif operator == '<':
    #         return pl.col(column_name) < value
    #     elif operator == '==':
    #         return pl.col(column_name) == value
    #     elif operator == '!=':
    #         return pl.col(column_name) != value
    #     elif operator == 'isin':
    #         return pl.col(column_name).is_in(value)
    #     else:
    #         raise Exception(f"Operator {operator} not supported")