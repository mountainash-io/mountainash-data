from typing import Dict, Any, List, Optional, Union, Sequence, Tuple

import pandas as pd
import polars as pl
import ibis as ibis
import ibis.expr.types as ir

from .base_dataframe import BaseDataFrame, get_ibis_connection
from .ibis_dataframe import IbisDataFrame
from mountainash_utils_dataframes.dataframe_utils import DataFrameUtils

# from multipledispatch import dispatch
# from pyarrow import Table as irTable  # assuming you are using pyarrow's Table for ir.Table

#TODO: incorporate lineage update on exit from this class -> ie: creation of a downstream dataframe.

class PandasDataFrame(BaseDataFrame):
    
    def __init__(self, 
                 df:            Union[pd.DataFrame, Any],
                 ibis_backend:  Optional[ibis.BaseBackend] = None, 
                 tablename_prefix=None                 
                 ) -> None:
        
        super().__init__(df=df, 
                         ibis_backend=ibis_backend, 
                         tablename_prefix=tablename_prefix)

    

    def init_default_ibis_backend_schema(self):
        self.default_ibis_backend_schema = "pandas"


    def init_native_table(self, df) -> pd.DataFrame:
        

        if isinstance(df, pd.DataFrame):
            native_df = df

        else:
            #we have a new table from pandas or polars
            native_df: pd.DataFrame = DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=df)
        
        return native_df



    #amterlialise
    def materialise(self) -> pd.DataFrame:

        return self.df
        

    def materialise_ibis(self) -> pd.DataFrame:

        return DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=self.ibis_df.execute())

        
        else:
            raise ValueError("Dataframe could not be not materialised")

# Column Selection
    def select(self, columns: List[str]|str) -> "PandasDataFrame":
        new_df = DataFrameUtils.select(df=self.df, columns=columns)

        return PandasDataFrame(df=new_df)

    def drop(self, columns: List[str]|str) -> "PandasDataFrame":

        new_df = DataFrameUtils.drop(df=self.df, columns=columns)

        return PandasDataFrame(df=new_df)
       

    def mutate(self, ibis_expr: Any, **kwargs) -> "PandasDataFrame":

        new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.df).mutate(expr=ibis_expr, **kwargs).execute()

        return PandasDataFrame(df=new_df)
       

    def aggregate(self,  **kwargs) -> "PandasDataFrame":

        new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.df).aggregate( **kwargs).execute()

        return PandasDataFrame(df=new_df)
       

    def pivot_wider(self,  **kwargs) -> "PandasDataFrame":

        new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.df).pivot_wider( **kwargs).execute()

        return PandasDataFrame(df=new_df)
       
    def pivot_longer(self, **kwargs) -> "PandasDataFrame":

        new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.df).pivot_longer( **kwargs).execute()

        return PandasDataFrame(df=new_df)


#Column Metadata
    def get_column_names(self) -> list:
        return self._get_column_names()      


#Joins
    

    def _resolve_join_backend(self, 
                                   right: "BaseDataFrame",
                                   execute_on: Optional[str] = None
                                   ) -> Tuple[ir.Table, ir.Table]:

        if execute_on and execute_on not in ["left", "right"]:
            raise ValueError("execute_on must be one of 'left', 'right' or None")


        #right has the same backed
        if isinstance(right, PandasDataFrame):

            left_table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.df)
            right_table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=right.df)

        #right a different backend or not ibis
        else:

            if execute_on == "left":

                #create temp table on left (self) backend
                left_table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.df)
                right_table =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=right.df))

            elif execute_on == "right":
                #create temp table on right

                if isinstance(right.df, ir.Table):
                    right_table = right.df
                    left_source_table = self.to_arrow()
                    left_table = right.create_table(name=self.generate_tablename("left_table"), obj=left_source_table, overwrite=True)

                else:
                    right_table = right.df
                    left_source_table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=right.df))
                    left_table = right.create_table(name=self.generate_tablename("left_table"), obj=left_source_table, overwrite=True)


            else:
                #bring both into memory
                left_table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.df)
                right_table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=right.df)


        return (left_table, right_table)




    def inner_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None) -> "PandasDataFrame":


        left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

        new_df: ir.Table = left_table.inner_join(right=right_table, predicates=predicates).execute()
 
        return PandasDataFrame(df=new_df)

    def left_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None) -> "PandasDataFrame":

        left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

        new_df: ir.Table = left_table.left_join(right=right_table, predicates=predicates).execute()
 
        return PandasDataFrame(df=new_df)

    def outer_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None) -> "PandasDataFrame":

        left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

        new_df: ir.Table = left_table.outer_join(right=right_table, predicates=predicates).execute()
 
        return PandasDataFrame(df=new_df)



# Row Selection


    def filter(self, expr) -> "PandasDataFrame":
        """Filter rows based on the given expression."""

        filtered_df = self._filter(expr=expr)
        return PandasDataFrame(filtered_df, ibis_table_lineage=self.get_lineage())
                


    def head(self, n: int) -> "PandasDataFrame":
        """Take the first n rows."""

        head_df = self._head(n)
        return PandasDataFrame(head_df, ibis_table_lineage=self.get_lineage())

        
### Aggregates        
    
    def count(self) -> int:
        return self._count()


### Query        
    
    def sql(self, query:str) -> "PandasDataFrame":
        query_df = self._sql(query)

        native_df = DataFrameUtils.cast_dataframe_to_pandas(query_df)        
        return PandasDataFrame(native_df, ibis_table_lineage=self.get_lineage())


# Ibis Interface    
    def set_ibis_backend(self) -> None:
        ibis.set_backend(backend="polars")
        
    def init_ibis_connection(self):    

        if self.df is not None:
            self.ibis_backend = get_ibis_connection("pandas")
            # self.ibis_backend = ibis.pandas.connect({self.tablename: self.df})