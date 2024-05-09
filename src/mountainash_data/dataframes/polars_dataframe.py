# from typing import Union, List, Dict, Any, Optional, Sequence, Tuple

# import pandas as pd
# import polars as pl
# import ibis as ibis
# import pyarrow as pa
# import ibis.expr.types as ir

# from .base_dataframe import BaseDataFrame
# from mountainash_data.dataframe_utils import DataFrameUtils
# # from multipledispatch import dispatch
# # from pyarrow import Table as irTable  # assuming you are using pyarrow's Table for ir.Table

# class PolarsDataFrame(BaseDataFrame):

#     def __init__(self, 
#                  df:            Union[pl.DataFrame, pl.LazyFrame, Any],
#                  ibis_backend:  Optional[ibis.BaseBackend] = None, 
#                  tablename_prefix=None                 
#                  ) -> None:
        
#         super().__init__(df=df, 
#                          ibis_backend=ibis_backend, 
#                          tablename_prefix=tablename_prefix)

    

#     def init_default_ibis_backend_schema(self):
#         self.default_ibis_backend_schema = "polars"


#     def init_native_table(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, pa.Table, ir.Table]) -> Union[pl.DataFrame, pl.LazyFrame]:
        
#         if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
#             native_df = df

#         else:
#             #we have a new table from pandas or polars
#             native_df: Union[pl.DataFrame, pl.LazyFrame] = DataFrameUtils.cast_dataframe_to_polars(df_dataframe=df)
        
#         return native_df

# #Materialisation Status
#     def is_materialised(self) -> bool:
#         return isinstance(self.native_df, pl.DataFrame)


#     def materialise(self) -> pl.DataFrame:

#         if isinstance(self.native_df, pl.LazyFrame):
#             return self.native_df.collect()
        
#         if isinstance(self.native_df, pl.DataFrame):
#             return self.native_df
        
#         else:
#             raise ValueError("Dataframe could not be not materialised")



# # Column Selection
#     def select(self, columns: Any) -> "PolarsDataFrame":

#         new_df = self._select_native(columns=columns)

#         return PolarsDataFrame(df=new_df, ibis_backend=self.ibis_backend)


#     def drop(self, columns: Any) -> "PolarsDataFrame":

#         new_df = self._drop_native(columns=columns)

#         return PolarsDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       



#     def mutate(self, ibis_expr: Any, **kwargs) -> "PolarsDataFrame":

#         new_df = self._mutate_ibis(ibis_expr=ibis_expr, **kwargs)

#         return PolarsDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
#     def _mutate_native(self, ibis_expr: Any, **kwargs) -> Union[pl.DataFrame, pl.LazyFrame]:

#         new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df).mutate(expr=ibis_expr, **kwargs).execute()

#         native_df = DataFrameUtils.cast_dataframe_to_polars(df_dataframe=new_df) 

#         return native_df


#     def aggregate(self,  **kwargs) -> "PolarsDataFrame":

#         new_df = self._aggregate_ibis( **kwargs)

#         return PolarsDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
#     def _aggregate_native(self, **kwargs) -> Union[pl.DataFrame, pl.LazyFrame]:

#         new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df).aggregate( **kwargs).execute()

#         native_df = DataFrameUtils.cast_dataframe_to_polars(df_dataframe=new_df) 

#         return native_df   


#     def pivot_wider(self,  **kwargs) -> "PolarsDataFrame":

#         new_df = self._pivot_wider_ibis( **kwargs)

#         return PolarsDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
#     def _pivot_wider_native(self, **kwargs) -> Union[pl.DataFrame, pl.LazyFrame]:

#         new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df).pivot_wider( **kwargs).execute()

#         native_df = DataFrameUtils.cast_dataframe_to_polars(df_dataframe=new_df) 

#         return native_df 

#     def pivot_longer(self, **kwargs) -> "PolarsDataFrame":

#         new_df  = self._pivot_longer_ibis( **kwargs)

#         return PolarsDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _pivot_longer_native(self, **kwargs) -> Union[pl.DataFrame, pl.LazyFrame]:

#         new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df).pivot_longer(**kwargs).execute()

#         native_df = DataFrameUtils.cast_dataframe_to_polars(df_dataframe=new_df) 

#         return native_df 


# #Column Metadata
#     def get_column_names(self) -> list:
#         return self._get_column_names_ibis()      


# #Joins
    
#     def _resolve_join_backend_native(self, 
#                                    right: "BaseDataFrame",
#                                    execute_on: Optional[str] = None
#                                    ) -> Tuple[ir.Table, ir.Table]:

#         if execute_on and execute_on not in ["left", "right"]:
#             raise ValueError("execute_on must be one of 'left', 'right' or None")

#         # Known: Left side is Polars

#         #right has the same backed
#         if isinstance(right, PolarsDataFrame):

#             left_table: ir.Table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df)
#             right_table: ir.Table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=right.native_df)

#         #right a different backend or not ibis
#         else:

#             if execute_on == "left":

#                 #create temp table on left (self) backend
#                 # left_table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df)
#                 # right_table =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=DataFrameUtils.cast_dataframe_to_polars(df_dataframe=right.native_df))

#                 left_table =    self.ibis_df   

#                 right_source_table = right.to_arrow()
#                 right_table = self.create_table_ibis(name=self.generate_tablename(prefix="right_table"), obj=right_source_table, overwrite=True)


#             elif execute_on == "right":

#                 #create temp table on right
#                 if isinstance(right.native_df, ir.Table):
#                     right_table = right.ibis_df
#                     left_source_table = self.to_arrow()
#                     left_table = right.create_table_ibis(name=self.generate_tablename(prefix="left_table"), obj=left_source_table, overwrite=True)

#                 else:
#                     #Right side is not ibis, and not polars. So want both sides to be on the same backend as RHS
#                     right_source_table = right.to_arrow_native()
#                     right_table = right.create_table_ibis(name=self.generate_tablename(prefix="right_table"), obj=right_source_table, overwrite=True)

#                     left_source_table = self.to_arrow_native()
#                     left_table = right.create_table_ibis(name=self.generate_tablename(prefix="left_table"), obj=left_source_table, overwrite=True)


#             else:
#                 #bring both into memory
#                 left_table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df)
#                 right_table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=right.native_df)


#         return (left_table, right_table)

#     def inner_join(self,             
#                    right: "BaseDataFrame", 
#                    predicates: Any,
#                    execute_on: Optional["str"] = None) -> "PolarsDataFrame":

#         new_df: pl.DataFrame = self._inner_join_ibis(right, predicates, execute_on=execute_on)

#         return PolarsDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _inner_join_native(self,             
#                    right: "BaseDataFrame", 
#                    predicates: Any,
#                    execute_on: Optional["str"] = None) -> Union[pl.DataFrame, pl.LazyFrame]:

#         left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

#         new_df: ir.Table = left_table.inner_join(right=right_table, predicates=predicates).execute()
 
#         return new_df


#     def left_join(self,             
#                    right: "BaseDataFrame", 
#                    predicates: Any,
#                    execute_on: Optional["str"] = None) -> "PolarsDataFrame":

#         new_df: pl.DataFrame = self._left_join_ibis(right, predicates, execute_on=execute_on)

#         return PolarsDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _left_join_native(self,             
#                    right: "BaseDataFrame", 
#                    predicates: Any,
#                    execute_on: Optional["str"] = None) -> Union[pl.DataFrame, pl.LazyFrame]:

#         left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

#         new_df: ir.Table = left_table.left_join(right=right_table, predicates=predicates).execute()
 
#         return new_df

#     def outer_join(self,             
#                    right: "BaseDataFrame", 
#                    predicates: Any,
#                    execute_on: Optional["str"] = None) -> "PolarsDataFrame":

#         new_df: pl.DataFrame = self._outer_join_ibis(right, predicates, execute_on=execute_on)

#         return PolarsDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _outer_join_native(self,             
#                    right: "BaseDataFrame", 
#                    predicates: Any,
#                    execute_on: Optional["str"] = None) -> Union[pl.DataFrame, pl.LazyFrame]:

#         left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

#         new_df: ir.Table = left_table.outer_join(right=right_table, predicates=predicates).execute()
 
#         return new_df

# # Row Selection
#     def filter(self, expr) -> "PolarsDataFrame":
#         """Filter rows based on the given expression."""

#         filtered_df = self._filter_ibis(expr=expr)
#         return PolarsDataFrame(filtered_df, ibis_backend=self.ibis_backend)
                

#     def _filter_native(self, expr) -> Union[pl.DataFrame, pl.LazyFrame]:
#         """Filter rows based on the given expression."""

#         filtered_df = self._filter_ibis(expr=expr)
#         return filtered_df


#     def head(self, n: int) -> "PolarsDataFrame":
#         """Take the first n rows."""

#         head_df = self._head_ibis(n)
#         return PolarsDataFrame(head_df, ibis_backend=self.ibis_backend)


# ### Aggregates        
    
#     def count(self) -> int:
#         return self._count_ibis()

# ### Query        
    
#     def sql(self, query:str) -> "PolarsDataFrame":

#         query_df = self._sql(query)
#         native_df = DataFrameUtils.cast_dataframe_to_polars(df_dataframe=query_df)

#         return PolarsDataFrame(native_df, ibis_backend=self.ibis_backend)



#     def _sql_native(self, query:str) -> Union[pl.DataFrame, pl.LazyFrame]:
#         new_df = self._sql_ibis(query)

#         native_df = DataFrameUtils.cast_dataframe_to_polars(df_dataframe=new_df)        
#         return native_df



#     def as_dict(self) -> Dict[str, List[Any]] | Any:
#         return self._as_dict_ibis()

#     def as_list(self) -> Dict[str, List[Any]] | Any:
#         return self._as_list_ibis()

#     def get_first_row_as_dict(self,
#         ) -> Dict[Any,Any]:
#         return self._get_first_row_as_dict_ibis()

# # #ibis connections:
# #     def set_ibis_backend(self) -> None:
# #         ibis.set_backend(backend="polars")

# #     def init_ibis_connection(self):    
# #         if self.df is not None:
# #             self.ibis_backend = get_ibis_connection("polars")

# #             # self.ibis_backend = ibis.polars.connect()#{self.tablename: self.df})






#     # def create_filter_expression(self, column_name: str, operator: str, value: Any):
#     #     if operator == '>':
#     #         return pl.col(column_name) > value
#     #     elif operator == '<':
#     #         return pl.col(column_name) < value
#     #     elif operator == '==':
#     #         return pl.col(column_name) == value
#     #     elif operator == '!=':
#     #         return pl.col(column_name) != value
#     #     elif operator == 'isin':
#     #         return pl.col(column_name).is_in(value)
#     #     else:
#     #         raise Exception(f"Operator {operator} not supported")