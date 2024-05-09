# from typing import Dict, Any, List, Optional, Union, Sequence, Tuple

# import pandas as pd
# import polars as pl
# import ibis as ibis
# import ibis.expr.types as ir

# from .base_dataframe import BaseDataFrame, get_ibis_connection
# from .ibis_dataframe import IbisDataFrame
# from mountainash_data.dataframes.utils.dataframe_utils import DataFrameUtils

# # from multipledispatch import dispatch
# # from pyarrow import Table as irTable  # assuming you are using pyarrow's Table for ir.Table

# #TODO: incorporate lineage update on exit from this class -> ie: creation of a downstream dataframe.

# class PandasDataFrame(BaseDataFrame):
    
#     def __init__(self, 
#                  df:            Union[pd.DataFrame, Any],
#                  ibis_backend:  Optional[ibis.BaseBackend] = None, 
#                  tablename_prefix=None                 
#                  ) -> None:
        
#         super().__init__(df=df, 
#                          ibis_backend=ibis_backend, 
#                          tablename_prefix=tablename_prefix)

    

#     def init_default_ibis_backend_schema(self):
#         self.default_ibis_backend_schema = "pandas"


#     def init_native_table(self, df) -> pd.DataFrame:
        
#         if isinstance(df, pd.DataFrame):
#             native_df = df

#         else:
#             #we have a new table from pandas or polars
#             native_df: pd.DataFrame = DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=df)
        
#         return native_df



#     #amterlialise
#     def materialise(self) -> pd.DataFrame:

#         return DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=self.native_df)
        

#     def materialise_ibis(self) -> pd.DataFrame:

#         return DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=self.ibis_df.execute())
        

# # Column Selection
#     def select(self, columns: List[str]|str) -> "PandasDataFrame":

#         new_df = DataFrameUtils.select(df=self.native_df, columns=columns)

#         return PandasDataFrame(df=new_df, ibis_backend=self.ibis_backend)



#     def drop(self, columns: List[str]|str) -> "PandasDataFrame":

#         new_df = DataFrameUtils.drop(df=self.native_df, columns=columns)

#         return PandasDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       


#     def mutate(self, ibis_expr: Any, **kwargs) -> "PandasDataFrame":

#         new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df).mutate(expr=ibis_expr, **kwargs).execute()

#         return PandasDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
#     def _mutate_native(self, ibis_expr: Any, **kwargs) -> pd.DataFrame:

#         new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df).mutate(expr=ibis_expr, **kwargs).execute()

#         native_df = DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=new_df) 

#         return native_df


#     def aggregate(self,  **kwargs) -> "PandasDataFrame":

#         new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df).aggregate( **kwargs).execute()

#         return PandasDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
#     def _aggregate_native(self,  **kwargs) -> pd.DataFrame:

#         new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df).aggregate( **kwargs).execute()

#         native_df = DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=new_df) 

#         return native_df


#     def pivot_wider(self,  **kwargs) -> "PandasDataFrame":

#         new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df).pivot_wider( **kwargs).execute()

#         return PandasDataFrame(df=new_df, ibis_backend=self.ibis_backend)
    
#     def _pivot_wider_native(self,  **kwargs) -> pd.DataFrame:

#         new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df).pivot_wider( **kwargs).execute()

#         native_df = DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=new_df) 
        
#         return native_df  
       
#     def pivot_longer(self, **kwargs) -> "PandasDataFrame":

#         new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df).pivot_longer( **kwargs).execute()

#         return PandasDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _pivot_longer_native(self, **kwargs) -> pd.DataFrame:

#         new_df =  DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df).pivot_longer( **kwargs).execute()

#         native_df = DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=new_df) 

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

#         # Known: Left side is Pandas
#         # 

#         #right has the same backed
#         if isinstance(right, PandasDataFrame):

#             left_table:     ir.Table = self.ibis_df
#             right_table:    ir.Table = right.ibis_df

#         #right a different backend or not ibis
#         else:

#             if execute_on == "left":

#                 # create temp table on left (self) backend
#                 left_table =    self.ibis_df   
#                 # right_table =   DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=right.native_df))
#                 # right_table =   right.create_table_ibis(name=self.generate_tablename(prefix="left_table"), obj=left_source_table, overwrite=True)

#                 # right_source_table = right.to_arrow()
#                 right_table = self.create_temp_table_ibis(df_dataframe=right.native_df, tablename_prefix="right_table", overwrite=True)

#             elif execute_on == "right":
                
#                 # create temp table on right
#                 if isinstance(right, IbisDataFrame):

#                     #Right side is ibis, but unknown backend.
#                     # copy self to right 

#                     right_table = right.ibis_df
#                     # left_source_table = self.to_arrow()
#                     # left_table = right.create_table_ibis(name=self.generate_tablename(prefix="left_table"), obj=left_source_table, overwrite=True)
#                     left_table = self.create_temp_table_ibis(df_dataframe=self.native_df, tablename_prefix="left_table", overwrite=True)

#                 else:
#                     #Right side is not ibis, and not pandas. So want both sides to be on the same backend as RHS
#                     right_source_table = right.to_arrow_native()
#                     right_table = right.create_table_ibis(name=self.generate_tablename(prefix="right_table"), obj=right_source_table, overwrite=True)

#                     left_source_table = self.to_arrow_native()
#                     left_table = right.create_table_ibis(name=self.generate_tablename(prefix="left_table"), obj=left_source_table, overwrite=True)


#             else:
#                 #bring both into local memory. They should be named...
#                 left_table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=self.native_df, tablename_prefix="left_table")
#                 right_table = DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=right.native_df, tablename_prefix="right_table")


#         return (left_table, right_table)




#     def inner_join(self,             
#                    right: "BaseDataFrame", 
#                    predicates: Any,
#                    execute_on: Optional["str"] = None) -> "PandasDataFrame":


#         left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

#         new_df: ir.Table = left_table.inner_join(right=right_table, predicates=predicates).execute()
 
#         return PandasDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _inner_join_native(self,             
#                    right: "BaseDataFrame", 
#                    predicates: Any,
#                    execute_on: Optional["str"] = None) -> pd.DataFrame:


#         left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

#         new_df = left_table.inner_join(right=right_table, predicates=predicates).execute()
#         native_df = DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=new_df) 

#         return native_df

#     def left_join(self,             
#                    right: "BaseDataFrame", 
#                    predicates: Any,
#                    execute_on: Optional["str"] = None) -> "PandasDataFrame":

#         left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

#         new_df: ir.Table = left_table.left_join(right=right_table, predicates=predicates).execute()
 
#         return PandasDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _left_join_native(self,             
#                    right: "BaseDataFrame", 
#                    predicates: Any,
#                    execute_on: Optional["str"] = None) -> pd.DataFrame:

#         left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

#         new_df = left_table.left_join(right=right_table, predicates=predicates).execute()
 
#         native_df = DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=new_df) 
        
#         return native_df

#     def outer_join(self,             
#                    right: "BaseDataFrame", 
#                    predicates: Any,
#                    execute_on: Optional["str"] = None) -> "PandasDataFrame":

#         left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

#         new_df = left_table.outer_join(right=right_table, predicates=predicates).execute()
 
#         new_df = DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=new_df) 

#         return PandasDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#     def _outer_join_native(self,             
#                    right: "BaseDataFrame", 
#                    predicates: Any,
#                    execute_on: Optional["str"] = None) -> pd.DataFrame:

#         left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

#         new_df = left_table.outer_join(right=right_table, predicates=predicates).execute()
 
#         native_df = DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=new_df) 


#         return native_df

# # Row Selection


#     def filter(self, ibis_expr) -> "PandasDataFrame":
#         """Filter rows based on the given expression."""

#         filtered_df = self._filter_ibis(ibis_expr=ibis_expr)
#         return PandasDataFrame(filtered_df, ibis_backend=self.ibis_backend)
                
#     def _filter_native(self, ibis_expr) -> pd.DataFrame:
#         """Filter rows based on the given expression."""

#         filtered_df = self._filter_ibis(ibis_expr=ibis_expr)
#         native_df = DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=filtered_df) 
#         return native_df

#     def head(self, n: int) -> "PandasDataFrame":
#         """Take the first n rows."""

#         head_df = self._head_ibis(n)
#         return PandasDataFrame(head_df, ibis_backend=self.ibis_backend)



# ### Aggregates        
    
#     def count(self) -> int:
#         return self._count_ibis()

# ### Query        
    
#     def sql(self, query:str) -> "PandasDataFrame":
#         query_df = self._sql_ibis(query=query)

#         native_df = DataFrameUtils.cast_dataframe_to_pandas(query_df)        
#         return PandasDataFrame(native_df, ibis_backend=self.ibis_backend)


#     def _sql_native(self, query:str) -> pd.DataFrame:
#         query_df = self._sql_ibis(query)

#         native_df = DataFrameUtils.cast_dataframe_to_pandas(query_df)        
#         return native_df


#     def as_dict(self) -> Dict[str, List[Any]] | Any:
#         return self._as_dict_ibis()

#     def as_list(self) -> Dict[str, List[Any]] | Any:
#         return self._as_list_ibis()
    
#     def get_first_row_as_dict(self,
#         ) -> Dict[Any,Any]:
#         return self._get_first_row_as_dict_ibis()
    

# # # Ibis Interface    
# #     def set_ibis_backend(self) -> None:
# #         ibis.set_backend(backend="polars")
        
# #     def init_ibis_connection(self):    

# #         if self.native_df is not None:
# #             self.ibis_backend = get_ibis_connection("pandas")
# #             # self.ibis_backend = ibis.pandas.connect({self.tablename: self.native_df})