from typing import Union, List, Dict, Any, Optional, Tuple

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis as ibis
import ibis.expr.types as ir
# import uuid

from .base_dataframe import BaseDataFrame

class IbisDataFrame(BaseDataFrame):

    def __init__(self, 
                 df:                    Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
                 ibis_backend:          Optional[ibis.BaseBackend] = None,
                 ibis_backend_schema:   Optional[str] = None,
                 tablename_prefix:      Optional[str] = None,
                #lineage_history: Optional[List[Any]] = None,
                 ) -> None:

        super().__init__(df=df, 
                         ibis_backend=ibis_backend, 
                         ibis_backend_schema=ibis_backend_schema, 
                         tablename_prefix=tablename_prefix)



    # =========
    # Initialisation
        
        # Call a method to set the backend schema after super().__init__
    def init_default_ibis_backend_schema(self):
        self.default_ibis_backend_schema = "polars"

    def init_native_table(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]
                          ) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]:
            
        return df


    def convert_backend_schema(self, new_backend_schema:str) -> "IbisDataFrame":

        #TODO: verify that the new backend schema is valid

        return IbisDataFrame(df=self.ibis_df, ibis_backend_schema=new_backend_schema)




#Materialisation Status
    def is_materialised(self) -> bool:
        return isinstance(self.ibis_df, ir.Table)


    def materialise(self) -> Union[pd.DataFrame, pl.DataFrame]:

        if isinstance(self.ibis_df, ir.Table):

            if self.ibis_backend_schema == "polars":
                return self.ibis_df.to_polars()
            else:
                return self.ibis_df.execute()
        else:
            raise ValueError("Dataframe could not be not materialised")


# Column Selection
    def select(self, ibis_expr: Any) -> "IbisDataFrame":

        new_df: ir.Table = self._select_ibis(ibis_expr=ibis_expr).alias(alias=self.generate_tablename(prefix="select"))
        # add a lineage record - name and transformations

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)


    def _select_ibis(self, ibis_expr: Any) -> Any:

        #Do not add a parameter name here for columns. That will be interpreted as a column name
        df_cols: Any = self.ibis_df.select(ibis_expr)
        return df_cols
    

    def drop(self, columns: Any) -> "IbisDataFrame":

        new_df: ir.Table = self._drop_ibis(columns).alias(alias=self.generate_tablename(prefix="drop"))
 
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
    def _drop_ibis(self, columns: Any) -> ir.Table:

        #Only drop columns if they exist in the dataframe
        existing_columns = self.get_column_names()
        columns = [x for x in columns if x in existing_columns]

        if not columns or len(columns) == 0:
            return self.ibis_df

        new_df: Any = self.ibis_df.drop(columns)
        return new_df

    def distinct(self) -> "BaseDataFrame":

        new_df: ir.Table = self._distinct_ibis().alias(alias=self.generate_tablename(prefix="distinct"))
 
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _distinct_ibis(self) -> ir.Table:
        new_df: Any = self.ibis_df.distinct()
        return new_df


    def rename(self, **kwargs) -> "BaseDataFrame":

        new_df: ir.Table = self._rename_ibis(**kwargs).alias(alias=self.generate_tablename(prefix="rename"))
 
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
    
    def _rename_ibis(self,  **kwargs) -> ir.Table:
        new_df: Any = self.ibis_df.rename( **kwargs)
        return new_df


    def try_cast(self, **kwargs) -> "BaseDataFrame":

        new_df: ir.Table = self._try_cast_ibis(**kwargs).alias(alias=self.generate_tablename(prefix="try_cast"))
 
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _try_cast_ibis(self,  **kwargs) -> ir.Table:
        new_df: Any = self.ibis_df.try_cast( **kwargs)
        return new_df

# Add Columns       
    def mutate(self, **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._mutate_ibis( **kwargs).alias(alias=self.generate_tablename(prefix="mutate"))
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
    def _mutate_ibis(self,  **kwargs) -> ir.Table:
        df_cols: Any = self.ibis_df.mutate( **kwargs)
        return df_cols
    
# Reshape
    def aggregate(self,  **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._aggregate_ibis( **kwargs).alias(alias=self.generate_tablename(prefix="aggregate"))
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
      
    def _aggregate_ibis(self, **kwargs) -> ir.Table:

        df_cols: Any = self.ibis_df.aggregate(**kwargs)
        return df_cols

    def pivot_wider(self,  **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._pivot_wider_ibis( **kwargs).alias(alias=self.generate_tablename(prefix="pivot_wider"))
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
    def _pivot_wider_ibis(self, **kwargs) -> ir.Table:
        df_cols: Any = self.ibis_df.pivot_wider(**kwargs)
        return df_cols

    def pivot_longer(self, **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._pivot_longer_ibis( **kwargs).alias(alias=self.generate_tablename(prefix="pivot_longer"))
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _pivot_longer_ibis(self, **kwargs) -> ir.Table:
        df_cols: Any = self.ibis_df.pivot_longer(**kwargs)
        return df_cols


    #Join Resolution!
    def _resolve_join_backend_ibis(self, 
                                   right: "BaseDataFrame",
                                   execute_on: Optional[str] = None
                                   ) -> Tuple[ir.Table, ir.Table]:

        if self.ibis_backend is None:
            raise Exception("Ibis client connection not established")

        if execute_on and execute_on not in ["left", "right"]:
            raise ValueError("execute_on must be one of 'left', 'right' or None")


        #right has the same backed
        if isinstance(right, IbisDataFrame) and self.ibis_backend == right.ibis_backend:

            left_table = self.ibis_df
            right_table = right.ibis_df

        #right a different backend or not ibis
        else:

            if execute_on == "left":

                #create temp table on left (self) backend
                left_table = self.ibis_df
                right_table = self.create_temp_table_ibis(df_dataframe=right.ibis_df, tablename_prefix="left_table", overwrite=True)


            elif execute_on == "right":
                #create temp table on right

                if not isinstance(right, IbisDataFrame):
                    raise ValueError("Right must be an IbisDataFrame to execute on right")

                right_table = right.ibis_df
                left_table = self.create_temp_table_ibis(df_dataframe=self.native_df, tablename_prefix="left_table", overwrite=True)


            else:
                #We have differing backends, and we are running locally. Bring both into local memory
                left_table = self.create_temp_table_ibis(df_dataframe=self.ibis_df, tablename_prefix="left_table", overwrite=True)
                right_table = self.create_temp_table_ibis(df_dataframe=right.ibis_df, tablename_prefix="right_table", overwrite=True)


        return (left_table, right_table)



    def inner_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None,
                   **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._inner_join_ibis(right=right, predicates=predicates, execute_on=execute_on, **kwargs).alias(self.generate_tablename(prefix="inner_join"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _inner_join_ibis(self, 
             right: "BaseDataFrame", 
             predicates: Any,
             execute_on: Optional["str"] = None,
             **kwargs
            ) -> ir.Table:

        left_table, right_table = self._resolve_join_backend_ibis(right=right, execute_on=execute_on)

        return left_table.inner_join(right=right_table, predicates=predicates, **kwargs)

    def left_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None,
                   **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._left_join_ibis(right=right, predicates=predicates, execute_on=execute_on, **kwargs).alias(self.generate_tablename(prefix="left_join"))


        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _left_join_ibis(self, 
             right: "BaseDataFrame", 
             predicates: Any,
             execute_on: Optional["str"] = None,
             **kwargs
            ) -> ir.Table:

        left_table, right_table = self._resolve_join_backend_ibis(right=right, execute_on=execute_on)

        return left_table.left_join(right=right_table, predicates=predicates, **kwargs)


    def outer_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None,
                   **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._outer_join_ibis(right=right, predicates=predicates, execute_on=execute_on, **kwargs).alias(self.generate_tablename(prefix="outer_join"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _outer_join_ibis(self, 
             right: "BaseDataFrame", 
             predicates: Any,
             execute_on: Optional["str"] = None,
             **kwargs
            ) -> ir.Table:

        left_table, right_table = self._resolve_join_backend_ibis(right=right, execute_on=execute_on)

        return left_table.outer_join(right=right_table, predicates=predicates, **kwargs)

# Row Selection

    def filter(self, ibis_expr: Any) -> "IbisDataFrame":
        """Filter rows based on the given expression."""

        new_df: ir.Table = self._filter_ibis(ibis_expr).alias(self.generate_tablename(prefix="filter"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)



    def head(self, n: int) -> "IbisDataFrame":
        """Take the first n rows."""

        new_df = self._head_ibis(n).alias(self.generate_tablename(prefix="head"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _head_ibis(self, n: int) -> ir.Table:

        if n < 0:
            raise ValueError("n must be greater than or equal to 0")

        return self.ibis_df.head(n=n) 
                
    def union(self, **kwargs) -> "IbisDataFrame":
        """Take the first n rows."""

        new_df = self._union_ibis(**kwargs).alias(alias=self.generate_tablename(prefix="union"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _union_ibis(self, **kwargs) -> ir.Table:
        return ibis.union(self.ibis_df, **kwargs) 


    def order_by(self, **kwargs) -> "IbisDataFrame":
        """Take the first n rows."""

        new_df = self._order_by_ibis(**kwargs).alias(self.generate_tablename(prefix="order_by"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
               
    def _order_by_ibis(self, **kwargs) -> ir.Table:
        return self.ibis_df.order_by(**kwargs) 
    
#Column Metadata
    def get_column_names(self) -> list:
        return self._get_column_names_ibis()   
            
    def _get_column_names_ibis(self) -> List[str]:
        return DataFrameUtils.get_column_names(df=self.ibis_df)
                
### Aggregates        
    
    def count(self) -> int:
       return self._count_ibis()
         
    def _count_ibis(self) -> int:

        return DataFrameUtils.count(self.ibis_df) 


### Query        
    
    def sql(self, query:str) -> "IbisDataFrame":
        new_df = self._sql_ibis(query=query).alias(alias=self.generate_tablename(prefix="sql"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)


    
    def as_dict(self) -> Dict[str, List[Any]] | Any:
        return self._as_dict_ibis()

    def _as_dict_ibis(self) -> Dict[str, List[Any]] | Any:

        return DataFrameUtils.cast_dataframe_to_dictonary_of_lists(df=self.ibis_df)

    def as_list(self) -> Dict[str, List[Any]] | Any:
        return self._as_list_ibis()

    def _as_list_ibis(self) -> Dict[str, List[Any]] | Any:
        
        return DataFrameUtils.cast_dataframe_to_list_of_dictionaries(df=self.ibis_df)

    def get_first_row_as_dict(self,
        ) -> Dict[Any,Any]:
        return self._get_first_row_as_dict_ibis()

    def _get_first_row_as_dict_ibis(
            self,
        ) -> Dict[Any,Any]:
        
        obj_df = self.head(n=1)
        obj_list = DataFrameUtils.cast_dataframe_to_list_of_dictionaries(df=obj_df.materialise())

        if len(obj_list) > 0:
            return obj_list[0]  
        else:
            return {}
      
