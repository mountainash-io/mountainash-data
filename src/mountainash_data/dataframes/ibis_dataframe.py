from typing import Union, List, Dict, Any, Optional, Tuple

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis as ibis
import ibis.expr.types as ir
import ibis.expr.schema as ibis_schema

# import uuid
from .utils.dataframe_utils import DataFrameUtils, init_ibis_connection
from .base_dataframe import BaseDataFrame

class IbisDataFrame(BaseDataFrame):

    def __init__(self, 
                 df:                    Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
                 ibis_backend:          Optional[ibis.BaseBackend] = None,
                 ibis_backend_schema:   Optional[str] = None,
                 tablename_prefix:      Optional[str] = None,
                 create_as_view:        Optional[bool] = False,
                #lineage_history: Optional[List[Any]] = None,
                 ) -> None:

        super().__init__(df=df, 
                         ibis_backend=ibis_backend, 
                         ibis_backend_schema=ibis_backend_schema, 
                         tablename_prefix=tablename_prefix,
                         create_as_view=create_as_view)



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

            return self.ibis_df.to_polars()
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

        #Will need to be addresses in https://github.com/mountainash-io/mountainash-data/issues/22
        # Needs to pass test_rename_method()  
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


    def _cast_types_ibis(self, 
                         df_ibis: ir.Table,
                         target_ibis_backend_schema: Optional[str],
                         fields_diff_types: List[str],
                         target_fields_types: Dict[str, Any]) -> ir.Table:

        if self.ibis_backend_schema in set("duckdb"):
            cast_dict = {field: target_fields_types[field] for field in fields_diff_types}
            print(f"CAST DICT: {cast_dict}")
            df_ibis = df_ibis.try_cast(cast_dict)
        else:

            for field in fields_diff_types:
                target_type = target_fields_types[field]
                print(f"Casting field: {field} to {target_type}")
                df_ibis = ( df_ibis 
                                .mutate(field = ibis._[field].cast(target_type ) )
                                .drop(field)
                                .rename({field: "field"})
                )
        return df_ibis


    #Join Resolution!
    def _resolve_join_backend_ibis(self, 
                                   right: "BaseDataFrame",
                                   execute_on: Optional[str] = None
                                   ) -> Tuple[ir.Table, ir.Table]:

        #TODO: Get schemas, and compare
        #IF different and diff backend, cast to target backend
        #IF different and same backend, cast to left backend
        # Finally, never use Pandas! Always use Ibis or Polars


        if self.ibis_backend is None:
            raise Exception("Ibis client connection not established")

        if execute_on and execute_on not in ["left", "right"]:
            raise ValueError("execute_on must be one of 'left', 'right' or None")

        #Test if we have the same backend object
        same_backend = False

        #Resolve table schema
        left_table_schema: ibis_schema.Schema = DataFrameUtils.get_table_schema(self.ibis_df)
        right_table_schema: ibis_schema.Schema = DataFrameUtils.get_table_schema(right.ibis_df)

        #find common keys in schemas:
        common_fields = list(set(left_table_schema.fields.keys()).intersection(set(right_table_schema.fields.keys())))

        #For common keys, check if the types are the same
        fields_diff_types: List[str] = [field for field in common_fields if left_table_schema.fields[field] != right_table_schema.fields[field]]
        fields_types_left: Dict[str, Any] = {field: left_table_schema[field] for field in fields_diff_types}
        fields_types_right: Dict[str, Any] = {field: right_table_schema[field] for field in fields_diff_types}


        #For in-memory dbs don't use equals (==) here as it just compares the connection string!
        if self.ibis_backend_schema in ["duckdb", "sqlite"] and self.ibis_backend is right.ibis_backend:
            same_backend = True
        #For database connections (like databases) it is OK to use == as it compares the connection string, which will be the same database, even if they are distinct connections
        if self.ibis_backend_schema not in ["duckdb", "sqlite"] and self.ibis_backend == right.ibis_backend:
            same_backend = True

        
        if isinstance(right, IbisDataFrame) and same_backend: 

            left_table = self.ibis_df
            right_table = right.ibis_df

            if len(fields_diff_types) > 0:

                right_table = self._cast_types_ibis(df_ibis=right_table,
                                                    target_ibis_backend_schema =self.ibis_backend_schema,
                                                    fields_diff_types=fields_diff_types, 
                                                    target_fields_types=fields_types_left)


        #right a different backend or not ibis
        else:

            if execute_on == "left":

                left_table = self.ibis_df
                right_table = self.create_temp_table_ibis(df_dataframe=right.ibis_df, tablename_prefix="right_table", current_ibis_backend=right.ibis_backend, target_ibis_backend=self.ibis_backend, overwrite=True)

                if len(fields_diff_types) > 0:
                    right_table = self._cast_types_ibis(df_ibis=right_table, 
                                                        target_ibis_backend_schema =right.ibis_backend_schema,
                                                        fields_diff_types=fields_diff_types, 
                                                        target_fields_types=fields_types_left)

            elif execute_on == "right":

                if not isinstance(right, IbisDataFrame):
                    raise ValueError("Right must be an IbisDataFrame to execute on right")

                left_table = right.create_temp_table_ibis(df_dataframe=self.ibis_df, tablename_prefix="left_table", current_ibis_backend=self.ibis_backend, target_ibis_backend=right.ibis_backend, overwrite=True)
                right_table = right.ibis_df

                if len(fields_diff_types) > 0:
                    left_table = self._cast_types_ibis(df_ibis=left_table, 
                                                       target_ibis_backend_schema =self.ibis_backend_schema,
                                                       fields_diff_types=fields_diff_types, 
                                                       target_fields_types=fields_types_right)


            else:

                default_ibis_backend = init_ibis_connection(self.default_ibis_backend_schema)

                #We have differing backends, and we are running locally. Bring both into local memory
                left_table =  self.create_temp_table_ibis(df_dataframe=self.ibis_df, tablename_prefix="left_table", current_ibis_backend=self.ibis_backend, target_ibis_backend=default_ibis_backend, overwrite=True)
                right_table = right.create_temp_table_ibis(df_dataframe=right.ibis_df, tablename_prefix="right_table", current_ibis_backend=right.ibis_backend, target_ibis_backend=default_ibis_backend, overwrite=True)

                if len(fields_diff_types) > 0:
                    right_table = self._cast_types_ibis(df_ibis=right_table, 
                                                        target_ibis_backend_schema =self.ibis_backend_schema,
                                                        fields_diff_types=fields_diff_types, 
                                                        target_fields_types=fields_types_left)

        return (left_table, right_table)



    def inner_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional[str] = None,
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
                   execute_on: Optional[str] = None,
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
                   execute_on: Optional[str] = None,
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
      
