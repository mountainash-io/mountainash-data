from typing import Union, List, Dict, Any, Optional, Tuple

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis as ibis
import ibis.expr.types as ir
import ibis.expr.schema as ibis_schema
from functools import lru_cache
import uuid

from mountainash_constants import CONST_DATAFRAME_FRAMEWORK

# import uuid
from .base_dataframe import BaseDataFrame


@lru_cache(maxsize=None)
def init_ibis_connection(ibis_schema: Optional[str] = None) -> ibis.BaseBackend:
    return ibis.connect(resource=f"{ibis_schema}://")


class IbisDataFrame(BaseDataFrame):

    def __init__(self, 
                 df:                    Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
                 ibis_backend:          Optional[ibis.BaseBackend] = None,
                 ibis_backend_schema:   Optional[str] = None,
                 tablename_prefix:      Optional[str] = None,
                 create_as_view:        Optional[bool] = False,
                #lineage_history: Optional[List[Any]] = None,
                 ) -> None:

        super().__init__(df=df)


        #default ibis schema. Schema is the type of backend, eg "polars", "duckdb", "postgres"
        self.default_ibis_backend_schema: Optional[str] = None
        self.init_default_ibis_backend_schema()        
        
        #Init the backend
        if not ibis_backend:

            if not ibis_backend_schema:
                ibis_backend_schema = self.default_ibis_backend_schema

            self.init_default_ibis_backend(default_ibis_schema=ibis_backend_schema)
        else:
            self.ibis_backend = ibis_backend
            ibis_backend_schema = self.ibis_backend.name

        #We now know the final schema type!
        self.ibis_backend_schema = ibis_backend_schema

        # self.ibis_temp_tablename: str = self.init_ibis_temp_tablename()

        #Init the dataframe. What Am I doing with a pyarrow table here?
        self.ibis_df: ir.Table = self.init_ibis_table(df=df, tablename_prefix=tablename_prefix, create_as_view=create_as_view)


    def _get_dataframe(self) -> ir.Table:
        return self.ibis_df


    # =========
    # Initialisation
        
    # Call a method to set the backend schema after super().__init__
    def init_default_ibis_backend_schema(self):
        self.default_ibis_backend_schema = "polars"

    def init_default_ibis_connection(self, ibis_schema: str) -> ibis.BaseBackend: 
        return init_ibis_connection(ibis_schema=ibis_schema)

    def init_native_table(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]
                          ) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]:
            
        return df


    def convert_backend_schema(self, new_backend_schema:str) -> "IbisDataFrame":

        #TODO: verify that the new backend schema is valid

        return IbisDataFrame(df=self.ibis_df, ibis_backend_schema=new_backend_schema)


    def init_default_ibis_backend(self, 
                                  default_ibis_schema: Optional[str] = None):    
            
        if not default_ibis_schema:
            default_ibis_schema = self.default_ibis_backend_schema

        #TODO: Check that we have a valid schema
        if default_ibis_schema not in ["duckdb", "polars", "sqlite"]:
            raise ValueError(f"Invalid default ibis schema: {default_ibis_schema}")

        #Using the function to cache the connection.
        self.ibis_backend = self.init_default_ibis_connection(ibis_schema=default_ibis_schema)
        # self.ibis_backend = ibis.connect(resource=f"{default_ibis_schema}://")
 
        if not self.ibis_backend:
            raise Exception("Ibis client connection not established")


    def init_ibis_table(self, 
                        df : Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table], 
                        tablename_prefix:Optional[str]=None,
                        create_as_view:        Optional[bool] = False,
                 ) -> ir.Table:
        
        if not self.ibis_backend:
            self.init_default_ibis_backend()

        if isinstance(df, ir.Table):
            # We already have ibis, but if generated manually, we may lack a name
            if df.has_name() and not tablename_prefix:
                ibis_df = df
            else:
                tablename = self.generate_tablename(prefix=tablename_prefix)
                ibis_df = df.alias(alias=tablename)

        else:
            #we have a new table from pandas or polars
            ibis_df = self.create_temp_table_ibis(df=df, tablename_prefix=tablename_prefix, current_ibis_backend=self.ibis_backend,  target_ibis_backend=self.ibis_backend, create_as_view=create_as_view)
        
        return ibis_df



    def generate_tablename(self, prefix: Optional[str] = None) -> str:

        if prefix:
            temp_tablename = f"{prefix}_{str(object=uuid.uuid4())}"
        else:   
            temp_tablename = str(object=uuid.uuid4())

        return temp_tablename 


    def create_temp_table_ibis(self,
                          df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
                          tablename_prefix: Optional[str] = None,
                          current_ibis_backend: Optional[ibis.BaseBackend] = None,
                          target_ibis_backend: Optional[ibis.BaseBackend] = None,
                          overwrite: Optional[bool] = True,
                          create_as_view: Optional[bool] = False
            ) -> ir.Table:

        if current_ibis_backend is None:
            current_ibis_backend = self.ibis_backend

        if target_ibis_backend is None:
            target_ibis_backend = self.ibis_backend


        tablename = self.generate_tablename(prefix=tablename_prefix)
            
        if current_ibis_backend is target_ibis_backend:

            if create_as_view and isinstance(df, ir.Table):
                ibis_df =  target_ibis_backend.create_view(name = tablename, obj=df, overwrite=overwrite)
                return ibis_df        
                    
        else:
            #When moving between backends, we need materialise to move to the new backend
            if isinstance(df, ir.Table):
                df = df.to_pyarrow()

        if target_ibis_backend.supports_temporary_tables:   
            ibis_df =  target_ibis_backend.create_table(name = tablename, obj=df, overwrite=overwrite, temp=True)
        else:
            ibis_df =  target_ibis_backend.create_table(name = tablename, obj=df, overwrite=overwrite)

        return ibis_df







#Materialisation Status
    def is_materialised(self) -> bool:
        return isinstance(self.ibis_df, ir.Table)


    def materialise(self, dataframe_framework: Optional[str]=None) -> Any:

        if dataframe_framework is None:
            dataframe_framework = CONST_DATAFRAME_FRAMEWORK.POLARS.value

        if dataframe_framework == CONST_DATAFRAME_FRAMEWORK.POLARS.value:
            return self.to_polars()

        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PANDAS.value:
            return self.to_pandas()

        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PYARROW_TABLE.value:
            return self.to_arrow()

        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PYARROW_RECORDBATCH.value:
            return self.to_pyarrow_recordbatch()

        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.IBIS.value:
            return self._get_dataframe()

        else:
            raise ValueError("Dataframe could not be not materialised")

    def to_arrow(self) -> pa.Table:
        return self._get_dataframe().to_pyarrow()

    def to_pyarrow_recordbatch(self, batchsize: int = 1) -> List[pa.RecordBatch]:

        temp = self._get_dataframe().to_pyarrow() 
        return temp.to_batches(max_chunksize=batchsize)         

    def to_pandas(self) -> pd.DataFrame:
        return self._get_dataframe().to_pandas()

    def to_polars(self) -> pl.DataFrame:
        return self._get_dataframe().to_polars()

    def to_ibis(self) -> ir.Table:
        return self._get_dataframe()


    def cast_dataframe_to_arrow(self) -> pa.Table:
        return self.to_arrow()

    def cast_dataframe_to_pyarrow_recordbatch(self, batchsize: int = 1) -> List[pa.RecordBatch]:
        return self.to_pyarrow_recordbatch()

    def cast_dataframe_to_pandas(self) -> pd.DataFrame:
        return self.to_pandas()

    def cast_dataframe_to_polars(self) -> pl.DataFrame:
        return self.to_polars()

    def cast_dataframe_to_ibis(self) -> ir.Table:
        return self._get_dataframe()



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
        if isinstance(columns, str):
            columns = [columns]

        existing_columns = self.get_column_names()
        columns = [x for x in columns if x in existing_columns]

        if not columns or len(columns) == 0:
            return self.ibis_df

        new_df: Any = self.ibis_df.drop(columns)
        return new_df

    def distinct(self) -> "IbisDataFrame":

        new_df: ir.Table = self._distinct_ibis().alias(alias=self.generate_tablename(prefix="distinct"))
 
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _distinct_ibis(self) -> ir.Table:
        new_df: Any = self.ibis_df.distinct()
        return new_df


    def rename(self, **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._rename_ibis(**kwargs).alias(alias=self.generate_tablename(prefix="rename"))
 
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
    
    def _rename_ibis(self,  **kwargs) -> ir.Table:      

        #Will need to be addresses in https://github.com/mountainash-io/mountainash-data/issues/22
        # Needs to pass test_rename_method()  
        new_df: Any = self.ibis_df.rename( **kwargs)
        return new_df


    def try_cast(self, **kwargs) -> "IbisDataFrame":

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

    def _resolve_join_backend(self, 
                                   right: "BaseDataFrame",
                                   execute_on: Optional[str] = None
                                   ) -> Tuple[ir.Table, ir.Table]:
        return self._resolve_join_backend_ibis(right=right, execute_on=execute_on)


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
        left_table_schema:  Optional[ibis_schema.Schema] = self.ibis_df.schema()
        right_table_schema: Optional[ibis_schema.Schema] = right._get_dataframe().schema()

        #find common keys in schemas:
        common_fields = list(set(left_table_schema.fields.keys()).intersection(set(right_table_schema.fields.keys())))

        #For common keys, check if the types are the same
        fields_diff_types: List[str] = [field for field in common_fields if left_table_schema.fields[field] != right_table_schema.fields[field]]
        fields_types_left: Dict[str, Any] = {field: left_table_schema[field] for field in fields_diff_types}
        fields_types_right: Dict[str, Any] = {field: right_table_schema[field] for field in fields_diff_types}


        #For in-memory dbs don't use equals (==) here as it just compares the connection string!
        if self.ibis_backend_schema in ["duckdb", "sqlite"] and self.ibis_backend is right._get_dataframe():
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
                right_table = self.create_temp_table_ibis(df=right._get_dataframe(), tablename_prefix="right_table", current_ibis_backend=right.ibis_backend, target_ibis_backend=self.ibis_backend, overwrite=True)

                if len(fields_diff_types) > 0:
                    right_table = self._cast_types_ibis(df_ibis=right_table, 
                                                        target_ibis_backend_schema =right.ibis_backend_schema,
                                                        fields_diff_types=fields_diff_types, 
                                                        target_fields_types=fields_types_left)

            elif execute_on == "right":

                if not isinstance(right, IbisDataFrame):
                    raise ValueError("Right must be an IbisDataFrame to execute on right")

                left_table = right.create_temp_table_ibis(df=self.ibis_df, tablename_prefix="left_table", current_ibis_backend=self.ibis_backend, target_ibis_backend=right.ibis_backend, overwrite=True)
                right_table = right.ibis_df

                if len(fields_diff_types) > 0:
                    left_table = self._cast_types_ibis(df_ibis=left_table, 
                                                       target_ibis_backend_schema =self.ibis_backend_schema,
                                                       fields_diff_types=fields_diff_types, 
                                                       target_fields_types=fields_types_right)


            else:

                default_ibis_backend = init_ibis_connection(self.default_ibis_backend_schema)

                #We have differing backends, and we are running locally. Bring both into local memory
                left_table =  self.create_temp_table_ibis(df=self.ibis_df, tablename_prefix="left_table", current_ibis_backend=self.ibis_backend, target_ibis_backend=default_ibis_backend, overwrite=True)
                right_table = right.create_temp_table_ibis(df=right._get_dataframe(), tablename_prefix="right_table", current_ibis_backend=right.ibis_backend, target_ibis_backend=default_ibis_backend, overwrite=True)

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

    def _filter_ibis(self, ibis_expr: Any) -> ir.Table:
        filtered_df = self.ibis_df.filter(ibis_expr)
        return filtered_df



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
        return self.ibis_df.columns
                
### Aggregates        
    
    def count(self) -> int:
       return self._count_ibis()
         
    def _count_ibis(self) -> int:

        return self.ibis_df.count().execute()


### Query        
    
    def sql(self, query:str) -> "IbisDataFrame":
        new_df = self._sql_ibis(query=query).alias(alias=self.generate_tablename(prefix="sql"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def _sql_ibis(self, query:str) -> ir.Table:

        if self.ibis_df.has_name():
            query = query.format(_=self.ibis_df.get_name())
        else:
            query = query.format(_="df")

        return self.ibis_df.sql(query=query)       
    


    def as_dict(self) -> Dict[str, List[Any]] | Any:
        return self._as_dict_ibis()

    def _as_dict_ibis(self) -> Dict[str, List[Any]] | Any:
        return self.ibis_df.to_pyarrow().to_pylist()

    def as_list(self) -> Dict[str, List[Any]] | Any:
        return self._as_list_ibis()

    def _as_list_ibis(self) -> Dict[str, List[Any]] | Any:        
        return self.ibis_df.to_pyarrow().to_pydict()



    def get_first_row_as_dict(self,
        ) -> Dict[Any,Any]:
        return self._get_first_row_as_dict_ibis()

    def _get_first_row_as_dict_ibis(
            self,
        ) -> Dict[Any,Any]:
        
        obj_df = self.head(n=1)
        obj_list = obj_df.ibis_df.to_pyarrow().to_pylist()
        if len(obj_list) > 0:
            return obj_list[0]  
        else:
            return {}
      
    def get_column_as_list(
            self,
            column:str
        ) -> List[Any]:
        
        obj_df = self.select(ibis_expr=column)
        obj_dict = obj_df.ibis_df.to_pyarrow().to_pydict()

        return obj_dict[column]
