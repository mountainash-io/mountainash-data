
from typing import List, Union, Dict, Any, Optional, Tuple
from abc import ABC, abstractmethod

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis.expr.types as ir
import ibis

from .utils.dataframe_utils import DataFrameUtils

# class IbisTableLineageWrapper:  

#     def __init__(self,
#                  ibis_backend:      ibis.BaseBackend, 
#                  ibis_tablename:    str, 
#                  ibis_table:        ir.Table):

#         self.ibis_backend:          ibis.BaseBackend = ibis_backend
#         self.ibis_tablename:        str =           ibis_tablename
#         self.ibis_table:            ir.Table =      ibis_table
#         self.openlineage_events:    List[Any] =    []
#         self.substrait_events:      List[Any] =     []

#         #also add polars operations here

#     def __del__(self):

#         if self.ibis_backend:

#             existing_tables = self.ibis_backend.list_tables()

#             if self.ibis_tablename in existing_tables:
                
#                 self.ibis_backend.drop_table(name=self.ibis_tablename)

#                 print(f"Backend temp table {self.ibis_tablename} closed.")


class BaseDataFrame(ABC):

    # df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame]
    # df: Any
    
    def __init__(self, 
                 df:                    Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
                 ibis_backend:          Optional[ibis.BaseBackend] = None,
                 ibis_backend_schema:   Optional[str] = None,
                 tablename_prefix:      Optional[str] = None,
                #lineage_history: Optional[List[Any]] = None,
                 ) -> None:
        
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
        self.native_df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, pa.Table] = self.init_native_table(df=df)
        self.ibis_df: ir.Table = self.init_ibis_table(df=df, tablename_prefix=tablename_prefix)


    #==============
    # Initialisers
    @abstractmethod
    def init_native_table(self, df) -> Any:
        pass

    @abstractmethod
    def init_default_ibis_backend_schema(self) -> None:
        pass


    def init_default_ibis_backend(self, 
                                  default_ibis_schema: Optional[str] = None):    
            
        if not default_ibis_schema:
            default_ibis_schema = self.default_ibis_backend_schema

        #TODO: Check that we have a valid schema
        if default_ibis_schema not in ["duckdb", "polars", "pandas", "sqlite"]:
            raise ValueError(f"Invalid default ibis schema: {default_ibis_schema}")

        self.ibis_backend = ibis.connect(resource=f"{default_ibis_schema}://")

        if not self.ibis_backend:
            raise Exception("Ibis client connection not established")


    def init_ibis_table(self, df, tablename_prefix:Optional[str]=None) -> ir.Table:
        
        if not self.ibis_backend:
            self.init_default_ibis_backend()

        if isinstance(df, ir.Table):
            # We already have ibis, but if generated manually, we may lack a name
            if df.has_name() and not tablename_prefix:
                ibis_df = df
            else:
                ibis_df = df.alias(alias=self.generate_tablename(prefix=tablename_prefix))

        else:
            #we have a new table from pandas or polars
            ibis_df = DataFrameUtils.create_temp_table_ibis(df_dataframe=df, tablename_prefix=tablename_prefix, ibis_backend=self.ibis_backend)
        
        return ibis_df



    def generate_tablename(self, prefix: Optional[str] = None) -> str:

        return DataFrameUtils.generate_tablename(prefix=prefix)

    def create_temp_table_ibis(self,
                          df_dataframe: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
                          tablename_prefix: Optional[str] = None,
                          ibis_backend: Optional[ibis.BaseBackend] = None,
                          overwrite: Optional[bool] = True,
            ) -> ir.Table:
        
        if ibis_backend is None:
            ibis_backend = self.ibis_backend

        ibis_df = DataFrameUtils.create_temp_table_ibis(df_dataframe=df_dataframe, 
                                                        tablename_prefix=tablename_prefix, 
                                                        ibis_backend=ibis_backend,
                                                        overwrite=overwrite)
        return ibis_df

    @abstractmethod
    def convert_backend_schema(self, new_backend_schema:str) -> "BaseDataFrame":
        pass


    # ==============
    ### Materialisation
        
    @abstractmethod
    def materialise(self) -> Union[pd.DataFrame, pl.DataFrame]:
        pass

    def materialize(self) -> Union[pd.DataFrame, pl.DataFrame]:
        return self.materialise()

    def execute(self) -> Union[pd.DataFrame, pl.DataFrame]:
        return self.materialise()

    def collect(self) -> Union[pd.DataFrame, pl.DataFrame]:
        return self.materialise()

    def to_arrow(self) -> pa.Table:
        return DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=self.ibis_df)

    def to_pandas(self) -> pd.DataFrame:
        return DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=self.ibis_df)


    def to_polars(self) -> pl.DataFrame:
        return DataFrameUtils.cast_dataframe_to_polars(df_dataframe=self.ibis_df)

    # def append_lineage_record(self, ibis_table: ir.Table) -> None:

    #     #TODO: Cater for full lineage of native and ibis transformations here.
    #     # Schema could be functioncall, kwargs, expression, execution plan as well as the table references

    #     if self.ibis_backend is not None:

    #         objIbisTableLineageWrapper = IbisTableLineageWrapper(
    #             ibis_backend=self.ibis_backend,
    #             ibis_tablename=self.get_ibis_tablename(),
    #             ibis_table=ibis_table
    #         )

    #         record = {self.get_ibis_tablename(): objIbisTableLineageWrapper}
    #         self.ibis_table_lineage.append(record)

    # def get_lineage(self) -> List[Dict[str, Any]]:
    #     return self.ibis_table_lineage.copy()

    # ==============
    ### Columns

    def get_column_as_list(
            self,
            column:str
        ) -> List[Any]:
        
        obj_df = self.select(ibis_expr=column)
        obj_dict = DataFrameUtils.cast_dataframe_to_dictonary_of_lists(df=obj_df.ibis_df)

        return obj_dict[column]


    def get_columns_as_dict(
            self,
            key_column:str,
            value_column:str
        ) -> Dict[Any,Any]:
        
        obj_df = self.select([key_column, value_column])
        obj_dict = DataFrameUtils.cast_dataframe_to_list_of_dictionaries(df=obj_df.ibis_df)

        #create a dictionary of key: value
        obj_dict = {x[key_column]: x[value_column] for x in obj_dict}

        return obj_dict

    @abstractmethod
    def select(self, ibis_expr: Any) -> "BaseDataFrame":
        pass

    @abstractmethod
    def drop(self, columns: Any) -> "BaseDataFrame":
        pass



    @abstractmethod
    def distinct(self) -> "BaseDataFrame":
        pass



    @abstractmethod
    def rename(self, **kwargs) -> "BaseDataFrame":
        pass


    @abstractmethod
    def try_cast(self, **kwargs) -> "BaseDataFrame":
        pass


    @abstractmethod
    def mutate(self,  **kwargs) -> "BaseDataFrame":
        pass


    @abstractmethod
    def aggregate(self, **kwargs) -> "BaseDataFrame":
        pass



    @abstractmethod
    def pivot_wider(self, **kwargs) -> "BaseDataFrame":
        pass



    @abstractmethod
    def pivot_longer(self, **kwargs) -> "BaseDataFrame":
        pass








    # ==============
    ### Rows

    @abstractmethod
    def filter(self, ibis_expr: Any) -> "BaseDataFrame":
        pass

    def _filter_ibis(self, ibis_expr: Any) -> ir.Table:
        filtered_df = self.ibis_df.filter(ibis_expr)
        return filtered_df


    @abstractmethod
    def head(self, n: int) -> "BaseDataFrame":
        pass




    @abstractmethod
    def union(self, **kwargs) -> "BaseDataFrame":
        pass



    @abstractmethod
    def order_by(self, **kwargs) -> "BaseDataFrame":
        pass

    # ==============
    ### Column Metadata

    @abstractmethod
    def get_column_names(self) -> List[str]:
        pass   



    # ==============
    ### Aggregate

    @abstractmethod
    def count(self) -> int:
        pass




    # ==============
    ### Formatting as raw data

    @abstractmethod
    def as_dict(self) -> Dict[str, List[Any]] | Any:
        pass



    @abstractmethod
    def as_list(self) -> Dict[str, List[Any]] | Any:
        pass




    @abstractmethod
    def get_first_row_as_dict(self,
        ) -> Dict[Any,Any]:
        pass





    # ==============
    ### Joins
    
    @abstractmethod
    def _resolve_join_backend_ibis(self, 
                                   right: "BaseDataFrame",
                                   execute_on: Optional[str] = None
                                   ) -> Tuple[ir.Table, ir.Table]:
        pass



    @abstractmethod
    def inner_join(self, 
             right: "BaseDataFrame", 
             predicates: Any,
             execute_on: Optional["str"] = None,
             **kwargs
            ) -> "BaseDataFrame":
        pass



    @abstractmethod
    def left_join(self, 
             right: "BaseDataFrame", 
             predicates: Any,
             execute_on: Optional["str"] = None,
             **kwargs
            ) -> "BaseDataFrame":
        pass



    @abstractmethod
    def outer_join(self, 
             right: "BaseDataFrame", 
             predicates: Any,
             execute_on: Optional["str"] = None,
             **kwargs
            ) -> "BaseDataFrame":
        pass


    
    # ==============
    ### Querying

    @abstractmethod
    def sql(self, query: str) -> "BaseDataFrame":
        pass


    def _sql_ibis(self, query:str) -> ir.Table:

        if self.ibis_df.has_name():
            query = query.format(_=self.ibis_df.get_name())
        else:
            query = query.format(_="df")

        return self.ibis_df.sql(query=query)        

