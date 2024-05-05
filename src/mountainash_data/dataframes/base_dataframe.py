
from typing import List, Union, Dict, Any, Optional, Tuple, Sequence
from abc import ABC, abstractmethod, abstractproperty
import pandas as pd
import polars as pl
import pyarrow as pa
import uuid

import ibis.expr.types as ir
# from ibis.common.deferred import Deferred
# from ibis import _
import ibis
import ibis.expr.types as ir

from .base_dataframe import BaseDataFrame
from .utils.dataframe_utils import DataFrameUtils
from functools import lru_cache 

# @lru_cache
# def get_ibis_connection(backend_type: str,
#                         connection_namespace: Optional[str] = "default"
#                         ):
    

#     if backend_type == "pandas":
#         return ibis.pandas.connect()
#     elif backend_type == "polars":
#         return ibis.polars.connect()
#     elif backend_type == "duckdb":
#         return ibis.duckdb.connect()
#     else:
#         raise ValueError(f"Backend type {backend_type} not supported")

class IbisTableLineageWrapper:  


    def __init__(self,
                 ibis_backend: ibis.BaseBackend, 
                 ibis_tablename: str, 
                 ibis_table: ir.Table):

        self.ibis_backend:      ibis.BaseBackend = ibis_backend
        self.ibis_tablename:    str =           ibis_tablename
        self.ibis_table:        ir.Table =      ibis_table
        self.openlineage_events: List[Any] =    []
        self.substrait_events:  List[Any] =     []

        #also add polars operations here

    def __del__(self):

        if self.ibis_backend:

            existing_tables = self.ibis_backend.list_tables()

            if self.ibis_tablename in existing_tables:
                
                self.ibis_backend.drop_table(self.ibis_tablename)

                print(f"Backend temp table {self.ibis_tablename} closed.")    


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
        
        #default ibis schema
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
        self.native_df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, pa.Table, None] = self.init_native_table(df=df)
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
            raise ValueError(f"Invalid deafault ibis schema: {default_ibis_schema}")

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
            tablename = self.generate_tablename(prefix=tablename_prefix)

            pyarrow_table: pa.Table = DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=df)
            ibis_df = self.ibis_backend.create_table(name=tablename, obj=pyarrow_table, overwrite=True)
        
        return ibis_df



    def generate_tablename(self, prefix: Optional[str] = None) -> str:

        if prefix:
            temp_tablename = f"{prefix}_{str(object=uuid.uuid4())}"
        else:   
            temp_tablename = str(object=uuid.uuid4())

        return temp_tablename

    # @abstractmethod
    def create_table_ibis(self, 
                          name: str,
                          obj: pa.Table,
                          overwrite: Optional[bool] = False,
                          temp_table: Optional[bool] = True,
                          ) -> None:

        #TODO, bsed on the backed, we need to see if we can create a temp table or not

        if not isinstance(obj, pa.Table):
            raise ValueError("obj must be a pyarrow Table")

        if temp_table is None:
            temp_table = True

        self.ibis_backend.create_table(name=name, obj=obj, overwrite=True, temp=temp_table)


    # ==============
    ### Materialisation
        
    @abstractmethod
    def materialise(self) -> Any:
        pass

    def materialize(self) -> Any:
        return self.materialise()

    def execute(self) -> Any:
        return self.materialise()

    def collect(self) -> Any:
        return self.materialise()

    def to_arrow_native(self) -> pa.Table:
        return DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=self.native_df)

    def to_arrow(self) -> pa.Table:
        return DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=self.ibis_df)

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

    @abstractmethod
    def select(self, columns: Any) -> "BaseDataFrame":
        pass

    def _select_ibis(self, columns: Any) -> Any:
        df_cols: Any = self.ibis_df.select(columns=columns)
        return df_cols


    def get_column_as_list(
            self,
            column:str
        ) -> List[Any]:
        
        obj_df = self.select(columns=column)
        obj_dict = DataFrameUtils.cast_dataframe_to_dictonary_of_lists(df=obj_df.ibis_df)

        return obj_dict[column]


    @abstractmethod
    def drop(self, columns: Any) -> "BaseDataFrame":
        pass

    def _drop_ibis(self, columns: Any) -> Any:
        df_cols: Any = self.ibis_df.drop(columns)
        return df_cols

    @abstractmethod
    def mutate(self, ibis_expr: Any, **kwargs) -> "BaseDataFrame":
        pass

    def _mutate_ibis(self, ibis_expr: Any, **kwargs) -> Any:
        df_cols: Any = self.ibis_df.mutate(exprs=ibis_expr, **kwargs)
        return df_cols

    @abstractmethod
    def aggregate(self, **kwargs) -> "BaseDataFrame":
        pass

    def _aggregate_ibis(self, **kwargs) -> Any:

        df_cols: Any = self.ibis_df.aggregate(**kwargs)
        return df_cols

    @abstractmethod
    def pivot_wider(self, **kwargs) -> "BaseDataFrame":
        pass

    def _pivot_wider_ibis(self, **kwargs) -> Any:
        df_cols: Any = self.ibis_df.pivot_wider(**kwargs)
        return df_cols

    @abstractmethod
    def pivot_longer(self, **kwargs) -> "BaseDataFrame":
        pass

    def _pivot_longer_ibis(self, **kwargs) -> Any:
        df_cols: Any = self.ibis_df.pivot_longer(**kwargs)
        return df_cols

    # ==============
    ### Column Metadata

    @abstractmethod
    def get_column_names(self) -> List[str]:
        pass

    def _get_column_names(self) -> List[str]:
        return DataFrameUtils.get_column_names(df=self.ibis_df)

    # ==============
    ### Rows

    @abstractmethod
    def filter(self, expr: Any) -> "BaseDataFrame":
        pass

    def _filter_ibis(self, expr: Any) -> Any:
        filtered_df = self.ibis_df.filter(expr)
        return filtered_df


    @abstractmethod
    def head(self, n: int) -> "BaseDataFrame":
        pass

    def _head_ibis(self, n: int) -> Any:

        if n < 0:
            raise ValueError("n must be greater than or equal to 0")

        df_head = self.ibis_df.head(n=n)
        return df_head


    # ==============
    ### Aggregate

    @abstractmethod
    def count(self) -> int:
        pass

    def _count_ibis(self) -> int:

        row_count: int = DataFrameUtils.count(df=self.ibis_df)
        return row_count    

    # ==============
    ### Formatting as raw data

    @abstractmethod
    def as_dict(self) -> Dict[str, List[Any]] | Any:
        pass

    def _as_dict_ibis(self) -> Dict[str, List[Any]] | Any:

        return DataFrameUtils.cast_dataframe_to_dictonary_of_lists(df=self.ibis_df)

    @abstractmethod
    def as_list(self) -> Dict[str, List[Any]] | Any:
        pass

    def _as_list_ibis(self) -> Dict[str, List[Any]] | Any:
        
        return DataFrameUtils.cast_dataframe_to_list_of_dictionaries(df=self.ibis_df)


    @abstractmethod
    def get_first_row_as_dict(self,
        ) -> Dict[Any,Any]:
        pass

    def _get_first_row_as_dict_ibis(
            self,
        ) -> Dict[Any,Any]:
        
        obj_df = self.head(n=1)
        obj_list = DataFrameUtils.cast_dataframe_to_list_of_dictionaries(df=obj_df.ibis_df)

        if len(obj_list) > 0:
            return obj_list[0]  
        else:
            return {}


    # # ==============
    # # Create Tables
        
    # def create_table(self, 
    #                       source_data_frame: BaseDataFrame,
    #                       target_table_name: str,
    #                       temp_table: bool = True
    #                       ) -> "BaseDataFrame":
        
    #     self.ibis_temp_tablename = table_name

    #     self.init_ibis_table()

    #     return self


    # ==============
    ### Joins
    @abstractmethod
    def _resolve_join_backend(self, 
                                   right: "BaseDataFrame",
                                   execute_on: Optional[str] = None
                                   ) -> Tuple[ir.Table, ir.Table]:
        pass
    # def _resolve_join_backend(self, 
    #                                right: "BaseDataFrame",
    #                                execute_on: Optional[str] = None
    #                                ) -> Tuple[ir.Table, ir.Table]:

    #     if execute_on and execute_on not in ["left", "right"]:
    #         raise ValueError("execute_on must be one of 'left', 'right' or None")


    #     if self.ibis_backend == right.ibis_backend:
    #         left_table = self.get_ibis_table()
    #         right_table = right.get_ibis_table()


    #     else:

    #         #This only makes sense for different backends!
    #         if execute_on == "left":
    #             #create temp table on left (self) backend
    #             left_table = self.get_ibis_table()
    #             right_source_table = right.get_ibis_table()

    #             if self.ibis_backend is None:
    #                 raise Exception("Ibis client connection not established")

    #             self.ibis_backend.create_table(name=f"temp_left_{right.get_ibis_tablename()}", obj=right_source_table.to_pyarrow(), overwrite=True, temp=True)

    #             #right table points to the temp table on the left backend
    #             right_table = self.ibis_backend.table(name=f"temp_left_{right.get_ibis_tablename()}")

    #         elif execute_on == "right":
    #             #create temp table on right
    #             left_source_table = self.get_ibis_table()
    #             right_table = right.get_ibis_table()

    #             if right.ibis_backend is None:
    #                 raise Exception("Ibis client connection not established")

    #             right.ibis_backend.create_table(name=f"temp_right_{self.get_ibis_tablename()}", obj=left_source_table.to_pyarrow(), overwrite=True, temp=True)

    #             #left table points to the temp table on the right backend
    #             left_table = right.ibis_backend.table(name=f"temp_right_{self.get_ibis_tablename()}")

    #         else:
    #             left_table = self.get_ibis_memtable()
    #             right_table = right.get_ibis_memtable()



    #     return (left_table, right_table)


    @abstractmethod
    def inner_join(self, 
             right: "BaseDataFrame", 
             predicates: Any,
             execute_on: Optional["str"] = None
            ) -> "BaseDataFrame":
        pass

    def _inner_join_ibis(self, 
             right: "BaseDataFrame", 
             predicates: Any,
             execute_on: Optional["str"] = None
            ) -> Any:

        left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

        return left_table.inner_join(right=right_table, predicates=predicates, rname="")

    @abstractmethod
    def left_join(self, 
             right: "BaseDataFrame", 
             predicates: Any,
             execute_on: Optional["str"] = None
            ) -> "BaseDataFrame":
        pass

    def _left_join_ibis(self, 
             right: "BaseDataFrame", 
             predicates: Any,
             execute_on: Optional["str"] = None
            ) -> Any:

        left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

        return left_table.left_join(right=right_table, predicates=predicates)


    @abstractmethod
    def outer_join(self, 
             right: "BaseDataFrame", 
             predicates: Any,
             execute_on: Optional["str"] = None
            ) -> "BaseDataFrame":
        pass

    def _outer_join_ibis(self, 
             right: "BaseDataFrame", 
             predicates: Any,
             execute_on: Optional["str"] = None
            ) -> Any:

        left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

        return left_table.outer_join(right=right_table, predicates=predicates)
    

    #     # left_table = self.get_ibis_table()
    #     # right_table = right.get_ibis_table()

    #     # #Should we only do this when the backends are differrent?
    #     # if self.ibis_backend != right.ibis_backend:
    #     #     #We need to decide when it is smart or dumb to do this! What is the memory impact?
    #     #     left_table = self.get_ibis_memtable()
    #     #     right_table = right.get_ibis_memtable()
    #     # else:
    #     #     print("outer_join using shared connection")

        



    # @abstractmethod
    # def get_native_table(self) ->  Any:
    #     return self.materialise()


    # ==============
    ### Querying

    @abstractmethod
    def sql(self, query: str) -> "BaseDataFrame":
        pass

    def _sql_ibis(self, query:str) -> Any:


        if self.ibis_df.has_name():
            query = query.format(_=self.ibis_df.get_name())
        else:
            query = query.format(_="df")




        #We have to call execute here - otherwise we only return an expression with no reference to the backward chain of operations.
        # # then when we call materialise later we get an error as upstream has been garbage collected.
        # if isinstance(self.df, ir.Table):

        #     print(f"Creating View: {self.get_ibis_tablename()}")
        #     self.df.alias(alias=self.get_ibis_tablename())
        #     print(f"Post Create View: {self.ibis_backend.list_tables()}")

        #     result = self.df.sql(query=query).execute()            
        # else:
        # result = self.get_ibis_table().sql(query=query).execute()

        # return result




    # ==============
    ### Ibis Interface

    # def set_ibis_backend(self) -> None:
    #     ibis.set_backend(backend="polars")

    # @abstractmethod
    # def init_ibis_connection(self) -> None:
    #     pass

    # def init_ibis_table(self) -> None:
        
    #     if not self.ibis_backend:
    #         self.init_ibis_connection()

    #     if not self.ibis_backend:
    #         raise Exception("Ibis client connection not established")

    #     tablename = self.get_ibis_tablename()
    #     existing_tables = self.ibis_backend.list_tables()

    #     if tablename not in existing_tables:
    #         # print(f"======================================")
    #         # print(f"Pre: df is: {type(self.df)}")
    #         # print(f"Pre: df is: {self.df}")
    #         # print(f"Pre: {self.ibis_backend.list_tables()}")
    #         # print(f"Creating table: {self.get_ibis_tablename()}")

    #         self.ibis_table = self.ibis_backend.create_table(name=self.get_ibis_tablename(), obj=self.df, overwrite=True, temp=False)

    #         self.append_lineage_record(ibis_table=self.ibis_table)

    #         # print(f"Post Create Table: {self.ibis_backend.list_tables()}")


    # def init_ibis_tablename(self) -> str:

    #     if not self.ibis_temp_tablename:
    #         self.ibis_temp_tablename = str(object=uuid.uuid4())

    #     return self.ibis_temp_tablename

    # def get_ibis_tablename(self) -> str:

    #     if not self.ibis_temp_tablename:
    #         return self.init_ibis_tablename()

    #     return self.ibis_temp_tablename


    # def get_ibis_table(self) ->  ir.Table:


    #     if self.ibis_backend is None:
    #         self.init_ibis_connection()

    #     if self.ibis_backend is None:
    #         raise Exception("Ibis client connection not established")

    #     if isinstance(self.df, ir.Table):
    #         return self.df
    #     else:
    #         self.init_ibis_table()

    #         return self.ibis_backend.table(name=self.get_ibis_tablename())

        # if self.ibis_table:
        #     return self.ibis_table
        # else:
        #     raise Exception("Ibis table not established") 




    # def get_ibis_memtable(self) -> ir.Table:

    #     table = ibis.memtable(data=self.to_arrow(), columns=self.get_column_names())

    #     return table


    # def __del__(self):

    #     if self.get_lineage():

    #         for record in self.get_lineage():
    #             for key, value in record.items():
    #                 del value

        # if self.ibis_backend:

        #     existing_tables = self.ibis_backend.list_tables()
        #     tablename = self.get_ibis_tablename()

        #     if tablename in existing_tables:
                
        #         self.ibis_backend._remove_table(tablename)

        #         print(f"Backend temp table {self.get_ibis_tablename()} closed.")


    # @abstractmethod
    # def create_column_constant_value(self, column_name: str, value: Any) -> None:
    #     pass

 



        



    # def get_ibis_table(self) ->  ir.Table:

    #     if not self.ibis_client:
    #         self.init_ibis_connection()

    #     return self.ibis_client.table("df") 


    # def create_filter_expression_(self, column_name: str, operator: str, value: Any) -> Deferred:

    #     expr: Deferred

    #     if operator == '>':
    #         expr = _[column_name] > value
    #     elif operator == '<':
    #         expr = _[column_name] < value
    #     elif operator == '==':
    #         expr = _[column_name] == value
    #     elif operator == '!=':
    #         expr = _[column_name] != value
    #     elif operator == 'isin':
    #         expr = _[column_name].is_in(value)
    #     else:
    #         raise Exception(f"Operator {operator} not supported")

    #     return expr

    # def prepare_validation_data_ibis(
    #     self,
    #     df_report_batch:    ir.Table,
    #     df_report_accounts: ir.Table,
    #     df_report_account_holders: ir.Table

    #     ) -> pd.DataFrame:

    #     ir_report_batch: ir.Table = df_report_batch[self.get_field_list_batch()]
    #     ir_report_accounts: ir.Table = df_report_accounts[self.get_field_list_accounts()]
    #     ir_report_account_holders: ir.Table = df_report_account_holders[self.get_field_list_account_holders()]

    #     df_validation_data: ir.Table = (
    #         ir_report_batch.inner_join(
    #             right=ir_report_accounts, 
    #             predicates=["batch_id", "record_id"]
    #         ).inner_join(
    #             right=ir_report_account_holders, 
    #             predicates=["batch_id"]
    #         )
    #     )

    #     return DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=df_validation_data)


