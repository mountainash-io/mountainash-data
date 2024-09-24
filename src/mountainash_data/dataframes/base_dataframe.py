
from typing import List, Union, Dict, Any, Optional, Tuple
from abc import ABC, abstractmethod

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis.expr.types as ir
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
                #lineage_history: Optional[List[Any]] = None,
                 ) -> None:
        
        self.native_df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table] = self.init_native_table(df=df)


    #==============
    # Initialisers
    @abstractmethod
    def init_native_table(self, df) -> Any:
        pass

    def generate_tablename(self, prefix: Optional[str] = None) -> str:
        return DataFrameUtils.generate_tablename(prefix=prefix)

    @abstractmethod
    def _get_dataframe(self) -> Any:
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
        return DataFrameUtils.cast_dataframe_to_arrow(df=self._get_dataframe())

    def to_pyarrow_recordbatch(self, batchsize: int = 1) -> List[pa.RecordBatch]:
        return DataFrameUtils.cast_dataframe_to_pyarrow_recordbatch(df=self._get_dataframe(), batchsize=batchsize)


    def to_pandas(self) -> pd.DataFrame:
        return DataFrameUtils.cast_dataframe_to_pandas(df=self._get_dataframe())


    def to_polars(self) -> pl.DataFrame:
        return DataFrameUtils.cast_dataframe_to_polars(df=self._get_dataframe())

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
        obj_dict = DataFrameUtils.cast_dataframe_to_dictonary_of_lists(df=obj_df._get_dataframe())

        return obj_dict[column]


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
    def _resolve_join_backend(self, 
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


 

