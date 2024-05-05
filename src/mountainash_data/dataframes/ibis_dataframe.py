from typing import Union, List, Dict, Any, Optional, Sequence, Tuple

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis as ibis
import ibis.expr.types as ir
# import uuid

from .base_dataframe import BaseDataFrame
from mountainash_utils_dataframes.dataframe_utils import DataFrameUtils
# from multipledispatch import dispatch
# from pyarrow import Table as irTable  # assuming you are using pyarrow's Table for ir.Table

class IbisDataFrame(BaseDataFrame):

    def __init__(self, 
                 df:                    Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table],
                 ibis_backend:          Optional[ibis.BaseBackend] = None,
                 ibis_backend_schema:   Optional[str] = None,
                 tablename_prefix:      Optional[str] = None,
                #lineage_history: Optional[List[Any]] = None,
                 ) -> None:

        super().__init__(df=df, 
                         ibis_backend=ibis_backend, 
                         ibis_backend_schema=ibis_backend_schema, 
                         tablename_prefix=tablename_prefix)

        # #set the default backend
        # self.default_ibis_backend_schema = "duckdb"

        # #Init the backend
        # if not ibis_backend:

        #     if not ibis_backend_schema:
        #         ibis_backend_schema = self.default_ibis_backend_schema

        #     self.init_default_ibis_backend(default_ibis_schema=ibis_backend_schema)
        # else:
        #     self.ibis_backend = ibis_backend
        #     ibis_backend_schema = self.ibis_backend.name

        # #We now know the final schema type!
        # self.ibis_backend_schema = ibis_backend_schema

        # # self.ibis_temp_tablename: str = self.init_ibis_temp_tablename()

        # #Init the dataframe
        # self.native_df = df
        # self.ibis_df: ir.Table = self.init_dataframe(df=df, tablename_prefix=tablename_prefix)

        # Now decide if we need to register lineage upon entry
        # if not lineage_history:
        #     lineage_history = []
        # #if the current tablename is not in the lineage history then add it
        # if self.ibis_df.get_name() not in lineage_history:
        #     lineage_history.append(self.get_lineage())
        

        # super().__init__(df=df_prepared, 
        #                  ibis_backend=ibis_backend
        #                  ibis_table_lineage=ibis_table_lineage, 
        #                  )



    # =========
    # Initialisation
        
        # Call a method to set the backend schema after super().__init__
    def init_default_ibis_backend_schema(self):
        self.default_ibis_backend_schema = "duckdb"


    # def init_dataframe(self, df: Any, tablename_prefix:Optional[str] = None) -> ir.Table:
    #     #This is a wrapper around specific ppreparation functions
    #     return self.init_ibis_table(df=df, tablename_prefix=tablename_prefix)

    # def init_default_ibis_backend(self, 
    #                               default_ibis_schema: Optional[str] = None):    
            
    #     if not default_ibis_schema:
    #         default_ibis_schema = self.default_ibis_backend_schema

    #     #TODO: Check that we have a valid schema
    #     if default_ibis_schema not in ["duckdb", "polars", "pandas", "sqlite"]:
    #         raise ValueError(f"Invalid deafault ibis schema: {default_ibis_schema}")

    #     self.ibis_backend = ibis.connect(resource=f"{default_ibis_schema}://")

    #     if not self.ibis_backend:
    #         raise Exception("Ibis client connection not established")


    # def init_ibis_table(self, df, tablename_prefix:Optional[str]=None) -> ir.Table:
        
    #     if not self.ibis_backend:
    #         self.init_default_ibis_backend()

    #     if isinstance(df, ir.Table):
    #         # We already have ibis, but if generated manually, we may lack a name
    #         if df.has_name() and not tablename_prefix:
    #             ibis_df = df
    #         else:
    #             ibis_df = df.alias(alias=self.generate_tablename(prefix=tablename_prefix))

    #     else:
    #         #we have a new table from pandas or polars
    #         tablename = self.generate_tablename(prefix=tablename_prefix)

    #         pyarrow_table: pa.Table = DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=df)
    #         ibis_df = self.ibis_backend.create_table(name=tablename, obj=pyarrow_table, overwrite=True)

        
    #     return ibis_df



#Materialisation Status
    def is_materialised(self) -> bool:
        return isinstance(self.ibis_df, ir.Table)


    def materialise(self) -> ir.Table:

        if isinstance(self.ibis_df, ir.Table):
            return self.ibis_df.execute()
        else:
            raise ValueError("Dataframe could not be not materialised")


# Column Selection
    def select(self, columns: Any) -> "IbisDataFrame":

        new_df: ir.Table = self._select_ibis(columns=columns).alias(alias=self.generate_tablename(prefix="select"))


        # add a lineage record - name and transformations

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def drop(self, columns: Any) -> "IbisDataFrame":

        new_df: ir.Table = self._drop_ibis(columns).alias(self.generate_tablename(prefix="drop"))
 
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       

# Add Columns       
    def mutate(self, ibis_expr: Any, **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._mutate_ibis(ibis_expr=ibis_expr, **kwargs).alias(self.generate_tablename(prefix="mutate"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
# Reshape
    def aggregate(self,  **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._aggregate_ibis( **kwargs).alias(self.generate_tablename(prefix="aggregate"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       

    def pivot_wider(self,  **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._pivot_wider_ibis( **kwargs).alias(self.generate_tablename(prefix="pivot_wider"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
    def pivot_longer(self, **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._pivot_longer_ibis( **kwargs).alias(self.generate_tablename(prefix="pivot_longer"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)


   


#Joins


    def _resolve_join_backend(self, 
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

                right_source_table = right.to_arrow()
                right_table = self.ibis_backend.create_table(name=self.generate_tablename(prefix="right_table"), obj=right_source_table, overwrite=True)

            elif execute_on == "right":
                #create temp table on right

                if not isinstance(right, IbisDataFrame):
                    raise ValueError("Right must be an IbisDataFrame to execute on right")

                right_table = right.ibis_df

                left_source_table = self.to_arrow()
                left_table = right.ibis_backend.create_table(name=self.generate_tablename(prefix="left_table"), obj=left_source_table, overwrite=True)


            else:
                #We have differing backends, and we are running locally. Bring both into local memory
                left_table = ibis.memtable(data=self.to_arrow(), columns=self.get_column_names(), name=self.generate_tablename(prefix="left_table"))
                right_table = ibis.memtable(data=right.to_arrow(), columns=right.get_column_names(), name=right.generate_tablename(prefix="right_table"))


        return (left_table, right_table)


    def inner_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None) -> "IbisDataFrame":

        left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

        new_df: ir.Table = left_table.inner_join(right_table, predicates).alias(self.generate_tablename(prefix="inner_join"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)


    def left_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None) -> "IbisDataFrame":

        left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

        new_df: ir.Table = left_table.left_join(right_table, predicates).alias(self.generate_tablename(prefix="left_join"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)


    def outer_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None) -> "IbisDataFrame":

        left_table, right_table = self._resolve_join_backend(right=right, execute_on=execute_on)

        new_df: ir.Table = left_table.outer_join(right_table, predicates).alias(self.generate_tablename(prefix="outer_join"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)



# Row Selection


    def filter(self, expr: Any) -> "IbisDataFrame":
        """Filter rows based on the given expression."""

        new_df: ir.Table = self.ibis_df.filter(expr).alias(self.generate_tablename(prefix="filter"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

                


    def head(self, n: int) -> "IbisDataFrame":
        """Take the first n rows."""

        new_df = self.ibis_df.head(n).alias(self.generate_tablename(prefix="head"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

#Column Metadata
    def get_column_names(self) -> list:
        return self._get_column_names()   
            
### Aggregates        
    
    def count(self) -> int:
       return self.ibis_df.count().execute()
        


### Query        
    
    def sql(self, query:str) -> "IbisDataFrame":
        new_df = self.ibis_df.sql(query=query).alias(alias=self.generate_tablename(prefix="sql"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)






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