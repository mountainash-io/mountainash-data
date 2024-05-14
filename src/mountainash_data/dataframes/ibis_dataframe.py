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



#Materialisation Status
    def is_materialised(self) -> bool:
        return isinstance(self.ibis_df, ir.Table)


    def materialise(self) -> pd.DataFrame:

        if isinstance(self.ibis_df, ir.Table):
            return self.ibis_df.execute()
        else:
            raise ValueError("Dataframe could not be not materialised")


# Column Selection
    def select(self, ibis_expr: Any) -> "IbisDataFrame":

        new_df: ir.Table = self._select_ibis(ibis_expr=ibis_expr).alias(alias=self.generate_tablename(prefix="select"))
        # add a lineage record - name and transformations

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    # def _select_native(self, columns: Any) -> ir.Table:
    #     return self._select_native(columns=columns)

    def drop(self, columns: Any) -> "IbisDataFrame":

        new_df: ir.Table = self._drop_ibis(columns).alias(alias=self.generate_tablename(prefix="drop"))
 
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
    # def _drop_native(self, columns: Any) -> ir.Table:
    #     return self._drop_ibis(columns=columns)

    def distinct(self) -> "BaseDataFrame":

        new_df: ir.Table = self._distinct_ibis().alias(alias=self.generate_tablename(prefix="distinct"))
 
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    def rename(self, **kwargs) -> "BaseDataFrame":

        new_df: ir.Table = self._rename_ibis(**kwargs).alias(alias=self.generate_tablename(prefix="rename"))
 
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
    
    def try_cast(self, **kwargs) -> "BaseDataFrame":

        new_df: ir.Table = self._try_cast_ibis(**kwargs).alias(alias=self.generate_tablename(prefix="try_cast"))
 
        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

# Add Columns       
    def mutate(self, **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._mutate_ibis( **kwargs).alias(alias=self.generate_tablename(prefix="mutate"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
    # def _mutate_native(self,  **kwargs) -> ir.Table:
    #     return self._mutate_ibis( **kwargs)


# Reshape
    def aggregate(self,  **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._aggregate_ibis( **kwargs).alias(alias=self.generate_tablename(prefix="aggregate"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
    # def _aggregate_native(self, **kwargs) -> ir.Table:
    #     return self._aggregate_ibis(**kwargs)



    def pivot_wider(self,  **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._pivot_wider_ibis( **kwargs).alias(alias=self.generate_tablename(prefix="pivot_wider"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)
       
    # def _pivot_wider_native(self, **kwargs) -> ir.Table:
    #     return self._pivot_wider_ibis(**kwargs)

    def pivot_longer(self, **kwargs) -> "IbisDataFrame":

        new_df: ir.Table = self._pivot_longer_ibis( **kwargs).alias(alias=self.generate_tablename(prefix="pivot_longer"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    # def _pivot_longer_native(self, **kwargs) -> ir.Table:
    #     return self._pivot_longer_ibis(**kwargs)

   


#Joins
    # def _resolve_join_backend_native(self, 
    #                                right:       "BaseDataFrame",
    #                                execute_on:  Optional[str] = None
    #                                ) -> Tuple[ir.Table, ir.Table]:
        
    #     return self._resolve_join_backend_ibis(right=right, execute_on=execute_on)

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

                # right_source_table = right.to_arrow()
                # right_table = self.ibis_backend.create_table(name=self.generate_tablename(prefix="right_table"), obj=right_source_table, overwrite=True)

                right_table = self.create_temp_table_ibis(df_dataframe=right.ibis_df, tablename_prefix="left_table", overwrite=True)


            elif execute_on == "right":
                #create temp table on right

                if not isinstance(right, IbisDataFrame):
                    raise ValueError("Right must be an IbisDataFrame to execute on right")

                right_table = right.ibis_df

                # left_source_table = self.to_arrow()
                # left_table = right.ibis_backend.create_table(name=self.generate_tablename(prefix="left_table"), obj=left_source_table, overwrite=True)

                left_table = self.create_temp_table_ibis(df_dataframe=self.native_df, tablename_prefix="left_table", overwrite=True)


            else:
                #We have differing backends, and we are running locally. Bring both into local memory
                # left_table = ibis.memtable(data=self.to_arrow(), columns=self.get_column_names(), name=self.generate_tablename(prefix="left_table"))
                # right_table = ibis.memtable(data=right.to_arrow(), columns=right.get_column_names(), name=right.generate_tablename(prefix="right_table"))


                left_table = self.create_temp_table_ibis(df_dataframe=self.ibis_df, tablename_prefix="left_table", overwrite=True)
                right_table = self.create_temp_table_ibis(df_dataframe=right.ibis_df, tablename_prefix="right_table", overwrite=True)


        return (left_table, right_table)



    def inner_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None) -> "IbisDataFrame":

        # left_table, right_table = self._resolve_join_backend_ibis(right=right, execute_on=execute_on)
        # new_df: ir.Table = left_table.inner_join(right_table, predicates).alias(self.generate_tablename(prefix="inner_join"))


        new_df: ir.Table = self._inner_join_ibis(right=right, predicates=predicates, execute_on=execute_on).alias(self.generate_tablename(prefix="inner_join"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    # def _inner_join_native(self, 
    #                         right: "BaseDataFrame", 
    #                         predicates: Any,
    #                         execute_on: Optional["str"] = None
    #                        ) -> ir.Table:
    #     return self._inner_join_ibis(right=right, predicates=predicates, execute_on=execute_on)



    def left_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None) -> "IbisDataFrame":

        # left_table, right_table = self._resolve_join_backend_ibis(right=right, execute_on=execute_on)
        # new_df: ir.Table = left_table.left_join(right_table, predicates).alias(self.generate_tablename(prefix="left_join"))

        new_df: ir.Table = self._left_join_ibis(right=right, predicates=predicates, execute_on=execute_on).alias(self.generate_tablename(prefix="left_join"))


        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    # def _left_join_native(self, 
    #                         right: "BaseDataFrame", 
    #                         predicates: Any,
    #                         execute_on: Optional["str"] = None
    #                        ) -> ir.Table:
    #     return self._left_join_ibis(right=right, predicates=predicates, execute_on=execute_on)

    def outer_join(self,             
                   right: "BaseDataFrame", 
                   predicates: Any,
                   execute_on: Optional["str"] = None) -> "IbisDataFrame":

        # left_table, right_table = self._resolve_join_backend_ibis(right=right, execute_on=execute_on)
        # new_df: ir.Table = left_table.outer_join(right_table, predicates).alias(self.generate_tablename(prefix="outer_join"))

        new_df: ir.Table = self._outer_join_ibis(right=right, predicates=predicates, execute_on=execute_on).alias(self.generate_tablename(prefix="outer_join"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    # def _outer_join_native(self, 
    #                         right: "BaseDataFrame", 
    #                         predicates: Any,
    #                         execute_on: Optional["str"] = None
    #                        ) -> ir.Table:
    #     return self._outer_join_ibis(right=right, predicates=predicates, execute_on=execute_on)



# Row Selection


    def filter(self, ibis_expr: Any) -> "IbisDataFrame":
        """Filter rows based on the given expression."""

        new_df: ir.Table = self.ibis_df.filter(ibis_expr).alias(self.generate_tablename(prefix="filter"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

    # def _filter_native(self, ibis_expr: Any) -> ir.Table:
    #     return self._filter_ibis(ibis_expr=ibis_expr)
                


    def head(self, n: int) -> "IbisDataFrame":
        """Take the first n rows."""

        new_df = self.ibis_df.head(n).alias(self.generate_tablename(prefix="head"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)

                


#Column Metadata
    def get_column_names(self) -> list:
        return self._get_column_names_ibis()   
            
### Aggregates        
    
    def count(self) -> int:
       return self._count_ibis()
         



### Query        
    
    def sql(self, query:str) -> "IbisDataFrame":
        new_df = self._sql_ibis(query=query).alias(alias=self.generate_tablename(prefix="sql"))

        return IbisDataFrame(df=new_df, ibis_backend=self.ibis_backend)


    # def _sql_native(self, query:str) -> ir.Table:
    #     return self._sql_ibis(query=query)




    
    def as_dict(self) -> Dict[str, List[Any]] | Any:
        return self._as_dict_ibis()

    # def _as_dict_native(self) -> Dict[str, List[Any]] | Any:
    #     return self._as_dict_ibis()

    def as_list(self) -> Dict[str, List[Any]] | Any:
        return self._as_list_ibis()

    # def _as_list_native(self) -> Dict[str, List[Any]] | Any:
    #     return self._as_list_ibis()

    def get_first_row_as_dict(self,
        ) -> Dict[Any,Any]:
        return self._get_first_row_as_dict_ibis()

    # def _get_first_row_as_dict_native(self,
    #     ) -> Dict[Any,Any]:
    #     return self._get_first_row_as_dict_ibis()



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