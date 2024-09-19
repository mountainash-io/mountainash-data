from typing import Union, Any,  Dict, List, Set, Optional, Sequence
import pandas as pd
import polars as pl
import pyarrow as pa

from mountainash_utils_dataclasses import DataclassUtils
import ibis.expr.types as ir
import ibis 


from mountainash_constants import CONST_DATAFRAME_FRAMEWORK
from ..base_dataframe import BaseDataFrame
from .dataframe_utils import DataFrameUtils
from ..ibis_dataframe import IbisDataFrame

class DataFrameFactory:

    @classmethod
    def create_ibis_dataframe_object_from_dataframe(cls, 
            df: Optional[Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]] = None,
            ibis_backend: Optional[ibis.BaseBackend] = None,
            ibis_backend_schema: Optional[str] = None,
            tablename_prefix: Optional[str] = None) -> IbisDataFrame:
        
        obj_df =  cls.create_dataframe_object(df=df, 
                                              ibis_backend = ibis_backend,
                                              ibis_backend_schema=ibis_backend_schema,
                                              tablename_prefix = tablename_prefix)

        if not isinstance(obj_df, IbisDataFrame):
            raise ValueError("Unexpected dataframe type returned")

        return obj_df



    @classmethod
    def create_ibis_dataframe_object_from_dictionary(cls, 
            data_dict: Dict[str, Union[Sequence[Any],List[Any]]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]] = None,
            ibis_backend: Optional[ibis.BaseBackend] = None,
            ibis_backend_schema: Optional[str] = None,
            tablename_prefix: Optional[str] = None) -> IbisDataFrame:

        df = DataFrameUtils.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict)
        #df = DataFrameUtils.create_pyarrow_table(data_dict=data_dict, column_dict=column_dict)

        obj_df =  cls.create_dataframe_object(df=df, 
                                              ibis_backend = ibis_backend,
                                              ibis_backend_schema=ibis_backend_schema,
                                              tablename_prefix = tablename_prefix)

        if not isinstance(obj_df, IbisDataFrame):
            raise ValueError("Unexpected dataframe type returned")

        return obj_df

    @classmethod
    def create_dataframe_object(cls,
                                
            df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
            ibis_backend: Optional[ibis.BaseBackend] = None,
            ibis_backend_schema: Optional[str] = None,
            tablename_prefix: Optional[str] = None

        ) -> BaseDataFrame|IbisDataFrame:

        if df is None:
            raise ValueError("create_dataframe_object: No dataframe provided for Ibis DataFrame creation")

        column_names = DataFrameUtils.get_column_names(df=df)
        
        if column_names is None or len(column_names) == 0:
            raise ValueError("create_dataframe_object: No column names found in the dataframe")
        
        if DataFrameUtils._is_recordbatch(df=df):
            df = DataFrameUtils.cast_dataframe_to_arrow(df=df)
               
        return IbisDataFrame(df=df, ibis_backend=ibis_backend, ibis_backend_schema=ibis_backend_schema, tablename_prefix=tablename_prefix)



    @classmethod
    def get_supported_dataframe_frameworks(cls) -> Set[str]:
        return DataclassUtils.get_enum_values_set(enumclass=CONST_DATAFRAME_FRAMEWORK)





