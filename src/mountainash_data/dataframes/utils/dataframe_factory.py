
# path: src/mountainash_data/dataframes/utils/dataframe_factory.py

from typing import Union, Any,  Dict, List, Set, Optional, Sequence
import pandas as pd
import polars as pl
import pyarrow as pa

import ibis.expr.types as ir
import ibis


from mountainash_constants import CONST_DATAFRAME_FRAMEWORK
from ..base_dataframe import BaseDataFrame
from ..ibis_dataframe import IbisDataFrame
from .dataframe_utils import DataFrameUtils
from ..utils.ibis_utils import get_default_ibis_backend_schema, init_ibis_connection

class DataFrameFactory:

    @classmethod
    def create_ibis_dataframe_object_from_dataframe(cls,
            df: Optional[Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]] = None,
            ibis_backend: Optional[ibis.BaseBackend] = None,
            ibis_backend_schema: Optional[str] = None,
            tablename_prefix: Optional[str] = None) -> Optional[BaseDataFrame]:

        if df is None:
            return df

        if isinstance(df, BaseDataFrame):
            return df

        obj_df =  cls.create_dataframe_object(df=df,
                                              ibis_backend = ibis_backend,
                                              ibis_backend_schema=ibis_backend_schema,
                                              tablename_prefix = tablename_prefix)

        if not isinstance(obj_df, BaseDataFrame):
            raise ValueError("Unexpected dataframe type returned")

        return obj_df



    @classmethod
    def create_ibis_dataframe_object_from_dictionary(cls,
            data_dict: Dict[str, Union[Sequence[Any],List[Any]]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]] = None,
            ibis_backend: Optional[ibis.BaseBackend] = None,
            ibis_backend_schema: Optional[str] = None,
            tablename_prefix: Optional[str] = None) -> Optional[BaseDataFrame]:

        if data_dict is None or len(data_dict) == 0:
            return None

        if not isinstance(data_dict, dict) and not isinstance(data_dict, list):
            raise ValueError("data_dict must be a dictionary or list of dictionaries")


        df = DataFrameUtils.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict)
        #df = DataFrameUtils.create_pyarrow_table(data_dict=data_dict, column_dict=column_dict)

        obj_df =  cls.create_dataframe_object(df=df,
                                              ibis_backend = ibis_backend,
                                              ibis_backend_schema=ibis_backend_schema,
                                              tablename_prefix = tablename_prefix)

        if not isinstance(obj_df, BaseDataFrame):
            raise ValueError("Unexpected dataframe type returned")

        return obj_df

    @classmethod
    def create_dataframe_object(cls,

            df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
            ibis_backend: Optional[ibis.BaseBackend] = None,
            ibis_backend_schema: Optional[str] = None,
            tablename_prefix: Optional[str] = None

        ) -> Optional[BaseDataFrame]:

        if df is None:
            return df

        if isinstance(df, BaseDataFrame):
            return df

        column_names: List[str] = DataFrameUtils.get_column_names(df=df)

        if column_names is None or len(column_names) == 0:
            return None

        #Identify the ibis backend
        if not ibis_backend:
            if not ibis_backend_schema:
                ibis_backend_schema = get_default_ibis_backend_schema()

            ibis_backend = init_ibis_connection(ibis_schema=ibis_backend_schema)

        #Pre-create an ibis table with the appropriate backend and tablename
        df = DataFrameUtils.cast_dataframe_to_ibis(df=df, ibis_backend=ibis_backend, tablename_prefix=tablename_prefix)

        return IbisDataFrame(df=df, ibis_backend=ibis_backend, ibis_backend_schema=ibis_backend_schema, tablename_prefix=tablename_prefix)



    @classmethod
    def get_supported_dataframe_frameworks(cls) -> Set[str]:
        return {value for value in CONST_DATAFRAME_FRAMEWORK.get_values_set()}
