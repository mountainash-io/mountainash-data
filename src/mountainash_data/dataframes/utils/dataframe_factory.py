from typing import Union, Any,  Dict, List, Set, Optional, Sequence
import pandas as pd
import polars as pl
import pyarrow as pa

from mountainash_utils_dataclasses import DataclassUtils
import ibis.expr.types as ir
import ibis 


from mountainash_constants import CONST_DATAFRAME_FRAMEWORK
from ..base_dataframe import BaseDataFrame
# from ..pandas_dataframe import PandasDataFrame
# from ..polars_dataframe import PolarsDataFrame
from .dataframe_utils import DataFrameUtils
from ..ibis_dataframe import IbisDataFrame

# from multipledispatch import dispatch
# from pyarrow import Table as irTable  # assuming you are using pyarrow's Table for ir.Table

class DataFrameFactory:

    #Convert between BaseDataFrame types
    # @classmethod
    # def get_materialised_polars_object(cls, obj_dataframe: BaseDataFrame ) -> PolarsDataFrame:

    #     #This should convert any non-polars to a materialised polars dataframe
    #     if isinstance(obj_dataframe, PolarsDataFrame):
    #         if isinstance(obj_dataframe.native_df, pl.LazyFrame):
    #             # return DataFrameFactory.create_dataframe_object(df=obj_dataframe.get_materialised_df(), dataframe_framework=CONST_DATAFRAME_FRAMEWORK.POLARS.value)
    #             return DataFrameFactory.create_polars_dataframe_object(df=obj_dataframe.native_df)

    #         else:
    #             return obj_dataframe
    #     else:
    #         # return DataFrameFactory.create_dataframe_object(df=obj_dataframe.get_materialised_df(), dataframe_framework=CONST_DATAFRAME_FRAMEWORK.POLARS.value)
    #         return DataFrameFactory.create_polars_dataframe_object(df=obj_dataframe.native_df)

    # @classmethod
    # def get_materialised_pandas_object(cls, obj_dataframe: BaseDataFrame ) -> BaseDataFrame|PandasDataFrame:

    #     #This should convert any non-polars to a materialised polars dataframe
    #     if isinstance(obj_dataframe, PandasDataFrame):
    #         return obj_dataframe
    #     else:
    #         return DataFrameFactory.create_dataframe_object(df=obj_dataframe.native_df, dataframe_framework=CONST_DATAFRAME_FRAMEWORK.PANDAS.value)


    # @classmethod
    # def get_materialised_dataframe_object(cls, obj_dataframe: BaseDataFrame, target_framework: str) -> BaseDataFrame:
        
    #     if target_framework == CONST_DATAFRAME_FRAMEWORK.POLARS.value:
    #         return cls.get_materialised_polars_object(obj_dataframe=obj_dataframe)

    #     elif target_framework == CONST_DATAFRAME_FRAMEWORK.PANDAS.value:
    #         return cls.get_materialised_pandas_object(obj_dataframe=obj_dataframe)

    #     else:
    #         raise ValueError(f"Conversion between {obj_dataframe.__class__.__name__} and {target_framework} is not supported")



    # Create Mouintain Ash Dataframe Wrapper types

    # @classmethod
    # def create_pandas_dataframe_object(cls, 
    #         df: Optional[Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table]] = None,
    #         data_dict: Optional[Dict[str, Union[Sequence,List]] | List[Dict[str, Any]]] = None,
    #         column_dict: Optional[Dict[str, str]] = None) -> PandasDataFrame:

    #     obj_df =  cls.create_dataframe_object(df=df, 
    #                                           data_dict=data_dict, 
    #                                           column_dict=column_dict, 
    #                                           dataframe_framework=CONST_DATAFRAME_FRAMEWORK.PANDAS.value)

    #     if not isinstance(obj_df, PandasDataFrame):
    #         raise ValueError("Unexpected dataframe type returned")

    #     return obj_df


    # @classmethod
    # def create_polars_dataframe_object(cls, 
    #         df: Optional[Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table]] = None,
    #         data_dict: Optional[Dict[str, Union[Sequence,List]] | List[Dict[str, Any]]] = None,
    #         column_dict: Optional[Dict[str, str]] = None) -> PolarsDataFrame:
        
    #     obj_df =  cls.create_dataframe_object(df=df, 
    #                                           data_dict=data_dict, 
    #                                           column_dict=column_dict, 
    #                                           dataframe_framework=CONST_DATAFRAME_FRAMEWORK.POLARS.value)

    #     if not isinstance(obj_df, PolarsDataFrame):
    #         raise ValueError("Unexpected dataframe type returned")

    #     return obj_df

    @classmethod
    def create_ibis_dataframe_object_from_dataframe(cls, 
            df: Optional[Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]] = None,
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
                                
            df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
            ibis_backend: Optional[ibis.BaseBackend] = None,
            ibis_backend_schema: Optional[str] = None,
            tablename_prefix: Optional[str] = None

        ) -> BaseDataFrame|IbisDataFrame:

        if df is None:
            raise ValueError("create_dataframe_object: No dataframe provided for Ibis DataFrame creation")

                
        # df = df if isinstance(df, ir.Table) else DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=df)
        return IbisDataFrame(df=df, ibis_backend=ibis_backend, ibis_backend_schema=ibis_backend_schema, tablename_prefix=tablename_prefix)


    # @classmethod
    # def create_dataframe_object_deprecated(cls,
                                
    #         df: Optional[Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table]] = None,
    #         data_dict: Optional[Dict[str, Union[Sequence,List]] | List[Dict[str, Any]]] = None,
    #         column_dict: Optional[Dict[str, str]] = None,
    #         dataframe_framework: Optional[str] = None,
    #         ibis_backend: Optional[ibis.BaseBackend] = None,
    #         tablename_prefix: Optional[str] = None
    #     ) -> BaseDataFrame|IbisDataFrame:

    #     #Autodetect the dataframe framework if not provided
    #     if not dataframe_framework:
    #         # if isinstance(df, pd.DataFrame):
    #         #     dataframe_framework = CONST_DATAFRAME_FRAMEWORK.PANDAS.value
    #         # elif isinstance(df, pl.DataFrame) or isinstance(df, pl.LazyFrame):
    #         #     dataframe_framework = CONST_DATAFRAME_FRAMEWORK.POLARS.value
    #         # if isinstance(df, ir.Table):
    #         dataframe_framework = CONST_DATAFRAME_FRAMEWORK.IBIS.value

    #     if dataframe_framework not in cls.get_supported_dataframe_frameworks():
    #         raise ValueError(f"Unsupported dataframe framework: {dataframe_framework}. dataframe_framework must be provided if creating a datafrom from a Dict or List")


    #     #Convert a List or Dict to a dataframe
    #     if df is None and data_dict is not None:# and column_dict is not None:
    #         df = DataFrameUtils.create_dataframe(data_dict=data_dict, column_dict=column_dict, dataframe_framework=dataframe_framework)


    #     # if dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PANDAS.value:
    #     #     if df is None:
    #     #         raise ValueError("create_dataframe_object: No dataframe provided for Pandas DataFrame creation")

    #     #     df = df if isinstance(df, pd.DataFrame) else DataFrameUtils.cast_dataframe_to_pandas(df_dataframe=df)
    #     #     return PandasDataFrame(df=df)
        
    #     # elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.POLARS.value:
    #     #     if df is None:
    #     #         raise ValueError("create_dataframe_object: No dataframe provided for Polars DataFrame creation")
                
    #     #     df = df if isinstance(df, (pl.DataFrame, pl.LazyFrame)) else DataFrameUtils.cast_dataframe_to_polars(df_dataframe=df)
    #     #     return PolarsDataFrame(df=df)
        
    #     if dataframe_framework == CONST_DATAFRAME_FRAMEWORK.IBIS.value:
    #         if df is None:
    #             raise ValueError("create_dataframe_object: No dataframe provided for Ibis DataFrame creation")
                
    #         df = df if isinstance(df, ir.Table) else DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=df)
    #         return IbisDataFrame(df=df, ibis_backend=ibis_backend, tablename_prefix=tablename_prefix)


    #     else:
    #         raise ValueError(f"Unsupported dataframe framework: {dataframe_framework}")

    # Native Dataframe creation
        
    # @classmethod
    # def create_pandas_dataframe(
    #         cls,
    #         data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
    #         column_dict: Optional[Dict[str, str]]=None) -> pd.DataFrame:

    #     df: pd.DataFrame = DataFrameUtils.create_pandas_dataframe(data_dict=data_dict, column_dict=column_dict)
    #     return df
        
    # @classmethod
    # def create_polars_dataframe(
    #         cls,
    #         data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
    #         column_dict: Optional[Dict[str, str]]=None) -> pl.DataFrame:
        
    #     df: pl.DataFrame = DataFrameUtils.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict)
    #     return df


    #Metadata

    @classmethod
    def get_supported_dataframe_frameworks(cls) -> Set[str]:
        return DataclassUtils.get_enum_values_set(enumclass=CONST_DATAFRAME_FRAMEWORK)





