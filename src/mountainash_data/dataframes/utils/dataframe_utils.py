import pandas as pd
import polars as pl
import pyarrow as pa
import ibis
from typing import Union, Any,  Dict, List, Optional, Sequence

import uuid
from mountainash_constants import CONST_DATAFRAME_FRAMEWORK
from mountainash_utils_dataclasses import DataclassUtils
import ibis.expr.types as ir

# from multipledispatch import dispatch
# from pyarrow import Table as irTable  # assuming you are using pyarrow's Table for ir.Table


class DataFrameUtils:


    # @staticmethod
    # def get_supported_dataframe_frameworks() -> set:
    #     return DataclassUtils.get_enum_values_set(enumclass=CONST_DATAFRAME_FRAMEWORK)

    @classmethod
    def create_dataframe(
            cls,
            dataframe_framework: str, 
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None
         ) -> Union[pd.DataFrame, pl.DataFrame, ir.Table]:

        if not dataframe_framework:
            raise ValueError("dataframe_framework must be specified")

        if dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PANDAS.value:
            return cls.create_pandas_dataframe(data_dict=data_dict, column_dict=column_dict)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.POLARS.value:
            return cls.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.IBIS.value:
            return cls.create_ibis_dataframe(data_dict=data_dict, column_dict=column_dict)
        else:
            raise ValueError(f"Unsupported dataframe framework: {dataframe_framework}")
        
    @staticmethod
    def create_pandas_dataframe(
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None) -> pd.DataFrame:

        if column_dict:

            if isinstance(data_dict, dict):
                return pd.DataFrame(data_dict).rename(columns=column_dict, errors='ignore')

            elif isinstance(data_dict, list):
                return pd.DataFrame(data_dict).rename(columns=column_dict, errors='ignore')
        else:
            return pd.DataFrame(data_dict)

    @staticmethod
    def create_polars_dataframe(
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None) -> pl.DataFrame:
        
        if column_dict:

            if isinstance(data_dict, dict):
                return pl.DataFrame(data=data_dict).rename(mapping=column_dict)

            elif isinstance(data_dict, list):
                return pl.DataFrame(data=data_dict).rename(mapping=column_dict)
        else:
            return pl.DataFrame(data=data_dict)
        
    @staticmethod
    def create_ibis_dataframe(
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None) -> ir.Table:
        
        df = DataFrameUtils.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict)
        columns = DataFrameUtils.get_column_names(df)

        return ibis.memtable(data=df, columns=columns)    

    @staticmethod
    def cast_dataframe_to_pandas(
        df_dataframe: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table]) -> pd.DataFrame:
        """Casts the input dataframe to a Pandas DataFrame.

        Supported input types are:
            - Pandas DataFrame
            - Polars DataFrame 
            - Polars LazyFrame
        
        Args:
            df_datafile: The input dataframe to cast.

        Returns:
            The dataframe cast to a Pandas DataFrame.

        Raises:
            TypeError: If the input dataframe type is unsupported.

        Example:
            df = DataframeUtils.cast_dataframe_to_pandas(polars_df)
        """
        if isinstance(df_dataframe, pd.DataFrame):
            return df_dataframe
        elif isinstance(df_dataframe, pl.DataFrame):
            return df_dataframe.to_pandas()
        elif isinstance(df_dataframe, pl.LazyFrame):
            return df_dataframe.collect().to_pandas()       
        elif isinstance(df_dataframe, ir.Table):
            return df_dataframe.execute()

        else:
            raise TypeError("Unsupported dataframe type")



    @staticmethod
    def cast_dataframe_to_polars(
        df_dataframe: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table]) -> pl.DataFrame:
        """Converts the input dataframe to a Polars DataFrame.

        Supported input types are:
            - Pandas DataFrame
            - Polars DataFrame 
            - Polars LazyFrame
        
        Args:
            df_datafile: The input dataframe to convert.

        Returns:
            The dataframe converted to a Polars DataFrame.

        Raises:
            TypeError: If the input dataframe type is unsupported.

        Example:
            polars_df = DataframeUtils.cast_dataframe_to_polars(pandas_df)
        """
        if isinstance(df_dataframe, pl.DataFrame):
            return df_dataframe
            
        elif isinstance(df_dataframe, pl.LazyFrame):
            return df_dataframe.collect()
        elif isinstance(df_dataframe, pd.DataFrame):
            return pl.from_pandas(data=df_dataframe)
        elif isinstance(df_dataframe, ir.Table):
            return pl.from_pandas(data=df_dataframe.to_pandas())
            # return df_dataframe.to_polars()
        else:
            raise TypeError("Unsupported dataframe type")


    @staticmethod
    def create_temp_table_ibis(
                          df_dataframe: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
                          tablename_prefix: Optional[str] = None,
                          ibis_backend: Optional[ibis.BaseBackend] = None,
                          overwrite: Optional[bool] = True,
            ) -> ir.Table:

        if not isinstance(df_dataframe, pa.Table):
            df_dataframe = DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=df_dataframe)

        if overwrite is None:
            overwrite = True
        
        tablename = DataFrameUtils.generate_tablename(prefix=tablename_prefix)

        #Some ibis backends are Temp tables by default
        if ibis_backend is not None:
            if ibis_backend.supports_temporary_tables:   
                new_table =  ibis_backend.create_table(name = tablename, obj=df_dataframe, overwrite=overwrite, temp=True)
            else:
                new_table =  ibis_backend.create_table(name = tablename, obj=df_dataframe, overwrite=overwrite)
        else:
            #Otherwise create a memory table on the default backend.
            new_table  = ibis.memtable(data=df_dataframe, 
                                columns=DataFrameUtils.get_column_names(df_dataframe), 
                                name=tablename) 
        return new_table


    @staticmethod
    def cast_dataframe_to_ibis(
        df_dataframe: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table],
        ibis_backend: Optional[ibis.BaseBackend] = None,
        tablename_prefix: Optional[str] = None) -> ir.Table:
        """Converts the input dataframe to a Polars DataFrame.

        Supported input types are:
            - Pandas DataFrame
            - Polars DataFrame 
            - Polars LazyFrame
        
        Args:
            df_datafile: The input dataframe to convert.

        Returns:
            The dataframe converted to a Polars DataFrame.

        Raises:
            TypeError: If the input dataframe type is unsupported.

        Example:
            polars_df = DataframeUtils.cast_dataframe_to_polars(pandas_df)
        """
        ibis.set_backend(backend="polars")
 

        if isinstance(df_dataframe, pl.DataFrame):
            df = DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=df_dataframe)
            return ibis.memtable(data=df_dataframe, 
                                 columns=DataFrameUtils.get_column_names(df_dataframe), 
                                 name=DataFrameUtils.generate_tablename(prefix=tablename_prefix))
        elif isinstance(df_dataframe, pl.LazyFrame):
            df = DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=df_dataframe)
            return ibis.memtable(data=df_dataframe.collect(), 
                                 columns=DataFrameUtils.get_column_names(df_dataframe), 
                                 name=DataFrameUtils.generate_tablename(prefix=tablename_prefix))
        elif isinstance(df_dataframe, pd.DataFrame):
            df = DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=df_dataframe)
            return ibis.memtable(data=df_dataframe, 
                                 columns=DataFrameUtils.get_column_names(df_dataframe), 
                                 name=DataFrameUtils.generate_tablename(prefix=tablename_prefix))
        elif isinstance(df_dataframe, ir.Table):
            return ibis.memtable(data=df_dataframe, 
                                 columns=DataFrameUtils.get_column_names(df_dataframe), 
                                 name=DataFrameUtils.generate_tablename(prefix=tablename_prefix))
        else:
            raise TypeError("Unsupported dataframe type")

    @staticmethod
    def cast_dataframe_to_arrow(df_dataframe: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, pa.Table, ir.Table]
        ) -> pa.Table:
        """Converts the input dataframe to an Apache Arrow Table.

        Supported input types are:
        - Pandas DataFrame
        - Polars DataFrame
        - Polars LazyFrame
        - Apache Arrow Table

        Args:
            df_dataframe: The input dataframe to convert.

        Returns:
            The dataframe converted to an Apache Arrow Table.

        Raises:
            TypeError: If the input dataframe type is unsupported.

        Example:
            arrow_table = cast_dataframe_to_arrow(pandas_df)
        """
        if isinstance(df_dataframe, pa.Table):
            return df_dataframe
        elif isinstance(df_dataframe, pl.DataFrame):
            return df_dataframe.to_arrow() 
        elif isinstance(df_dataframe, pl.LazyFrame):
            return df_dataframe.collect().to_arrow() 
        elif isinstance(df_dataframe, pd.DataFrame):
            return pa.Table.from_pandas(df_dataframe)
        elif isinstance(df_dataframe, ir.Table):
            return df_dataframe.to_pyarrow()
        else:
            raise TypeError(f"Unsupported dataframe type: {type(df_dataframe)}")





    @staticmethod
    def cast_dataframe_to_dictonary_of_lists(df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table]) -> Dict[Any,List[Any]]:
        """Converts a DataFrame to a dictionary.
        
        Args:
            df: The input DataFrame.
            orient: The orientation of the output dictionary.
                'records' returns {"column": [values]}
                'list' returns [{"column": value}]
        
        Returns:
            The converted dictionary.
            
        Raises:
            ValueError: If invalid orientation.
            TypeError: If unsupported DataFrame type.

        Example:
            dict_df = DataframeUtils.cast_dataframe_to_dictonary_of_lists(df, 'list')
        """

        if isinstance(df, pd.DataFrame):

            return df.to_dict(orient='list')
            
        elif isinstance(df, pl.DataFrame):
                return df.to_dict(as_series=False) 
            
        elif isinstance(df, pl.LazyFrame):
                return df.collect().to_dict(as_series=False) 

        elif isinstance(df, ir.Table):

            return df.execute().to_dict(orient='list')

            # if orient == 'list':
            #     return df.execute().to_dict(orient='records')
            # else:
            #     return df.execute().to_dict(orient='series')          
        else:
            raise TypeError("Unsupported dataframe type")
        

    @staticmethod
    def cast_dataframe_to_list_of_dictionaries(df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table]) -> List[Dict[Any,Any]]:
        """Converts a DataFrame to a dictionary.
        
        Args:
            df: The input DataFrame.
            orient: The orientation of the output dictionary.
                'records' returns {"column": [values]}
                'list' returns [{"column": value}]
        
        Returns:
            The converted dictionary.
            
        Raises:
            ValueError: If invalid orientation.
            TypeError: If unsupported DataFrame type.

        Example:
            dict_df = DataframeUtils.cast_dataframe_to_list_of_dictionaries(df, 'list')
        """

        if isinstance(df, pd.DataFrame):

            return df.to_dict(orient='records')
           
        elif isinstance(df, pl.DataFrame):
                return df.to_dicts() 
            
        elif isinstance(df, pl.LazyFrame):
                return df.collect().to_dicts() 

        elif isinstance(df, ir.Table):

            return df.execute().to_dict(orient='records')
        else:
            raise TypeError("Unsupported dataframe type")



    #Columns

    @staticmethod            
    def drop(df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table], 
                     columns: List[str]|str
                     ) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table]:

        if isinstance(columns, str):
            columns = [columns]

        if isinstance(df, pd.DataFrame):
            return df.drop(columns=columns, errors='ignore')
        elif isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            return df.drop(columns)
        elif isinstance(df, (ir.Table)):
            return df.drop(*columns)
        else:
            raise ValueError("Unsupported DataFrame type")


    @staticmethod            
    def select( df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table], 
                        columns: List[str]|str
                        ) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table]:

        if isinstance(columns, str):
            columns = [columns]

        if isinstance(df, pd.DataFrame):
            return df[columns]
        elif isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            return df.select(columns)
        elif isinstance(df, (ir.Table)):
            return df.select(columns)
        else:
            raise ValueError("Unsupported DataFrame type")
        
    # Column Metadata
    @staticmethod        
    def get_column_names(df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table] ) -> List:

        if isinstance(df, pd.DataFrame):
            return df.columns.tolist()
        elif isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            return df.columns
        elif isinstance(df, (ir.Table)):
            return df.columns
        else:
            raise ValueError("Unsupported DataFrame type")


    #Rows
        
    @staticmethod            
    def head( df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table], 
              n: int
                        ) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table]:

        if n < 0:
            raise ValueError("n must be greater than or equal to 0")

        if isinstance(df, pd.DataFrame):
            return df.head(n=n)
        elif isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            return df.head(n=n)
        elif isinstance(df, (ir.Table)):
            return df.head(n=n)
        else:
            raise ValueError("Unsupported DataFrame type")        
        
    @staticmethod            
    def count( df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table], 
                        ) -> int:

        if isinstance(df, pd.DataFrame):
            return df.shape[0]
        elif isinstance(df, (pl.DataFrame)):
            dict_count =  df.select(pl.len()).rows(named=True) 
            return dict_count[0]["len"]
        elif isinstance(df, (pl.LazyFrame)):
            dict_count =  df.select(pl.len()).collect().rows(named=True) 
            return dict_count[0]["len"]
        elif isinstance(df, (ir.Table)):
            return df.count().execute()
        else:
            raise ValueError("Unsupported DataFrame type")  
        

    @staticmethod            
    def generate_tablename(prefix: Optional[str] = None) -> str:

        if prefix:
            temp_tablename = f"{prefix}_{str(object=uuid.uuid4())}"
        else:   
            temp_tablename = str(object=uuid.uuid4())

        return temp_tablename        