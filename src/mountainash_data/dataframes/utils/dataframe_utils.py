import pandas as pd
import polars as pl
import pyarrow as pa
import ibis
from typing import Union, Any,  Dict, List, Optional, Sequence

import uuid
from mountainash_constants import CONST_DATAFRAME_FRAMEWORK
# from mountainash_utils_dataclasses import DataclassUtils
import ibis.expr.types as ir

# from multipledispatch import dispatch
# from pyarrow import Table as irTable  # assuming you are using pyarrow's Table for ir.Table


class DataFrameUtils:


    # @staticmethod
    # def get_supported_dataframe_frameworks() -> set:
    #     return DataclassUtils.get_enum_values_set(enumclass=CONST_DATAFRAME_FRAMEWORK)


    #--------------------
    # Casting Dictionary to Dataframe

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
            return cls.create_pandas_dataframe(cls, data_dict=data_dict, column_dict=column_dict)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.POLARS.value:
            return cls.create_polars_dataframe(cls, data_dict=data_dict, column_dict=column_dict)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.IBIS.value:
            return cls.create_ibis_dataframe(cls, data_dict=data_dict, column_dict=column_dict)
        else:
            raise ValueError(f"Unsupported dataframe framework: {dataframe_framework}")
        
    @staticmethod
    def create_pandas_dataframe(
            cls,
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None) -> pd.DataFrame:


        cls.validate_mapping(data_dict=data_dict, column_dict=column_dict)


        if column_dict:

            df = pd.DataFrame(data_dict)
            df_cols = DataFrameUtils.get_column_names(df)
            new_colnames = {col: column_dict.get(col, col) for col in df_cols}

            return df.rename(columns=new_colnames, errors='ignore')
        
        else:
            return pd.DataFrame(data_dict)

    @staticmethod
    def create_polars_dataframe(
            cls,
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None) -> pl.DataFrame:
        
        cls.validate_mapping(data_dict=data_dict, column_dict=column_dict)


        if column_dict:


            df = pl.DataFrame(data_dict)
            df_cols = DataFrameUtils.get_column_names(df)
            new_colnames = {col: column_dict.get(col, col) for col in df_cols}

            return df.rename(mapping=new_colnames)

            # if isinstance(data_dict, dict):
            #     return pl.DataFrame(data=data_dict).rename(mapping=column_dict)

            # elif isinstance(data_dict, list):
            #     return pl.DataFrame(data=data_dict).rename(mapping=column_dict)
        else:
            return pl.DataFrame(data=data_dict)
            
    @staticmethod
    def create_pyarrow_table(
        data_dict: Dict[str, Union[Sequence, List]] | List[Dict[str, Any]],
        column_dict: Optional[Dict[str, str]] = None
    ) -> pa.Table:
        
        if isinstance(data_dict, dict):
            # Convert dict of lists/sequences to PyArrow table
            table = pa.Table.from_pydict(data_dict)
        elif isinstance(data_dict, list):
            # Convert list of dicts to PyArrow table
            table = pa.Table.from_pylist(data_dict)
        else:
            raise ValueError("Input must be a dictionary of sequences or a list of dictionaries")

        if column_dict:
            # Rename columns if column_dict is provided
            # new_names = [column_dict.get(col, col) for col in table.column_names]
            df_cols = DataFrameUtils.get_column_names(table)
            new_colnames = {col: column_dict.get(col, col) for col in df_cols}

            table = table.rename_columns(new_colnames)

        return table

    @staticmethod
    def create_ibis_dataframe(
            cls,
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None) -> ir.Table:
        
        df = DataFrameUtils.create_polars_dataframe(cls, data_dict=data_dict, column_dict=column_dict)
        columns = DataFrameUtils.get_column_names(df)

        return ibis.memtable(data=df, columns=columns)    

    #--------------------
    # Casting Dataframe to Dataframe

    @staticmethod
    def cast_dataframe_to_pandas(
        df_dataframe: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]) -> pd.DataFrame:
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

        if isinstance(df_dataframe, pa.Table):  
            return pl.DataFrame(data=df_dataframe).to_pandas()          
        elif isinstance(df_dataframe, pd.DataFrame):
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
        if isinstance(df_dataframe, pa.Table):  
            return pl.DataFrame(df_dataframe)        
        elif isinstance(df_dataframe, pl.DataFrame):
            return df_dataframe            
        elif isinstance(df_dataframe, pl.LazyFrame):
            return df_dataframe.collect()
        elif isinstance(df_dataframe, pd.DataFrame):
            return pl.from_pandas(data=df_dataframe)
        elif isinstance(df_dataframe, ir.Table):
            # return pl.from_pandas(data=df_dataframe.to_pandas())
            return df_dataframe.to_polars()
        else:
            raise TypeError("Unsupported dataframe type")


    @staticmethod
    def create_temp_table_ibis(
                          df_dataframe: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
                          tablename_prefix: Optional[str] = None,
                          ibis_backend: Optional[ibis.BaseBackend] = None,
                          overwrite: Optional[bool] = True,
            ) -> ir.Table:

        if not isinstance(df_dataframe, pl.DataFrame):
            df_dataframe = DataFrameUtils.cast_dataframe_to_polars(df_dataframe=df_dataframe)

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
        df_dataframe: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
        ibis_backend: Optional[ibis.BaseBackend|str] = None,
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

        if ibis_backend is None:
            ibis_backend = "polars"
        ibis.set_backend(backend=ibis_backend) 


        if isinstance(df_dataframe, pa.Table):
            return ibis.memtable(data=df_dataframe, 
                                 columns=DataFrameUtils.get_column_names(df_dataframe), 
                                 name=DataFrameUtils.generate_tablename(prefix=tablename_prefix))

        elif isinstance(df_dataframe, pl.DataFrame):
            df_dataframe = DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=df_dataframe)
            return ibis.memtable(data=df_dataframe, 
                                 columns=DataFrameUtils.get_column_names(df_dataframe), 
                                 name=DataFrameUtils.generate_tablename(prefix=tablename_prefix))
        elif isinstance(df_dataframe, pl.LazyFrame):
            df_dataframe = DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=df_dataframe)
            return ibis.memtable(data=df_dataframe, 
                                 columns=DataFrameUtils.get_column_names(df_dataframe), 
                                 name=DataFrameUtils.generate_tablename(prefix=tablename_prefix))
        elif isinstance(df_dataframe, pd.DataFrame):
            df_dataframe = DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=df_dataframe)
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
            return pl.DataFrame(data=df_dataframe).to_arrow()
        elif isinstance(df_dataframe, ir.Table):
            return df_dataframe.to_pyarrow()
        else:
            raise TypeError(f"Unsupported dataframe type: {type(df_dataframe)}")



    #--------------------
    # Casting Dataframe to Dictionary

    @staticmethod
    def cast_dataframe_to_dictonary_of_lists(df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]) -> Dict[Any,List[Any]]:
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

        if isinstance(df, pa.Table):  
            #Convert pyarrow to polars!
            df =  pl.DataFrame(df)


        #Now convert dataframes
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
        
    #--------------------
    # Casting Dataframe to List of Dicts

    @staticmethod
    def cast_dataframe_to_list_of_dictionaries(df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]) -> List[Dict[Any,Any]]:
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

        if isinstance(df, pa.Table):  
            #Convert pyarrow to polars!
            df =  pl.DataFrame(df)

        #Now convert dataframes
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



    #--------------------
    # Dataframe Column Operations

    @staticmethod            
    def drop(df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table], 
                     columns: List[str]|str
                     ) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]:



        if isinstance(columns, str):
            columns = [columns]

        existing_columns = DataFrameUtils.get_column_names(df)
        columns_to_drop = [col for col in columns if col in existing_columns]

        #if an empty list, just retrun the original dataframe!
        if not columns_to_drop or len(columns_to_drop) == 0:
            return df

        #TODO: Add a warning if columns doesn't exist


        if isinstance(df, pa.Table):  
            #Convert pyarrow to polars!
            df =  pl.DataFrame(df)
            return df.drop(columns_to_drop).to_arrow()

        elif isinstance(df, pd.DataFrame):
            return df.drop(columns=columns_to_drop, errors='ignore')
        elif isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            return df.drop(columns_to_drop)
        elif isinstance(df, (ir.Table)):
            return df.drop(*columns_to_drop)
        else:
            raise ValueError("Unsupported DataFrame type")


    @staticmethod            
    def select( df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table], 
                        columns: List[str]|str
                        ) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]:

        if isinstance(columns, str):
            columns = [columns]

        existing_columns = DataFrameUtils.get_column_names(df)
        columns_to_select = [col for col in columns if col in existing_columns]

        if not columns_to_select or len(columns_to_select) == 0:
            return df


        #TODO: Add a warning if columns doesn't exist

        if isinstance(df, pa.Table):  
            #Convert pyarrow to polars!
            df =  pl.DataFrame(df)
            df.select(columns_to_select).to_arrow()

        elif isinstance(df, pd.DataFrame):
            return df[columns_to_select]
        elif isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            return df.select(columns_to_select)
        elif isinstance(df, (ir.Table)):
            return df.select(columns_to_select)
        else:
            raise ValueError("Unsupported DataFrame type")
        
    # Column Metadata
    @staticmethod        
    def get_column_names(df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table] ) -> List:

        if isinstance(df, pa.Table):  
            #Convert pyarrow to polars!
            df =  pl.DataFrame(df)

        if isinstance(df, pd.DataFrame):
            return df.columns.tolist()
        elif isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            return df.columns
        elif isinstance(df, (ir.Table)):
            return df.columns
        else:
            raise ValueError("Unsupported DataFrame type")


    #--------------------
    # Dataframe Row Operations
        
    @staticmethod            
    def head( df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table], 
              n: int
            ) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]:

        if n < 0:
            raise ValueError("n must be greater than or equal to 0")

        if isinstance(df, pa.Table):  
            #Convert pyarrow to polars!
            df =  pl.DataFrame(df)
            return df.head(n=n).to_arrow()
        elif isinstance(df, pd.DataFrame):
            return df.head(n=n)
        elif isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            return df.head(n=n)
        elif isinstance(df, (ir.Table)):
            return df.head(n=n)
        else:
            raise ValueError("Unsupported DataFrame type")        
        
    @staticmethod            
    def count( df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table], 
                        ) -> int:

        if isinstance(df, pa.Table):  
            #Convert pyarrow to polars!
            df =  pl.DataFrame(df)

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

    @staticmethod
    def validate_mapping(data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]], column_dict: Optional[Dict[str, str]]=None):
        
        #Column Validation
        if column_dict:
            if None in column_dict.values():
                raise (ValueError("Column Aliases cannot be None"))
            
            columnTypes = []
            for i in list(column_dict.values()):
                columnTypes.append(type(i))
            
            if str not in columnTypes:
                raise (TypeError("Column Aliases have to be strings"))

            
        #Data Validation


            


