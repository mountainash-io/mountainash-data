import pandas as pd
import polars as pl
import pyarrow as pa
import ibis
from typing import Union, Any,  Dict, List, Optional, Sequence
from functools import lru_cache

import uuid
from mountainash_constants import CONST_DATAFRAME_FRAMEWORK
# from mountainash_utils_dataclasses import DataclassUtils
import ibis.expr.types as ir
import collections
import ibis.expr.schema as ibis_schema

# from multipledispatch import dispatch
# from pyarrow import Table as irTable  # assuming you are using pyarrow's Table for ir.Table

@lru_cache(maxsize=None)
def init_ibis_connection(ibis_schema: Optional[str] = None) -> ibis.BaseBackend:
    return ibis.connect(resource=f"{ibis_schema}://")


class DataFrameUtils:


    @classmethod
    def validate_column_mapping(cls,
                                column_dict: Optional[Dict[str, str]]=None) -> bool:
        """
        Validates the column mapping for a given data dictionary and column dictionary.

        Args:
            data_dict (Dict[str, Union[Sequence,List]] | List[Dict[str, Any]]): The data dictionary to validate.
            column_dict (Optional[Dict[str, str]]): The column dictionary to validate. Defaults to None.

        Raises:
            ValueError: If source and target column names are not specified or if duplicate column names are found.
            TypeError: If column names are not strings.
        """

        # Column Validation
        if column_dict is not None:

            try:
                # Cannot have duplicate column names in the keys or the values
                if len(column_dict) != len(set(column_dict.values())):
                    duplicate_values = [item for item, count in collections.Counter(column_dict.values()).items() if count > 1]    
                    raise ValueError(f"Source and target column names must be unique. Duplicate column names: {duplicate_values}")

                if len(column_dict) != len(set(column_dict.keys())):
                    duplicate_values = [item for item, count in collections.Counter(column_dict.keys()).items() if count > 1]    
                    raise ValueError(f"Source and target column names must be unique. Duplicate column names: {duplicate_values}")

                if None in column_dict.values():
                    raise ValueError("Source and target column names must be specified")

                columnTypes = [type(colname) for colname in column_dict.values()]
                if any(coltype != str for coltype in columnTypes):
                    raise ValueError("Column names must be strings")
                
            except ValueError as e:
                print(f"Error validating column mapping: {e}")
                return False
            except TypeError as e:
                print(f"Error validating column mapping: {e}")
                return False
        return True

    #--------------------
    # Casting Dictionary to Dataframe

    @classmethod
    def create_dataframe(
            cls,
            dataframe_framework: str, 
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None,
            column_types: Optional[Dict[str, str]]=None
         ) -> Union[pd.DataFrame, pl.DataFrame, ir.Table]:

        if not dataframe_framework:
            raise ValueError("dataframe_framework must be specified")
        

        if dataframe_framework == CONST_DATAFRAME_FRAMEWORK.PANDAS.value:
            return cls.create_pandas_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.POLARS.value:
            return cls.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        elif dataframe_framework == CONST_DATAFRAME_FRAMEWORK.IBIS.value:
            return cls.create_ibis_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        else:
            raise ValueError(f"Unsupported dataframe framework: {dataframe_framework}")
        
    @classmethod
    def create_pandas_dataframe(
            cls,
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None,
            column_types: Optional[Dict[str, str]]=None) -> pd.DataFrame:

        pl_df = cls.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        return cls.cast_dataframe_to_pandas(df_dataframe=pl_df)

        # if column_dict and cls.validate_column_mapping(column_dict=column_dict):

        #     df = pd.DataFrame(data_dict)
        #     df_cols = DataFrameUtils.get_column_names(df)
        #     new_colnames = {col: column_dict.get(col, col) for col in df_cols}

        #     return df.rename(columns=new_colnames, errors='ignore')
        
        # else:
        #     return pd.DataFrame(data_dict)

    @classmethod
    def create_polars_dataframe(
            cls,
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]]=None,
            column_types: Optional[Dict[str, str]]=None) -> pl.DataFrame:

        if column_dict and cls.validate_column_mapping(column_dict=column_dict):

            df = pl.DataFrame(data_dict, strict=False)
            df_cols = DataFrameUtils.get_column_names(df)
            new_colnames = {col: column_dict.get(col, col) for col in df_cols}

            return df.rename(mapping=new_colnames)

        else:
            return pl.DataFrame(data=data_dict, strict=False)
            
    @classmethod
    def create_pyarrow_table(
        cls,
        data_dict: Dict[str, Union[Sequence, List]] | List[Dict[str, Any]],
        column_dict: Optional[Dict[str, str]] = None,
        column_types: Optional[Dict[str, str]] = None
    ) -> pa.Table:
        
        pl_df = cls.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)
        return cls.cast_dataframe_to_pyarrow(df_dataframe=pl_df)


        # if isinstance(data_dict, dict):
        #     # Convert dict of lists/sequences to PyArrow table
        #     table = pa.Table.from_pydict(data_dict)
        # elif isinstance(data_dict, list):
        #     # Convert list of dicts to PyArrow table
        #     table = pa.Table.from_pylist(data_dict)
        # else:
        #     raise ValueError("Input must be a dictionary of sequences or a list of dictionaries")

        # if column_dict and cls.validate_column_mapping(column_dict=column_dict):

        #     # Rename columns if column_dict is provided
        #     # new_names = [column_dict.get(col, col) for col in table.column_names]
        #     df_cols = cls.get_column_names(table)
        #     new_colnames = {col: column_dict.get(col, col) for col in df_cols}

        #     table = table.rename_columns(new_colnames)

        # return table

    @classmethod
    def create_ibis_dataframe(
            cls,
            data_dict: Dict[str, Union[Sequence,List]] | List[Dict[str, Any]],
            column_dict: Optional[Dict[str, str]] = None,
            column_types: Optional[Dict[str, str]]=None ) -> ir.Table:
        
        pl_df = cls.create_polars_dataframe(data_dict=data_dict, column_dict=column_dict, column_types=column_types)

        # columns = DataFrameUtils.get_column_names(df)
        # table_schema = DataFrameUtils.get_table_schema(df)

        return DataFrameUtils.cast_dataframe_to_ibis(df_dataframe=pl_df)

        # return ibis.memtable(data=df, columns=columns, schema=table_schema)    

    #--------------------
    # Casting Dataframe to Dataframe

    @staticmethod
    def cast_dataframe_to_pandas(
        df_dataframe: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table] ) -> pd.DataFrame:
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
        df_dataframe: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table] ) -> pl.DataFrame:
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
            return pl.DataFrame(data=df_dataframe)        
        elif isinstance(df_dataframe, pl.DataFrame):
            return df_dataframe            
        elif isinstance(df_dataframe, pl.LazyFrame):
            return df_dataframe.collect()
        elif isinstance(df_dataframe, pd.DataFrame):
            return pl.from_pandas(data=df_dataframe)
        elif isinstance(df_dataframe, ir.Table):
            return df_dataframe.to_polars()
        else:
            raise TypeError("Unsupported dataframe type")


    @staticmethod
    def create_temp_table_ibis(
                          df_dataframe: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table],
                          tablename_prefix: Optional[str] = None,
                          current_ibis_backend: Optional[ibis.BaseBackend] = None,
                          target_ibis_backend: Optional[ibis.BaseBackend] = None,
                          overwrite: Optional[bool] = True,
                          create_as_view: Optional[bool] = False
            ) -> ir.Table:


        if overwrite is None:
            overwrite = True
        

        tablename = DataFrameUtils.generate_tablename(prefix=tablename_prefix)

        if target_ibis_backend is None:

            #This will use the default backend in-memory connection 
            new_table  = ibis.memtable(data=df_dataframe, 
                                columns=DataFrameUtils.get_column_names(df_dataframe), 
                                # schema=table_schema, 
                                name=tablename) 
            
            # print(f"A create_temp_table_ibis: type:{type(df_dataframe)} - {ibis_backend.name}  to:{DataFrameUtils.get_table_schema(new_table)} ")

        else:
            
            if current_ibis_backend is target_ibis_backend:

                if create_as_view:   
                    new_table =  target_ibis_backend.create_view(name = tablename, obj=df_dataframe, overwrite=overwrite)
                    return new_table                
            else:
                #When moving between backends, we need materialise to move to the new backend
                if isinstance(df_dataframe, ir.Table):
                    df_dataframe = DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=df_dataframe)

            if target_ibis_backend.supports_temporary_tables:   
                new_table =  target_ibis_backend.create_table(name = tablename, obj=df_dataframe, overwrite=overwrite, temp=True)
            else:
                new_table =  target_ibis_backend.create_table(name = tablename, obj=df_dataframe, overwrite=overwrite)


            # print(f"B create_temp_table_ibis: type:{type(df_dataframe)} - {ibis_backend.name}: {DataFrameUtils.get_table_schema(new_table)} ")


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

        df_dataframe

        if ibis_backend is None:
            ibis_backend = "polars"

        if isinstance(ibis_backend, str):
            ibis_backend = init_ibis_connection(ibis_backend)

        #Now we should have a backend instance
        #ibis.set_backend(backend=ibis_backend) 

        # columns = DataFrameUtils.get_column_names(df_dataframe)
        table_schema = DataFrameUtils.get_table_schema(df_dataframe)
        tablename = DataFrameUtils.generate_tablename(prefix=tablename_prefix)

        # if isinstance(df_dataframe, (pl.DataFrame, pl.LazyFrame, pd.DataFrame)):
        if isinstance(df_dataframe, (pd.DataFrame)):
            df_dataframe = DataFrameUtils.cast_dataframe_to_arrow(df_dataframe=df_dataframe)

        if ibis_backend.supports_temporary_tables:   
            new_table =  ibis_backend.create_table(name = tablename, obj=df_dataframe, schema=table_schema, overwrite=True, temp=True)
        else:
            new_table =  ibis_backend.create_table(name = tablename, obj=df_dataframe, schema=table_schema, overwrite=True)
        return new_table

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

        if isinstance(df, (pa.Table, pd.DataFrame)):  
            #Convert pyarrow to polars!
            df =  pl.DataFrame(df)
            
        if isinstance(df, pl.DataFrame):
            return df.to_dict(as_series=False) 
            
        elif isinstance(df, pl.LazyFrame):
            return df.collect().to_dict(as_series=False) 

        elif isinstance(df, ir.Table):
            return df.to_polars().to_dict(as_series=False)

        else:
            raise TypeError("Unsupported dataframe type")
        

    #--------------------
    # Casting Dataframe to Dictionary

    @staticmethod
    def cast_dataframe_to_dictonary_of_series(df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table]) -> Dict[str,pl.Series]:
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

        if isinstance(df, (pa.Table, pd.DataFrame)):  
            #Convert pyarrow to polars!
            df =  pl.DataFrame(df)


        #Now convert dataframes
        if isinstance(df, pl.DataFrame):
            return df.to_dict(as_series=True) 
            
        elif isinstance(df, pl.LazyFrame):
            return df.collect().to_dict(as_series=True) 

        elif isinstance(df, ir.Table):
            return df.to_polars().to_dict(as_series=True)

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
            return df.to_polars().to_dicts()
        
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


    # Column Metadata
    @staticmethod        
    def get_table_schema(df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table] ) -> ibis_schema.Schema:

        if isinstance(df, (pa.Table, pd.DataFrame, pl.DataFrame, pl.LazyFrame)):  
            df = DataFrameUtils.head(df=df, n=0)
            df = DataFrameUtils.cast_dataframe_to_polars(df_dataframe=df)
            native_schema = df.schema
            return ibis_schema.Schema.from_polars(polars_schema=native_schema)
        elif isinstance(df, (ir.Table)):
            return df.schema()
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

            


            


