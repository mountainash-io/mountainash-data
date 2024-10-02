from abc import abstractmethod, ABC
from typing import Union, Any,  Dict, List, Optional
import uuid

import pandas as pd
import polars as pl
import pyarrow as pa
from pyarrow.interchange import from_dataframe

import ibis
import ibis.expr.types as ir
import ibis.expr.schema as ibis_schema

from .dataframe_functions import init_ibis_connection
from .filter import FilterNode
from ..base_dataframe import BaseDataFrame

class BaseDataFrameStrategy(ABC):

    @classmethod
    def validate_dataframe_input(cls,
                                df: Union[BaseDataFrame, pa.Table, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> bool:

        if not isinstance(df, (BaseDataFrame, pa.Table, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.RecordBatch)) and not cls._is_recordbatch(df=df):  
            raise TypeError(f"Unsupported dataframe type. Receieved: {type(df)}")
        
        return True


    @classmethod
    def supports_pyarrow_interchange(cls, 
                                     df: Union[pa.Table, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> bool:
        
        #See whether dataframes support the interchange method:
        # https://arrow.apache.org/docs/python/generated/pyarrow.interchange.from_dataframe.html


        #If the dataframe has the __dataframe__ method
        if hasattr(df, '__dataframe__'):
            return True
        return False

    @classmethod            
    def generate_tablename(cls, prefix: Optional[str] = None) -> str:

        if prefix:
            temp_tablename = f"{prefix}_{str(object=uuid.uuid4())}"
        else:   
            temp_tablename = str(object=uuid.uuid4())

        return temp_tablename 


    def create_temp_table_ibis(self,
                          df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
                          tablename_prefix: Optional[str] = None,
                          current_ibis_backend: Optional[ibis.BaseBackend] = None,
                          target_ibis_backend: Optional[ibis.BaseBackend] = None,
                          overwrite: Optional[bool] = True,
                          create_as_view: Optional[bool] = False
            ) -> ir.Table:


        self.validate_dataframe_input(df=df)

        if overwrite is None:
            overwrite = True

        tablename = self.generate_tablename(prefix=tablename_prefix)

        if target_ibis_backend is None:

            if self._is_recordbatch(df=df):
                df = self.cast_to_pyarrow_table(df=df)

            #This will use the default backend in-memory connection 
            new_table  = ibis.memtable(data=df, 
                                columns=self._get_column_names(df=df), 
                                name=tablename) 

        else:
            
            if current_ibis_backend is target_ibis_backend:

                if create_as_view and isinstance(df, ir.Table):
                    new_table =  target_ibis_backend.create_view(name = tablename, obj=df, overwrite=overwrite)
                    return new_table                
            else:
                #When moving between backends, we need materialise to move to the new backend
                if isinstance(df, ir.Table):
                    df = self.cast_to_pyarrow_table(df=df)

            if target_ibis_backend.supports_temporary_tables:   
                new_table =  target_ibis_backend.create_table(name = tablename, obj=df, overwrite=overwrite, temp=True)
            else:
                new_table =  target_ibis_backend.create_table(name = tablename, obj=df, overwrite=overwrite)


        return new_table

    @classmethod
    def _is_recordbatch(cls, df: Any) -> bool:

        if isinstance(df, list):
            return isinstance(df[0], pa.RecordBatch)
        else:
            return isinstance(df, pa.RecordBatch)


    #######################

    @abstractmethod
    def _cast_to_pandas(self, df: Any) -> pd.DataFrame:
        pass

    def cast_to_pandas(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> pd.DataFrame:
        self.validate_dataframe_input(df=df)
        return self._cast_to_pandas(df=df)


    @abstractmethod
    def _cast_to_polars(self, df: Any) -> pl.DataFrame:
        pass

    def cast_to_polars(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> pl.DataFrame:
        self.validate_dataframe_input(df=df)
        return self._cast_to_polars(df=df)


    @abstractmethod
    def _cast_to_pyarrow_table(self, df: Any) -> pa.Table:
        pass

    def cast_to_pyarrow_table(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> pa.Table:

        self.validate_dataframe_input(df=df)

        # if self.supports_pyarrow_interchange(df=df):
        #     return from_dataframe(df=df)
        # else:
        return self._cast_to_pyarrow_table(df=df)


    @abstractmethod
    def _cast_to_pyarrow_recordbatch(self, df: Any,  batchsize: int) -> List[pa.RecordBatch]:
        pass

    def cast_to_pyarrow_recordbatch(self, 
                                        df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
                                        batchsize: int = 1) -> List[pa.RecordBatch]:

        self.validate_dataframe_input(df=df)

        if self.supports_pyarrow_interchange(df=df):
            temp_df = self._cast_to_pyarrow_table(df=df)   
            return from_dataframe(df=temp_df).to_batches(max_chunksize=batchsize)
        else:
            return self._cast_to_pyarrow_recordbatch(df=df, batchsize=batchsize)


    def cast_to_ibis(self, 
                     df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],        
                     ibis_backend: Optional[ibis.BaseBackend|str] = None,
                     tablename_prefix: Optional[str] = None) -> ir.Table:
        
        self.validate_dataframe_input(df=df)

        if ibis_backend is None:
            ibis_backend = "polars"

        if isinstance(ibis_backend, str):
            ibis_backend = init_ibis_connection(ibis_backend)

        # table_schema = self.get_table_schema(df=df)
        tablename = self.generate_tablename(prefix=tablename_prefix)

        if isinstance(df, (pd.DataFrame)) or self._is_recordbatch(df=df):
            df = self.cast_to_pyarrow_table(df=df)

        if ibis_backend.supports_temporary_tables:   
            new_table =  ibis_backend.create_table(name = tablename, obj=df, #schema=table_schema,
                                                    overwrite=True, temp=True)
        else:
            new_table =  ibis_backend.create_table(name = tablename, obj=df, #schema=table_schema, 
                                                   overwrite=True)
        return new_table






    @abstractmethod
    def _cast_to_dictonary_of_lists(self, df: Any) -> Dict[Any,List[Any]]:
        pass

    def cast_to_dictonary_of_lists(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> Dict[Any,List[Any]]:
        self.validate_dataframe_input(df=df)
        return self._cast_to_dictonary_of_lists(df=df)        


    @abstractmethod
    def _cast_to_dictonary_of_series(self, df: Any) -> Dict[str,pl.Series]:
        pass

    def cast_to_dictonary_of_series(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> Dict[str,pl.Series]:
        self.validate_dataframe_input(df=df)
        return self._cast_to_dictonary_of_series(df=df)        


    @abstractmethod
    def _cast_to_list_of_dictionaries(self, df: Any) -> List[Dict[Any,Any]]:
        pass

    def cast_to_list_of_dictionaries(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> List[Dict[Any,Any]]:
        self.validate_dataframe_input(df=df)
        return self._cast_to_list_of_dictionaries(df=df)        


    @abstractmethod
    def _get_column_names(self, df: Any) -> List[str]:
        pass

    def get_column_names(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> List[str]:
        self.validate_dataframe_input(df=df)
        return self._get_column_names(df=df)  

    #This one is implemented in reverse in the subclasses
    @abstractmethod
    def get_table_schema(self, df) -> ibis_schema.Schema:
        pass

    def _get_table_schema(self, df) -> ibis_schema.Schema:

        df_head = self.head(df=df, n=0)
        df_pl: pl.DataFrame = self._cast_to_polars(df=df_head)
        native_schema = df_pl.schema
        return ibis_schema.Schema.from_polars(polars_schema=native_schema)


    @abstractmethod
    def _drop(self, 
              df: Any, 
              columns: List[str]) -> Union[pa.Table, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.RecordBatch, List[pa.RecordBatch]]:
        pass


    def drop(self, 
             df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch], 
             columns: List[str]|str) -> Union[pa.Table, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.RecordBatch, List[pa.RecordBatch]]:
    
        self.validate_dataframe_input(df=df)

        if isinstance(columns, str):
            columns = [columns]

        existing_columns = self._get_column_names(df=df)
        columns_to_drop = [col for col in columns if col in existing_columns]

        #if an empty list, just retrun the original dataframe!
        if not columns_to_drop or len(columns_to_drop) == 0:
            return df
        
        return self._drop(df=df, columns=columns_to_drop)


      

    @abstractmethod
    def _select(self,
                df: Any, 
                columns: List[str]) -> Union[pa.Table, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.RecordBatch, List[pa.RecordBatch]]:
        pass


    def select(self,
               df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch], 
               columns: List[str]|str) -> Union[pa.Table, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.RecordBatch, List[pa.RecordBatch]]:
        
        self.validate_dataframe_input(df=df)

        if isinstance(columns, str):
            columns = [columns]

        existing_columns = self._get_column_names(df=df)
        columns_to_select = [col for col in columns if col in existing_columns]

        if not columns_to_select or len(columns_to_select) == 0:
            return df        

        return self._select(df=df, columns=columns_to_select)




    @abstractmethod
    def _head(self, 
              df: Any, 
              n: int) -> Union[pa.Table, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.RecordBatch, List[pa.RecordBatch]]:
        pass

    def head(self, 
             df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
             n: int) -> Union[pa.Table, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.RecordBatch, List[pa.RecordBatch]]:

        if n < 0:
            raise ValueError("n must be greater than or equal to 0")

        self.validate_dataframe_input(df=df)
        return self._head(df=df, n=n)

    @abstractmethod
    def _count(self, df: Any) -> int:
        pass

    def count(self, 
              df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> int:
        self.validate_dataframe_input(df=df)
        return self._count(df=df)


    @abstractmethod
    def _filter(self, df: Any, condition: FilterNode) -> Any:
        pass

    def filter(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], condition: FilterNode) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]:
        self.validate_dataframe_input(df)
        return self._filter(df, condition)
    

    @abstractmethod
    def _split_in_batches(self, df: Any, batch_size: int) -> List[Any]:
        pass

    def split_in_batches(self, 
            df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
            batch_size: int
        ) -> List[Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]]:
        
        self.validate_dataframe_input(df=df)
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")
        
        return self._split_in_batches(df, batch_size)    