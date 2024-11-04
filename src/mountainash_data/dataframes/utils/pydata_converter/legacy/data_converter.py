from dataclasses import is_dataclass, fields
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Type, Union, Sequence, Set
import inspect

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis.expr.types as ir

from .. import BaseDataFrameStrategy, PandasDataFrameUtils, PolarsDataFrameUtils, PolarsLazyFrameUtils, PyArrowTableUtils, IbisDataFrameUtils, PyArrowRecordBatchUtils

# from .filter import FilterCondition, FilterNode
from ...base_dataframe import BaseDataFrame



class InputType(Enum):
    """Enumeration of supported input data structure types."""
    LIST_OF_DICTS = auto()
    DICT_OF_LISTS = auto()
    DATACLASS = auto()
    DATAFRAME = auto()  # For existing DataFrame types
    UNKNOWN = auto()

class DataConverter:
    """Adapter for converting various data structures to DataFrame format."""


    ################################
    # Conversion Methods

    @classmethod
    def convert(cls, data: Any) -> pl.DataFrame:
        """
        Convert input data to a Polars DataFrame.
        
        Args:
            data: Input data of any supported type
            
        Returns:
            pl.DataFrame: Converted data
            
        Raises:
            ValueError: If conversion fails
        """
        # Validate input first
        cls.validate(data)

        input_type = cls.detect_input_type(data)

        if input_type == InputType.LIST_OF_DICTS:
            return cls._convert_list_of_dicts(data)
        
        elif input_type == InputType.DICT_OF_LISTS:
            return cls._convert_dict_of_lists(data)
        
        elif input_type == InputType.DATACLASS:
            return cls._convert_dataclass(data)
        
        elif input_type == InputType.DATAFRAME:
            # For existing DataFrame types, return as is
            if isinstance(data, pl.DataFrame):
                return data
            else:
                return cls.cast_dataframe_to_polars(data)
       
        raise ValueError(f"Unable to convert data of type: {type(data)}")
    


    @classmethod
    def _convert_list_of_dicts(cls, data: List[Dict[str, Any]]) -> pl.DataFrame:
        """Convert list of dictionaries to Polars DataFrame."""
        return pl.DataFrame(data)

    @classmethod
    def _convert_dict_of_lists(cls, data: Dict[str, List[Any]]) -> pl.DataFrame:
        """Convert dictionary of lists to Polars DataFrame."""
        return pl.DataFrame(data)

    @classmethod
    def _convert_dataclass(cls, data: Any) -> pl.DataFrame:

        """Convert dataclass or list of dataclasses to Polars DataFrame."""
        if isinstance(data, list):
            # Convert list of dataclasses to list of dicts
            data_dicts = [cls._dataclass_to_dict(item) for item in data]
            return pl.DataFrame(data_dicts)
        else:
            # Convert single dataclass to dict
            data_dict = cls._dataclass_to_dict(data)
            return pl.DataFrame([data_dict])

    @classmethod
    def _dataclass_to_dict(cls, obj: Any, fields) -> Dict[str, Any]:
        """Convert a dataclass instance to a dictionary."""
        return {field.name: getattr(obj, field.name) for field in fields(obj)}



    @classmethod
    def detect_input_type(cls, data: Any) -> InputType:
        """
        Detect the type of input data structure.
        
        Args:
            data: Input data of any supported type
            
        Returns:
            InputType: Enumeration indicating the detected type
        """
        if data is None:
            return InputType.UNKNOWN

        # Check for DataFrame types first
        if isinstance(data, (pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table)):
            return InputType.DATAFRAME

        # Check for list of dictionaries
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            return InputType.LIST_OF_DICTS

        # Check for dictionary of lists
        if isinstance(data, dict) and all(isinstance(v, (list, tuple, Sequence)) for v in data.values()):
            return InputType.DICT_OF_LISTS

        # Check for dataclass
        if is_dataclass(data) or (isinstance(data, list) and len(data) > 0 and is_dataclass(data[0])):
            return InputType.DATACLASS

        return InputType.UNKNOWN

    @classmethod
    def _get_available_fields(cls, data: Any) -> Set[str]:
        """
        Get available field names from the input data structure.
        
        Args:
            data: Input data of any supported type
            
        Returns:
            Set[str]: Set of available field names
        """
        if isinstance(data, list) and data:
            if isinstance(data[0], dict):
                return set(data[0].keys())
            elif is_dataclass(data[0]):
                return {field.name for field in fields(data[0])}
        elif isinstance(data, dict):
            return set(data.keys())
        elif is_dataclass(data):
            return {field.name for field in fields(data)}
        return set()

    ################################
    # Validation Methods

    # @classmethod
    # def _validate_column_mapping(cls, column_dict: Dict[str, str]) -> bool:
    #     """
    #     Validates the column mapping dictionary.
        
    #     Args:
    #         column_dict: Dictionary mapping source column names to target column names
            
    #     Returns:
    #         bool: True if valid
            
    #     Raises:
    #         ValueError: If mapping is invalid
    #     """
    #     if not isinstance(column_dict, dict):
    #         raise ValueError("Column mapping must be a dictionary")

    #     # Check for duplicate target names
    #     target_names = list(column_dict.values())
    #     if len(target_names) != len(set(target_names)):
    #         duplicates = [name for name in target_names if target_names.count(name) > 1]
    #         raise ValueError(f"Duplicate target column names found: {set(duplicates)}")

    #     return True



    # @classmethod
    # def _validate_column_existence(cls, data: Any, column_dict: Dict[str, str]) -> None:
    #     """
    #     Validate that all source columns exist in the data.
        
    #     Args:
    #         data: Input data
    #         column_dict: Column mapping dictionary
            
    #     Raises:
    #         ValueError: If any source column doesn't exist
    #     """
    #     available_fields = cls._get_available_fields(data)
    #     missing_fields = set(column_dict.keys()) - available_fields
        
    #     if missing_fields:
    #         raise ValueError(f"Source columns not found in data: {missing_fields}")



    # @classmethod
    # def validate_list_of_dicts(cls, 
    #                           data: List[Dict[str, Any]], 
    #                           column_dict: Optional[Dict[str, str]] = None) -> bool:
    #     """
    #     Validate that a list of dictionaries has consistent keys.
        
    #     Args:
    #         data: List of dictionaries to validate
    #         column_dict: Optional column mapping dictionary
            
    #     Returns:
    #         bool: True if valid
            
    #     Raises:
    #         ValueError: If validation fails
    #     """
    #     if not data:
    #         return True

    #     if column_dict:
    #         cls._validate_column_mapping(column_dict)
    #         cls._validate_column_existence(data, column_dict)
    #         required_keys = set(column_dict.keys())
    #     else:
    #         required_keys = set(data[0].keys())

    #     # Check all dictionaries have the required keys
    #     for idx, d in enumerate(data[1:], start=1):
    #         missing_keys = required_keys - set(d.keys())
    #         if missing_keys:
    #             raise ValueError(f"Missing required keys at index {idx}: {missing_keys}")

    #     return True

    # @classmethod
    # def validate_dict_of_lists(cls, 
    #                           data: Dict[str, List[Any]], 
    #                           column_dict: Optional[Dict[str, str]] = None) -> bool:
    #     """
    #     Validate that a dictionary of lists has consistent lengths.
        
    #     Args:
    #         data: Dictionary of lists to validate
    #         column_dict: Optional column mapping dictionary
            
    #     Returns:
    #         bool: True if valid
            
    #     Raises:
    #         ValueError: If validation fails
    #     """
    #     if not data:
    #         return True

    #     if column_dict:
    #         cls._validate_column_mapping(column_dict)
    #         cls._validate_column_existence(data, column_dict)
    #         keys_to_check = set(column_dict.keys())
    #     else:
    #         keys_to_check = set(data.keys())

    #     # Get length of first list
    #     first_key = next(iter(keys_to_check))
    #     reference_length = len(data[first_key])

    #     # Check all required lists have the same length
    #     for key in keys_to_check:
    #         if len(data[key]) != reference_length:
    #             raise ValueError(
    #                 f"Inconsistent list lengths. Key '{key}' has length {len(data[key])}, "
    #                 f"expected {reference_length}"
    #             )

    #     return True

    # @classmethod
    # def validate_dataclass(cls, 
    #                       data: Any, 
    #                       column_dict: Optional[Dict[str, str]] = None) -> bool:
    #     """
    #     Validate that data is a dataclass or list of dataclasses.
        
    #     Args:
    #         data: Data to validate
    #         column_dict: Optional column mapping dictionary
            
    #     Returns:
    #         bool: True if valid
            
    #     Raises:
    #         ValueError: If validation fails
    #     """
    #     if isinstance(data, list):
    #         if not data:
    #             return True
            
    #         if not is_dataclass(data[0]):
    #             raise ValueError(f"First item is not a dataclass: {type(data[0])}")

    #         reference_type = type(data[0])
            
    #         # Check all items are the same dataclass type
    #         for idx, item in enumerate(data[1:], start=1):
    #             if not isinstance(item, reference_type):
    #                 raise ValueError(
    #                     f"Inconsistent types at index {idx}. "
    #                     f"Expected {reference_type}, got {type(item)}"
    #                 )
    #     else:
    #         if not is_dataclass(data):
    #             raise ValueError(f"Input is not a dataclass: {type(data)}")

    #     if column_dict:
    #         cls._validate_column_mapping(column_dict)
    #         cls._validate_column_existence(data, column_dict)

    #     return True

    # @classmethod
    # def validate(cls, 
    #             data: Any, 
    #             column_dict: Optional[Dict[str, str]] = None) -> bool:
    #     """
    #     Validate input data based on its detected type.
        
    #     Args:
    #         data: Input data to validate
    #         column_dict: Optional column mapping dictionary
            
    #     Returns:
    #         bool: True if valid
            
    #     Raises:
    #         ValueError: If validation fails
    #     """
    #     input_type = cls.detect_input_type(data)

    #     if input_type == InputType.UNKNOWN:
    #         raise ValueError(f"Unsupported input type: {type(data)}")

    #     if column_dict:
    #         cls._validate_column_mapping(column_dict)

    #     if input_type == InputType.LIST_OF_DICTS:
    #         return cls.validate_list_of_dicts(data, column_dict)
    #     elif input_type == InputType.DICT_OF_LISTS:
    #         return cls.validate_dict_of_lists(data, column_dict)
    #     elif input_type == InputType.DATACLASS:
    #         return cls.validate_dataclass(data, column_dict)
        
    #     return True





    ################################
    # Strategy Based Methods


    @classmethod
    def _is_recordbatch(cls, df: Any) -> bool:

        if isinstance(df, list) and len(df) > 0:
            return isinstance(df[0], pa.RecordBatch)
        else:
            return isinstance(df, pa.RecordBatch)

    @classmethod
    def validate_dataframe_supported(cls, 
              df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> Optional[bool]:
        
        if df is None:
            return df
        
        strategy = cls._get_strategy(df)
        return strategy.validate_dataframe_input(df)

    @classmethod
    def _get_strategy(cls, 
                      df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> BaseDataFrameStrategy:
        
        if isinstance(df, BaseDataFrame):
            return IbisDataFrameUtils()
        elif isinstance(df, ir.Table):
            return IbisDataFrameUtils()
        elif cls._is_recordbatch(df=df):
            return PyArrowRecordBatchUtils()
        elif isinstance(df, pa.Table):
            return PyArrowTableUtils()
        elif isinstance(df, pl.DataFrame ):
            return PolarsDataFrameUtils()
        elif isinstance(df, pl.LazyFrame ):
            return PolarsLazyFrameUtils()
        elif isinstance(df, pd.DataFrame):
            return PandasDataFrameUtils()
        else:
            raise TypeError(f"Unsupported dataframe type. Received {type(df)}")


    @classmethod
    def cast_dataframe_to_pandas(cls, df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> pd.DataFrame:

        if df is None:
            return df

        strategy = cls._get_strategy(df=df)
        return strategy.cast_to_pandas(df=df)

    @classmethod
    def cast_dataframe_to_polars(cls, df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> pl.DataFrame:

        if df is None:
            return df
        
        strategy = cls._get_strategy(df=df)
        return strategy.cast_to_polars(df=df)

    @classmethod
    def cast_dataframe_to_arrow(cls, 
                                df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]]) -> pa.Table:
        
        if df is None:
            return df
        
        strategy = cls._get_strategy(df=df)
        return strategy._cast_to_pyarrow_table(df)

    @classmethod
    def cast_dataframe_to_pyarrow_recordbatch(cls, 
                                              df: Union[BaseDataFrame, pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
                                              batchsize: int = 1) -> List[pa.RecordBatch]:
        
        if df is None:
            return df
        
        strategy = cls._get_strategy(df=df)
        return strategy.cast_to_pyarrow_recordbatch(df=df, batchsize=batchsize)




    @classmethod
    def cast_dataframe_to_ibis(cls, 
                               df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame, ir.Table, pa.Table, pa.RecordBatch, List[pa.RecordBatch]], 
                               ibis_backend=None, 
                               tablename_prefix=None) -> ir.Table:
        
        if df is None:
            return df
        
        strategy = cls._get_strategy(df=df)
        return strategy.cast_to_ibis(df=df, ibis_backend=ibis_backend, tablename_prefix=tablename_prefix)
