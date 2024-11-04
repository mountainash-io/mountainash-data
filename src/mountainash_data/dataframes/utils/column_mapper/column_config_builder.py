from typing import Any, Optional, Type, Union, Tuple, Dict
import collections

from .constants import DataType, NullStrategy
from .column_config import ColumnConfig, TypeConfig

class ColumnConfigBuilder:
    """Builder for creating column configurations."""
    
    def build_mapping_config(self,
                             column_mapping: Dict[str, str|Dict[str,str]]) -> Dict[str, ColumnConfig]:
        """
        Build a column configuration from a column mapping.
        
        Args:
            column_mapping: Column mapping
        
        Returns:
            Dict[str, ColumnConfig]: Column configuration
        """
        column_config = {}

        for source_name, target_name in column_mapping.items():

            if isinstance(target_name, dict):

                target_name = target_name.get("target_name", source_name)
                data_type = target_name.get("data_type", None)
                null_strategy = target_name.get("null_strategy", NullStrategy.ALLOW)
                default_value = target_name.get("default_value", None)
            else:
                #We have a simple mapping of string to string
                target_name = target_name if target_name else source_name
                data_type = None
                null_strategy = NullStrategy.ALLOW
                default_value = None

            
            column_config[source_name] = self.create_config(
                source_name=source_name,
                target_name=target_name,
                data_type=data_type,
                null_strategy=null_strategy,
                default_value=default_value
            )
            
        return column_config

    @classmethod
    def create_config(cls,
                     source_name: str,
                     target_name: str,
                     data_type: Optional[Union[DataType, Type]],
                     null_strategy: Optional[NullStrategy] = NullStrategy.ALLOW,
                     default_value: Optional[Any] = None
                     
                     ) -> Tuple[str, ColumnConfig]:
        """
        Create a column configuration.
        
        Args:
            source_name: Original column name
            target_name: New column name
            data_type: Target data type
            null_strategy: How to handle null values
            default_value: Default value for null replacement
            
        Returns:
            Tuple[str, ColumnConfig]: Source name and column configuration
        """
        if isinstance(data_type, type):
            data_type = DataType.from_python_type(data_type)
            
        type_config = TypeConfig(
            data_type=data_type,
            null_strategy=null_strategy,
            default_value=default_value
        )
        
        return source_name, ColumnConfig(
            target_name=target_name,
            type_config=type_config
        )    
    
#     @classmethod
#     def validate_column_mapping(cls,
#                                 column_mapping: Optional[Dict[str, str|Dict[str,str]]]=None) -> bool:
#         """
#         Validates the column mapping for a given data dictionary and column dictionary.

#         Args:
# \            column_dict (Optional[Dict[str, str|Dict[str,str]]]]): The column dictionary to validate. Defaults to None.

#         Raises:
#             ValueError: If source and target column names are not specified or if duplicate column names are found.
#             TypeError: If column names are not strings.
#         """

#         # Column Validation
#         if column_mapping is not None:

#             try:
#                 # Initial tests

#                 # Cannot have duplicate column names in the keys or the values
#                 if len(column_mapping) != len(set(column_mapping.values())):

#                     duplicate_values = [item for item, count in collections.Counter(column_mapping.values()).items() if count > 1]    
#                     raise ValueError(f"Source column names must be unique. Duplicate column names: {duplicate_values}")

#                 # if len(column_mapping) != len(set(column_mapping.keys())):
#                 #     duplicate_values = [item for item, count in collections.Counter(column_mapping.keys()).items() if count > 1]    
#                 #     raise ValueError(f"Source and target column names must be unique. Duplicate column names: {duplicate_values}")

#                 # if None in column_mapping.values():
#                 #     raise ValueError("Source and target column names must be specified")

#                 column_types = [type(colname) for colname in column_mapping.values()]

#                 if any(coltype != str for coltype in column_types):
#                     raise ValueError("Column names must be strings")
                
#             except ValueError as e:
#                 print(f"Error validating column mapping: {e}")
#                 return False
            
#             except TypeError as e:
#                 print(f"Error validating column mapping: {e}")
#                 return False
            
#         return True    