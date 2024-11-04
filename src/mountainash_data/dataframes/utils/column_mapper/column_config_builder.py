from typing import Any, Optional, Type, Union, Tuple, Dict

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
    