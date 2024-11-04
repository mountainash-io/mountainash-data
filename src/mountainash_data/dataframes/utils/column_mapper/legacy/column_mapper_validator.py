from dataclasses import is_dataclass, fields
from typing import Any, Dict, List, Set, Type, Optional, Sequence
import polars as pl

class ColumnMappingValidator:
    """Validates column mapping configurations and operations."""
    
    @classmethod
    def validate_mapping_structure(cls, mapping: Dict[str, str]) -> None:
        """
        Validate the basic structure of a column mapping.
        
        Args:
            mapping: Column mapping dictionary
            
        Raises:
            ValueError: If mapping structure is invalid
        """
        if not isinstance(mapping, dict):
            raise ValueError("Column mapping must be a dictionary")

        if not all(isinstance(k, str) and isinstance(v, str) 
                  for k, v in mapping.items()):
            raise ValueError("All mapping keys and values must be strings")

    @classmethod
    def validate_no_duplicate_targets(cls, mapping: Dict[str, str]) -> None:
        """
        Validate that there are no duplicate target column names.
        
        Args:
            mapping: Column mapping dictionary
            
        Raises:
            ValueError: If duplicate target names are found
        """
        target_names = list(mapping.values())
        duplicates = {name for name in target_names if target_names.count(name) > 1}
        if duplicates:
            raise ValueError(f"Duplicate target column names found: {duplicates}")

    @classmethod
    def validate_source_columns_exist(cls, 
                                    existing_columns: List[str], 
                                    mapping: Dict[str, str]) -> None:
        """
        Validate that all mapped source columns exist in the data.
        
        Args:
            existing_columns: List of existing column names
            mapping: Column mapping dictionary
            
        Raises:
            ValueError: If mapped columns don't exist
        """
        missing_columns = set(mapping.keys()) - set(existing_columns)
        if missing_columns:
            raise ValueError(f"Mapped columns not found in data: {missing_columns}")

    @classmethod
    def validate_mapping(cls, 
                        existing_columns: List[str], 
                        mapping: Dict[str, str]) -> None:
        """
        Perform all column mapping validations.
        
        Args:
            existing_columns: List of existing column names
            mapping: Column mapping dictionary
            
        Raises:
            ValueError: If any validation fails
        """
        cls.validate_mapping_structure(mapping)
        cls.validate_no_duplicate_targets(mapping)
        cls.validate_source_columns_exist(existing_columns, mapping)

