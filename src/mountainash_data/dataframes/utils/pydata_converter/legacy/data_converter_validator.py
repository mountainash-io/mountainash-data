from dataclasses import is_dataclass, fields
from typing import Any, Dict, List, Set, Type, Optional, Sequence
import polars as pl


class DataStructureValidator:
    """Validates input data structures before conversion."""
    
    @classmethod
    def validate_list_of_dicts(cls, data: List[Dict[str, Any]]) -> None:
        """
        Validate a list of dictionaries structure.
        
        Args:
            data: List of dictionaries to validate
            
        Raises:
            ValueError: If validation fails
        """
        if not data:
            return

        if not isinstance(data, list):
            raise ValueError("Expected a list of dictionaries")

        if not all(isinstance(item, dict) for item in data):
            raise ValueError("All items must be dictionaries")

        # Check key consistency
        reference_keys = set(data[0].keys())
        for idx, d in enumerate(data[1:], start=1):
            current_keys = set(d.keys())
            if current_keys != reference_keys:
                missing = reference_keys - current_keys
                extra = current_keys - reference_keys
                error_msg = f"Inconsistent keys at index {idx}."
                if missing:
                    error_msg += f" Missing keys: {missing}."
                if extra:
                    error_msg += f" Extra keys: {extra}."
                raise ValueError(error_msg)

    @classmethod
    def validate_dict_of_lists(cls, data: Dict[str, List[Any]]) -> None:
        """
        Validate a dictionary of lists structure.
        
        Args:
            data: Dictionary of lists to validate
            
        Raises:
            ValueError: If validation fails
        """
        if not data:
            return

        if not isinstance(data, dict):
            raise ValueError("Expected a dictionary of lists")

        if not all(isinstance(v, (list, tuple, Sequence)) for v in data.values()):
            raise ValueError("All values must be sequences (lists, tuples)")

        # Check length consistency
        if data:
            reference_length = len(next(iter(data.values())))
            for key, value in data.items():
                if len(value) != reference_length:
                    raise ValueError(
                        f"Inconsistent list lengths. Key '{key}' has length {len(value)}, "
                        f"expected {reference_length}"
                    )

    @classmethod
    def validate_dataclass_instance(cls, data: Any) -> None:
        """
        Validate a single dataclass instance.
        
        Args:
            data: Dataclass instance to validate
            
        Raises:
            ValueError: If validation fails
        """
        if not is_dataclass(data):
            raise ValueError(f"Expected a dataclass instance, got {type(data)}")

    @classmethod
    def validate_dataclass_list(cls, data: List[Any]) -> None:
        """
        Validate a list of dataclass instances.
        
        Args:
            data: List of dataclass instances to validate
            
        Raises:
            ValueError: If validation fails
        """
        if not data:
            return

        if not isinstance(data, list):
            raise ValueError("Expected a list of dataclass instances")

        # Validate first item
        if not is_dataclass(data[0]):
            raise ValueError(f"Expected dataclass instances, first item is {type(data[0])}")

        # Check type consistency
        reference_type = type(data[0])
        for idx, item in enumerate(data[1:], start=1):
            if not isinstance(item, reference_type):
                raise ValueError(
                    f"Inconsistent types at index {idx}. "
                    f"Expected {reference_type}, got {type(item)}"
                )

    @classmethod
    def _get_field_names(cls, data: Any) -> Set[str]:
        """
        Get available field names from the input data structure.
        
        Args:
            data: Input data of any supported type
            
        Returns:
            Set[str]: Set of field names
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

    @classmethod
    def validate_column_existence(cls, 
                                data: Any, 
                                required_columns: Set[str]) -> None:
        """
        Validate that required columns exist in the data structure.
        
        Args:
            data: Input data
            required_columns: Set of required column names
            
        Raises:
            ValueError: If any required columns are missing
        """
        available_fields = cls.get_field_names(data)
        missing_fields = required_columns - available_fields
        if missing_fields:
            raise ValueError(f"Required columns not found in data: {missing_fields}")