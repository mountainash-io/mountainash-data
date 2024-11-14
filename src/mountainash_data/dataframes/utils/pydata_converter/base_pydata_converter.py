from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Dict, Optional

import polars as pl

class InputType(Enum):
    """Enumeration of supported input data structure types."""
    LIST_OF_DICTS = auto()
    DICT_OF_LISTS = auto()
    DATACLASS = auto()
    PYDANTIC = auto()
    DATAFRAME = auto()
    UNKNOWN = auto()

class BasePyDataConverter(ABC):
    """Abstract base class for data structure conversion strategies."""
    
    @abstractmethod
    def can_handle(self, data: Any) -> bool:
        """
        Check if this strategy can handle the input data.
        
        Args:
            data: Input data to check
            
        Returns:
            bool: True if this strategy can handle the data
        """
        pass

    @abstractmethod
    def validate(self, 
                data: Any, 
                column_mapping: Optional[Dict[str, str]] = None) -> None:
        """
        Validate the input data structure.
        
        Args:
            data: Input data to validate
            column_mapping: Optional column mapping dictionary
            
        Raises:
            ValueError: If validation fails
        """
        pass

    @abstractmethod
    def convert(self, 
                data: Any, 
                column_mapping: Optional[Dict[str, str]] = None) -> pl.DataFrame:
        """
        Convert input data to a Polars DataFrame.
        
        Args:
            data: Input data to convert
            column_mapping: Optional column mapping dictionary
            
        Returns:
            pl.DataFrame: Converted data
        """
        pass