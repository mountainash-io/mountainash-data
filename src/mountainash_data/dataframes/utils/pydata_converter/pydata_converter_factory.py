from abc import ABC, abstractmethod
from dataclasses import is_dataclass, fields
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Type, Union, Sequence
import inspect

import pandas as pd
import polars as pl
import pyarrow as pa
import ibis.expr.types as ir
from pydantic import BaseModel
from .base_pydata_converter import BasePyDataConverter

# from .pydata_converter_factory import PyDataConverterFactory
from .pydata_converter_dataclass import PyDataConverterDataclass
from .pydata_converter_pydantic import PyDataConverterPydantic
from .pydata_converter_pydict import PyDataConverterPydict
from .pydata_converter_pylist import PyDataConverterPylist


class PyDataConverterFactory:
    """Factory for creating appropriate conversion strategies."""
    
    _strategies: List[Type[BasePyDataConverter]] = [
        # PyDataConverterFactory,
        PyDataConverterDataclass,
        PyDataConverterPydantic,
        PyDataConverterPydict,
        PyDataConverterPylist
    ]
    
    @classmethod
    def get_strategy(cls, data: Any) -> BasePyDataConverter:
        """
        Get the appropriate strategy for the input data.
        
        Args:
            data: Input data to convert
            
        Returns:
            ConversionStrategy: Appropriate strategy for the data
            
        Raises:
            ValueError: If no suitable strategy is found
        """
        for strategy_class in cls._strategies:
            strategy = strategy_class()
            if strategy.can_handle(data):
                return strategy
                
        raise ValueError(f"No suitable conversion strategy found for type: {type(data)}")