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
from ..column_mapper import ColumnMapper


class PyDataConverterPylist(BasePyDataConverter):
    """Strategy for handling list of dictionaries."""
    
    def can_handle(self, data: Any) -> bool:
        return (isinstance(data, list) and 
                len(data) > 0 and 
                isinstance(data[0], dict))

    def validate(self, 
                data: Any, 
                column_mapping: Optional[Dict[str, str|Dict[str,str]]] = None) -> None:
        if not data:
            return

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

        if column_mapping:
            missing_fields = set(column_mapping.keys()) - reference_keys
            if missing_fields:
                raise ValueError(f"Mapped columns not found in data: {missing_fields}")

    def convert(self, 
                data: Any, 
                column_mapping: Optional[Dict[str, str|Dict[str,str]]] = None,
                filter_unmapped: Optional[bool] = False) -> pl.DataFrame:
        
        df =  pl.DataFrame(data)

        if column_mapping:
            # Create ColumnMapConfig
            map_config = ColumnMapper.create_config(
                mapping=column_mapping,
                filter_unmapped=filter_unmapped  # Keep unmapped columns by default
            )
            # Apply the mapping using ColumnMapper
            df = ColumnMapper.apply_mapping(df, map_config)

        return df
       
