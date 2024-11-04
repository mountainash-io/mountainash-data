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


class PyDataConverterDataclass(BasePyDataConverter):
    """Strategy for handling dataclass instances."""
    
    def can_handle(self, data: Any) -> bool:
        return (is_dataclass(data) or 
                (isinstance(data, list) and 
                 len(data) > 0 and 
                 is_dataclass(data[0])))

    def validate(self, 
                data: Any, 
                column_mapping: Optional[Dict[str, str|Dict[str,str]]] = None) -> None:
        if isinstance(data, list):
            if not data:
                return

            # Check all items are the same dataclass type
            dataclass_type = type(data[0])
            for idx, item in enumerate(data[1:], start=1):
                if not isinstance(item, dataclass_type):
                    raise ValueError(
                        f"Inconsistent types at index {idx}. "
                        f"Expected {dataclass_type}, got {type(item)}"
                    )

        if column_mapping:
            # Get available fields
            obj = data[0] if isinstance(data, list) else data
            available_fields = {field.name for field in fields(obj)}
            missing_fields = set(column_mapping.keys()) - available_fields
            if missing_fields:
                raise ValueError(f"Mapped columns not found in dataclass: {missing_fields}")

    def convert(self, 
                data: Any, 
                column_mapping: Optional[Dict[str, str|Dict[str,str]]] = None,
                filter_unmapped: Optional[bool] = False) -> pl.DataFrame:
        # Convert to list of dicts
        if isinstance(data, list):
            data_dicts = [
                {field.name: getattr(item, field.name) for field in fields(item)}
                for item in data
            ]
        else:
            data_dicts = [{
                field.name: getattr(data, field.name) for field in fields(data)
            }]

        # Create DataFrame
        df = pl.DataFrame(data_dicts)

        if column_mapping:
                # Create ColumnMapConfig
                map_config = ColumnMapper.create_config(
                    mapping=column_mapping,
                    filter_unmapped=filter_unmapped  # Keep unmapped columns by default
                )
                # Apply the mapping using ColumnMapper
                df = ColumnMapper.apply_mapping(df, map_config)


        return df