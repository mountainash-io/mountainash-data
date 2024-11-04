
from dataclasses import dataclass
from typing import Any, Optional, Dict

from .constants import DataType, NullStrategy


@dataclass
class TypeConfig:
    """Configuration for type enforcement on a column."""
    data_type: DataType
    null_strategy: NullStrategy = NullStrategy.ALLOW
    default_value: Optional[Any] = None
    
    def __post_init__(self):
        if self.null_strategy == NullStrategy.DEFAULT and self.default_value is None:
            raise ValueError("Default value must be provided when using DEFAULT null strategy")

@dataclass
class ColumnConfig:
    """Combined configuration for a column including mapping and type enforcement."""
    target_name: str
    type_config: TypeConfig


@dataclass
class ColumnMapConfig:
    """Configuration for column mapping operations."""
    mapping: Dict[str, ColumnConfig]  # from:to mapping
    filter_unmapped: bool = False  # whether to exclude columns not in mapping

