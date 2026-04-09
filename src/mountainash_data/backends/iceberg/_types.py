"""Iceberg to PyArrow type conversion helpers.

Extracted from the legacy base_pyiceberg_connection.py /
base_pyiceberg_operations.py during the Phase 3 deduplication. These
functions take Iceberg schema fields and return their PyArrow equivalents.

Pure functions, no classes. No external mountainash_data imports.
"""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    StringType,
    StructType,
    TimestampType,
    TimeType,
    UUIDType,
)


def iceberg_type_to_pyarrow(iceberg_type) -> pa.DataType:
    """Convert a single Iceberg field type to its PyArrow equivalent.

    Supports primitive types and the common composite types (list, map,
    struct). Unknown types fall back to ``pa.string()``.

    Args:
        iceberg_type: An Iceberg type object (e.g. ``BooleanType()``,
            ``ListType(...)``, etc.)

    Returns:
        The corresponding ``pa.DataType``.
    """
    if isinstance(iceberg_type, BooleanType):
        return pa.bool_()
    elif isinstance(iceberg_type, IntegerType):
        return pa.int32()
    elif isinstance(iceberg_type, LongType):
        return pa.int64()
    elif isinstance(iceberg_type, FloatType):
        return pa.float32()
    elif isinstance(iceberg_type, DoubleType):
        return pa.float64()
    elif isinstance(iceberg_type, DateType):
        return pa.date32()
    elif isinstance(iceberg_type, TimeType):
        return pa.time64("us")
    elif isinstance(iceberg_type, TimestampType):
        if iceberg_type.with_timezone:
            return pa.timestamp("us", tz="UTC")
        else:
            return pa.timestamp("us")
    elif isinstance(iceberg_type, StringType):
        return pa.string()
    elif isinstance(iceberg_type, UUIDType):
        # UUIDs are usually handled as strings in PyArrow
        return pa.string()
    elif isinstance(iceberg_type, BinaryType):
        return pa.binary()
    elif isinstance(iceberg_type, DecimalType):
        return pa.decimal128(iceberg_type.precision, iceberg_type.scale)
    elif isinstance(iceberg_type, FixedType):
        return pa.binary(iceberg_type.length)
    elif isinstance(iceberg_type, ListType):
        # Recursive call for list element type (one level deep)
        inner_type = iceberg_type.element_type
        pa_element_type = iceberg_type_to_pyarrow(inner_type)
        return pa.list_(pa_element_type)
    elif isinstance(iceberg_type, MapType):
        # For maps, we default to string keys and values
        return pa.map_(pa.string(), pa.string())
    elif isinstance(iceberg_type, StructType):
        # For structs, we create a nested field structure
        struct_fields = [
            pa.field(
                nested_field.name,
                pa.string(),
                nullable=not nested_field.required,
            )
            for nested_field in iceberg_type.fields
        ]
        return pa.struct(struct_fields)
    else:
        # Default fallback for unrecognised types
        return pa.string()


def iceberg_schema_to_pyarrow(iceberg_schema) -> pa.Schema:
    """Convert a full Iceberg Schema to a PyArrow Schema.

    Args:
        iceberg_schema: A ``pyiceberg.schema.Schema`` instance.

    Returns:
        Equivalent ``pa.Schema``.
    """
    pa_fields = []
    for field in iceberg_schema.fields:
        pa_type = iceberg_type_to_pyarrow(field.field_type)
        pa_fields.append(pa.field(field.name, pa_type, nullable=not field.required))
    return pa.schema(pa_fields)
