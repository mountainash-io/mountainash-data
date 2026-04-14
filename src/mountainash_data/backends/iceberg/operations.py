"""Iceberg table mutations: create, drop, insert, upsert, truncate, view ops.

Created in Phase 3 by deduplicating and splitting the legacy
base_pyiceberg_connection.py and base_pyiceberg_operations.py.

These functions take an active connection (IcebergConnectionBase instance) and
perform mutations. They are NOT class methods — they are module-level functions
that the connection class delegates to when consumers call e.g.
``connection.insert(...)``.

The ``connection`` parameter in each function is an IcebergConnectionBase
instance (typed as Any to avoid a circular import; the structural contract is
that it exposes ``.catalog_backend``, ``.table()``, ``.table_exists()``, and
``._schema_cache``).
"""

from __future__ import annotations

import typing as t
from datetime import date, datetime
from time import sleep

import pyarrow as pa
from mountainash_dataframes import DataFrameUtils, SupportedDataFrames
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import SortOrder

from mountainash_data.backends.iceberg._types import iceberg_type_to_pyarrow


# ---------------------------------------------------------------------------
# DataFrame preparation helper
# ---------------------------------------------------------------------------


def prepare_dataframe_for_iceberg(
    df: SupportedDataFrames,
    target_schema: t.Optional[Schema] = None,
    target_table: t.Optional[Table] = None,
) -> pa.Table:
    """Preprocess a dataframe to match Iceberg table schema requirements.

    Args:
        df: Input dataframe (any SupportedDataFrames type).
        target_schema: Iceberg schema to cast to. If None and
            ``target_table`` is supplied, the schema is read from the table.
        target_table: Iceberg table to extract schema from when
            ``target_schema`` is None.

    Returns:
        PyArrow table cast to the target schema.
    """
    if target_schema is None and target_table is None:
        return DataFrameUtils.to_pyarrow(df=df)

    if target_schema is None and target_table is not None:
        target_schema = target_table.schema()

    df = DataFrameUtils.to_pyarrow(df=df)

    pa_schema = pa.schema([])

    for field in target_schema.fields:
        iceberg_type = field.field_type
        iceberg_nullable = not field.required
        pa_type = iceberg_type_to_pyarrow(iceberg_type)
        pa_schema = pa_schema.append(pa.field(field.name, pa_type, nullable=iceberg_nullable))

    # Cast the dataframe to the new schema
    cast_arrays = []
    for field in pa_schema:
        if field.name in df.column_names:
            col_index = df.column_names.index(field.name)

            if pa.types.is_timestamp(field.type) and not pa.types.is_timestamp(
                df.column(col_index).type
            ):
                try:
                    values = df.column(col_index).to_pylist()
                    converted = [
                        datetime.fromisoformat(str(v)) if v is not None else None
                        for v in values
                    ]
                    cast_arrays.append(pa.array(converted, type=field.type))
                except Exception:
                    cast_arrays.append(df.column(col_index).cast(field.type, safe=True))
            elif pa.types.is_date(field.type) and not pa.types.is_date(
                df.column(col_index).type
            ):
                try:
                    values = df.column(col_index).to_pylist()
                    converted = [
                        date.fromisoformat(str(v)) if v is not None else None
                        for v in values
                    ]
                    cast_arrays.append(pa.array(converted, type=field.type))
                except Exception:
                    cast_arrays.append(df.column(col_index).cast(field.type, safe=True))
            elif pa.types.is_decimal(field.type):
                try:
                    values = df.column(col_index).to_pylist()
                    from decimal import Decimal

                    converted = [
                        Decimal(str(v)) if v is not None else None for v in values
                    ]
                    cast_arrays.append(pa.array(converted, type=field.type))
                except Exception:
                    cast_arrays.append(df.column(col_index).cast(field.type, safe=True))
            else:
                cast_arrays.append(df.column(col_index).cast(field.type, safe=True))
        else:
            # Create empty column for missing fields
            cast_arrays.append(pa.array([None] * len(df), type=field.type))

    return pa.Table.from_arrays(cast_arrays, schema=pa_schema)


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------


def retry_operation(operation_func: t.Callable, max_attempts: int = 3) -> t.Any:
    """Retry a PyIceberg operation that might fail due to commit conflicts.

    Args:
        operation_func: Zero-argument callable to retry.
        max_attempts: Maximum number of attempts.

    Returns:
        Return value of ``operation_func`` on success.

    Raises:
        RuntimeError: If all attempts fail.
    """
    attempts = 0
    last_error = None

    while attempts < max_attempts:
        try:
            return operation_func()
        except Exception as e:
            attempts += 1
            print(e)
            if attempts >= max_attempts:
                break
            sleep(0.2)

    raise last_error if last_error else RuntimeError("Operation failed for unknown reason")


# ---------------------------------------------------------------------------
# Table mutations
# ---------------------------------------------------------------------------


def create_table(
    connection: t.Any,
    table_name: str | t.Tuple[str, ...],
    schema: Schema,
    df: t.Optional[t.Any] = None,
    location: str | None = None,
    partition_spec: t.Optional[PartitionSpec] = None,
    sort_order: t.Optional[SortOrder] = None,
    overwrite: t.Optional[bool] = False,
) -> t.Optional[Table]:
    """Create an Iceberg table.

    Args:
        connection: Active ``IcebergConnectionBase`` instance.
        table_name: Table identifier (string or tuple of namespace parts).
        schema: Iceberg schema for the new table.
        df: Optional dataframe to append immediately after creation.
        location: Optional storage location override.
        partition_spec: Optional partition specification.
        sort_order: Optional sort order.
        overwrite: If True, drop an existing table first.

    Returns:
        The created Iceberg Table object, or None if backend unavailable.
    """
    connection.connect()

    params: dict[str, t.Any] = {
        "identifier": table_name,
        "schema": schema,
    }

    if location is not None:
        params["location"] = location
    if partition_spec is not None:
        params["partition_spec"] = partition_spec
    if sort_order is not None:
        params["sort_order"] = sort_order

    if not overwrite and connection.table_exists(table_name):
        print(
            f"Cannot create table - table already exists: {table_name}. "
            "Set overwrite=True."
        )
        return False

    if overwrite:
        connection.catalog_backend.drop_table(table_name)

    obj_table = (
        connection.catalog_backend.create_table(**params)
        if connection.catalog_backend is not None
        else None
    )

    # Refresh the schema cache after recreating the table
    connection.get_schema(table_name, refresh=True)

    if df is not None:
        pa_df = prepare_dataframe_for_iceberg(df, target_schema=schema)
        obj_table.append(pa_df)

    return obj_table


def drop_table(
    connection: t.Any,
    table_name: str | t.Tuple[str, ...],
    purge: t.Optional[bool] = False,
) -> bool:
    """Drop an Iceberg table.

    Args:
        connection: Active ``IcebergConnectionBase`` instance.
        table_name: Table identifier.
        purge: If True, purge data files as well.

    Returns:
        True if the table was dropped, False otherwise.
    """
    connection.connect()

    try:
        if connection.table_exists(table_name):
            if purge:
                (
                    connection.catalog_backend.purge_table(table_name)
                    if connection.catalog_backend is not None
                    else None
                )
            else:
                (
                    connection.catalog_backend.drop_table(table_name)
                    if connection.catalog_backend is not None
                    else None
                )
            return True
        return False
    except Exception:
        return False


def insert(
    connection: t.Any,
    table_name: str | t.Tuple[str, ...],
    df: t.Any,
    prevent_duplicates: t.Optional[bool] = False,
) -> bool:
    """Insert (append) data into an Iceberg table.

    Args:
        connection: Active ``IcebergConnectionBase`` instance.
        table_name: Table identifier.
        df: Input dataframe (any SupportedDataFrames type).
        prevent_duplicates: If True, use upsert semantics that skip
            existing rows instead of appending.

    Returns:
        True on success, False on failure.
    """
    connection.connect()

    try:
        schema = connection.get_schema(table_name)
        pa_df = prepare_dataframe_for_iceberg(df, target_schema=schema)
        table = connection.table(table_name)

        if prevent_duplicates:
            print(
                f"NOTE: insert operation on {table_name} may have prevented "
                "duplicates being added to the table. Records matching the "
                "existing keys were dropped. If you wish to perform a full "
                "insert set 'prevent_duplicates=False'. If you wish to also "
                "update existing rows use upsert()"
            )
            (
                table.upsert(
                    df=pa_df,
                    when_matched_update_all=False,
                    when_not_matched_insert_all=True,
                )
                if connection.catalog_backend is not None
                else None
            )
        else:
            print(
                f"NOTE: insert operation on {table_name} performing a straight "
                "append, which may cause duplicates to be added to the table. "
                "To avoid duplicates set 'prevent_duplicates=True'. To update "
                "rows that match existing natural keys use upsert()"
            )
            (
                table.append(df=pa_df)
                if connection.catalog_backend is not None
                else None
            )

        return True
    except Exception:
        return False


def upsert(
    connection: t.Any,
    table_name: str | t.Tuple[str, ...],
    df: t.Any,
    natural_key_columns: list[str] | None = None,
    when_matched_update_all: bool = True,
    when_not_matched_insert_all: bool = True,
    case_sensitive: bool = True,
) -> None:
    """Upsert data into an Iceberg table.

    Args:
        connection: Active ``IcebergConnectionBase`` instance.
        table_name: Table identifier.
        df: Input dataframe (any SupportedDataFrames type).
        natural_key_columns: Column(s) used to match existing rows.
            Maps to ``join_cols`` in pyiceberg's ``Table.upsert()``.
        when_matched_update_all: Update matched rows (default True).
        when_not_matched_insert_all: Insert unmatched rows (default True).
        case_sensitive: Case-sensitive key comparison (default True).
    """
    connection.connect()

    schema = connection.get_schema(table_name)
    pa_df = prepare_dataframe_for_iceberg(df, target_schema=schema)

    params: dict[str, t.Any] = {
        "df": pa_df,
        "when_matched_update_all": when_matched_update_all,
        "when_not_matched_insert_all": when_not_matched_insert_all,
        "case_sensitive": case_sensitive,
    }

    if natural_key_columns is not None:
        params["join_cols"] = natural_key_columns

    table = connection.table(table_name)
    (
        table.upsert(**params)
        if connection.catalog_backend is not None
        else None
    )


def truncate(
    connection: t.Any,
    table_name: str | t.Tuple[str, ...],
) -> None:
    """Truncate an Iceberg table (not yet implemented).

    Args:
        connection: Active ``IcebergConnectionBase`` instance.
        table_name: Table identifier.

    Raises:
        NotImplementedError: Always — pyiceberg does not expose a native
            truncate; this requires deleting all files manually.
    """
    raise NotImplementedError(
        "truncate() is not implemented for the Iceberg backend. "
        "pyiceberg does not expose a native truncate operation."
    )


# ---------------------------------------------------------------------------
# View operations
# ---------------------------------------------------------------------------


def create_view(
    connection: t.Any,
    view_name: str | t.Tuple[str, ...],
) -> None:
    """Create an Iceberg view (not yet implemented).

    Args:
        connection: Active ``IcebergConnectionBase`` instance.
        view_name: View identifier.

    Raises:
        NotImplementedError: Always — view creation is not yet wired up.
    """
    raise NotImplementedError(
        "create_view() is not implemented for the Iceberg backend."
    )


def drop_view(
    connection: t.Any,
    view_name: str | t.Tuple[str, ...],
) -> bool:
    """Drop an Iceberg view.

    Args:
        connection: Active ``IcebergConnectionBase`` instance.
        view_name: View identifier.

    Returns:
        True if the view was dropped, False otherwise.
    """
    connection.connect()

    try:
        if connection.view_exists(view_name):
            (
                connection.catalog_backend.drop_view(view_name)
                if connection.catalog_backend is not None
                else None
            )
            return True
        return False
    except Exception:
        return False
