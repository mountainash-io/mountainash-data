# Quick Overview

  mountainash-data provides a unified interface for cross-database operations using Ibis as the underlying abstraction. It handles joins between different databases by intelligently moving data to where it's needed.

## Basic Example

```
  from mountainash_settings import SettingsParameters
  from mountainash_data import Oracle_IbisConnection, Snowflake_IbisConnection, IbisDataFrame

  # 1. Set up connections
  oracle_conn = Oracle_IbisConnection(db_auth_settings_parameters=oracle_auth_params)
  oracle_conn.connect()

  snowflake_conn = Snowflake_IbisConnection(db_auth_settings_parameters=snowflake_auth_params)
  snowflake_conn.connect()

  # 2. Load tables
  oracle_df = IbisDataFrame(
      df=oracle_conn.ibis_backend.table("customers", schema="sales"),
      ibis_backend=oracle_conn.ibis_backend
  )

  snowflake_df = IbisDataFrame(
      df=snowflake_conn.ibis_backend.table("orders", schema="transactions"),
      ibis_backend=snowflake_conn.ibis_backend
  )

  # 3. Join tables - three options:
  # Execute on Oracle (brings Snowflake data to Oracle)
  result = oracle_df.left_join(snowflake_df, predicates=["customer_id"], execute_on="left")

  # Execute on Snowflake (brings Oracle data to Snowflake)
  result = oracle_df.left_join(snowflake_df, predicates=["customer_id"], execute_on="right")

  # Execute locally (default - brings both to DuckDB/Polars)
  result = oracle_df.left_join(snowflake_df, predicates=["customer_id"])
```
##  Key Points

  - execute_on parameter controls where the join happens:
    - "left": Oracle backend
    - "right": Snowflake backend
    - None: Local backend (DuckDB/Polars)
  - Performance tip: Execute on the backend with the larger table to minimize data transfer
  - Available join methods: inner_join(), left_join(), outer_join()

  The library automatically handles schema resolution and data type mapping between the different databases.
