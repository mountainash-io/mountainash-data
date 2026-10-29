---
title: Reading tables and running queries
description: Read tables as Ibis expressions, run queries and SQL, and pass tables to mountainash.
date: 2026-10-29
---

# Reading tables and running queries

Reading goes through Ibis. `IbisBackend.table()` returns a backend-native Ibis table expression; you build queries on it with the [Ibis expression API](https://ibis-project.org/reference/expression-tables), then execute them through Ibis or through the backend's `run_expr()`. `to_sql()` shows the SQL a query compiles to, and `run_sql()` turns a SQL string into a table expression. The same table can be handed to the separate `mountainash` package as a relation ([Running mountainash expressions against databases](../07-running-mountainash-expressions-against-databases/index.md)).

The examples use an in-memory DuckDB or SQLite backend; see [Installing and connecting](../02-installing-and-connecting/index.md) for connection options.

## table() and Ibis expressions

<!-- concept:141 -->
`table(name, *, namespace=None)` looks a table up on the live connection and returns an `ibis.Table`. Nothing is read at that point: the result is an expression bound to the backend, and every filter, aggregate or join you add builds a larger expression that runs on the database when you ask for results. `namespace=` takes any `NamespaceLike` and follows the same single-level rule as inspection (see [Namespace and NamespaceLike](../05-inspecting-databases/index.md#namespace-and-namespacelike)). A missing table raises the error Ibis raises for it.

```python
from mountainash_data import IbisBackend

ROWS = [
    {"id": 1, "region": "north", "total": 120},
    {"id": 2, "region": "south", "total": 95},
    {"id": 3, "region": "north", "total": 80},
]

with IbisBackend(dialect="duckdb", database=":memory:") as backend:
    backend.create_table("sales", ROWS)
    sales = backend.table("sales")
    print(type(sales).__name__)
    print(sales.schema())
    north = sales.filter(sales.region == "north").aggregate(total=sales.total.sum())
    print(north.to_pyarrow().to_pylist())
```

```text
Table
ibis.Schema {
  id      int64
  region  string
  total   int64
}
[{'total': 200}]
```

Ibis's own terminal methods (`to_pyarrow()`, `to_pandas()`, `to_polars()`, `execute()`) work on the expression directly, because it is bound to the backend's connection. The [local tables recipe](https://github.com/mountainash-io/mountainash-data/tree/develop/examples/local_tables) builds the same filter-and-aggregate query. Expression building itself is Ibis's concern; the [Ibis documentation](https://ibis-project.org/) covers it in full.

## Fluent chaining

<!-- concept:21 -->
`IbisBackend` methods that change state return the backend itself, so they chain: `connect()`, `create_table()`, `insert()`, `drop_table()`, `create_view()`, `upsert()`, `add_columns()`, `create_index()`, `drop_index()` and the other write operations all return `self`. Methods that return data (`table()`, `run_expr()`, `run_sql()`, `to_sql()`, the `list_*` and `inspect_*` methods) end the chain. Without a `with` block, close the backend yourself:

```python
from mountainash_data import IbisBackend

backend = (
    IbisBackend(dialect="sqlite", database=":memory:")
    .connect()
    .create_table("sales", [{"id": 1, "region": "north", "total": 120}])
    .insert("sales", [{"id": 2, "region": "south", "total": 95}])
)
try:
    print(backend.table("sales").count().execute())
finally:
    backend.close()
```

```text
2
```

The write methods in the chain are covered in [Creating and changing tables](../08-creating-and-changing-tables/index.md).

## run_expr and to_sql

<!-- concept:139 -->
`run_expr(expr, *, params=None, limit="default", **kwargs)` executes an Ibis expression on the backend's connection and returns the result Ibis produces for that kind of expression: a Python scalar for a scalar expression and a pandas `DataFrame` for a table. `params` binds Ibis scalar parameters, and `limit` keeps Ibis's semantics, where `"default"` applies the Ibis default row limit (unlimited unless you set one). Calling `expr.execute()` gives the same result. The difference is that `run_expr()` goes through the backend. Inside a [`transaction()`](../10-transactions/index.md) scope on SQLite, DuckDB or PostgreSQL, a package call is protected: Ibis cannot commit the scope's transaction behind it, and a failure poisons the unit of work so that it rolls back. An expression executed directly bypasses that protection.

<!-- concept:140 -->
`to_sql(expr, *, params=None, limit=None, pretty=False)` compiles an expression to the SQL string the connected dialect would run, without executing it. It is useful for logging, for review, and for checking what a portable expression becomes on a particular engine. `pretty=True` formats the output over several lines.

```python
from mountainash_data import IbisBackend

with IbisBackend(dialect="duckdb", database=":memory:") as backend:
    backend.create_table("sales", [
        {"id": 1, "region": "north", "total": 120},
        {"id": 2, "region": "south", "total": 95},
    ])
    sales = backend.table("sales")
    by_region = sales.group_by("region").aggregate(total=sales.total.sum()).order_by("region")

    print(backend.run_expr(sales.total.sum()))
    frame = backend.run_expr(by_region)
    print(type(frame).__name__)
    print(frame)
    print(backend.to_sql(by_region))
```

```text
215
DataFrame
  region  total
0  north    120
1  south     95
SELECT * FROM (SELECT "t0"."region", SUM("t0"."total") AS "total" FROM "sales" AS "t0" GROUP BY 1) AS "t1" ORDER BY "t1"."region" ASC
```

## run_sql

<!-- concept:22 -->
`run_sql(query, *, schema=None, dialect=None)` wraps a SQL `SELECT` as an Ibis table expression. It does not return rows: the query becomes a subquery that you can execute, filter or join like any other table. `schema` supplies the result schema when Ibis cannot infer it, and `dialect` names the SQL dialect the string is written in when it differs from the backend's, so Ibis can transpile it. Because the result is an expression, `run_sql()` is for queries that produce rows. Statements such as DDL or `UPDATE` belong in the package's write methods or on the [raw driver handle](../11-adopting-existing-connections/index.md).

```python
from mountainash_data import IbisBackend

with IbisBackend(dialect="duckdb", database=":memory:") as backend:
    backend.create_table("sales", [
        {"id": 1, "region": "north", "total": 120},
        {"id": 2, "region": "south", "total": 95},
    ])
    big = backend.run_sql("SELECT id, total FROM sales WHERE total > 100")
    print(type(big).__name__)
    print(backend.run_expr(big))
    print(big.mutate(doubled=big.total * 2).to_pyarrow().to_pylist())
```

```text
Table
   id  total
0   1    120
[{'id': 1, 'total': 120, 'doubled': 240}]
```

## Running mountainash expressions on these tables

The Ibis table from `table()` is also the handoff to the separate [`mountainash`](https://github.com/mountainash-io/mountainash) package: `mountainash.relation(backend.table(name))` gives a relation whose expressions compile to SQL on the same connection, and which can join database tables with Polars frames and Python data in one plan. [Running mountainash expressions against databases](../07-running-mountainash-expressions-against-databases/index.md) covers the handoff, write-once expressions, cross-source joins, and conforming and validating database tables.
