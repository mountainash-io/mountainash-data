# Local tables and Ibis expressions

Follow the [source installation](../../README.md#installation) with the `duckdb` extra.

Run from the repository root (or use an absolute script path from another directory):

```bash
python examples/local_tables/example.py
```

## Expected output

```text
north total: 200
sales rows: 3
```

This example creates a temporary in-memory DuckDB backend, creates the `sales` table with two rows, inserts another row, and executes an Ibis filter-and-aggregate expression. Both returned results are asserted before printing; the backend closes when the `with` block exits.
