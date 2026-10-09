# Upsert and additive schema changes

Follow the [source installation](../../README.md#installation) with the `duckdb` extra.

Run from the repository root (or use an absolute script path from another directory):

```bash
python examples/mutations/example.py
```

## Expected output

```text
updated north total: 140
inserted west total: 80
added column: note (3 null values)
```

DuckDB requires a unique constraint or index on the conflict columns. This recipe
creates a unique index on `id`, upserts an updated north sale and a new west sale,
then adds a nullable `note` column. The final rows verify the update, insertion,
preservation of the south sale and null values in the new column.
