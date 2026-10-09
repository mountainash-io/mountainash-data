# SQLite index lifecycle

Follow the [source installation](../../README.md#installation) with the `sqlite` extra.

Run from the repository root (or use an absolute script path from another directory):

```bash
python examples/indexes/example.py
```

## Expected output

```text
created index: ix_sales_region on region
indexes after drop: 0
```

The script creates an in-memory SQLite `sales` table, creates an index on `region`, inspects its `IndexInfo` via `list_indexes`, drops it, and asserts that no indexes remain. SQLite is a local backend with index create/list/drop support; the `with` block closes its connection.
