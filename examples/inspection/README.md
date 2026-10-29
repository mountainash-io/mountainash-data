# Namespace-aware inspection

Follow the [source installation](../../README.md#installation) with the `sqlite` extra.

Run from the repository root (or use an absolute script path from another directory):

```bash
python examples/inspection/example.py
```

## Expected output

```text
namespace: main
tables: sales
columns: id, region, total
```

The script creates an in-memory SQLite table in the `main` namespace, then uses `Namespace(path=("main",))` to list and inspect it. It also inspects the namespace itself and asserts the returned location, columns, and table membership before printing.
