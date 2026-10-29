# Transaction scopes

## Prerequisites

Follow the [source installation](../../README.md#installation) with the `sqlite`
extra, which installs the dependencies needed by Ibis's SQLite backend.

## Run

From the repository root:

```bash
python examples/transactions/example.py
```

## Expected output

```text
scope active: True
native open: True
after commit - scope active: False
after commit - native open: False
committed rows: [(1, 'north', 120), (2, 'south', 95)]
after rollback: [(1, 'north', 120), (2, 'south', 95)]
```

## What it demonstrates

The package's `transaction()` context commits its own successful SQLite unit of
work and rolls it back when the body raises. `in_transaction()` reports package
scope registration; `native_transaction_open()` reports SQLite's native state.
The commit scope uses raw SQL; the rollback scope uses `backend.insert()` with a
fresh input. It never reuses an Ibis memtable expression after rollback.

Direct raw SQL bypasses package-call protection. This recipe demonstrates scope
ownership, not interception of raw statements. An absolute script path also works
from another working directory.
