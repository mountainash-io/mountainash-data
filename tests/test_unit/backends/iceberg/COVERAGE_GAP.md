# Iceberg test coverage gap

Before Phase 3 (writing-plans audit), no tests existed for any of:

- `databases/connections/pyiceberg/base_pyiceberg_connection.py` (884 LOC)
- `databases/connections/pyiceberg/pyiceberg_rest_connection.py` (205 LOC)
- `databases/operations/pyiceberg/base_pyiceberg_operations.py` (868 LOC)
- `databases/operations/pyiceberg/pyiceberg_rest_operations.py` (207 LOC)

The Phase 3 refactor proceeds *without* a regression net for iceberg.
This is acceptable because the user (sole consumer) confirmed iceberg
is in prototype use only. Tests added during this phase target the new
shape (IcebergBackend protocol, inspection model conversion) rather
than reproducing legacy behavior.

If iceberg moves to production use later, a separate hardening pass
should add tests for the salvaged operations.
