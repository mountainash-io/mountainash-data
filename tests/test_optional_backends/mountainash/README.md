# Optional Mountain Ash dtype bridge cohort

These three tests require the separate `mountainash` package, including
`mountainash.core.dtypes.canonical.MountainashDtype`, alongside data's normal
local database and test dependencies. They exercise real dtype conversion and
an in-memory DuckDB table; no remote database is needed.

In a separately provisioned environment with those dependencies, run explicitly:

```sh
python -m pytest tests/test_optional_backends/mountainash -q -ra
```

This bridge integration belongs to the explicit optional tier despite its local
database use. It is excluded from default core collection. Do not install
`mountainash` in the core environment to satisfy these tests. This cohort has
**not been qualified by the selected-settings-stores migration**; its assertions
are preserved for separate optional-integration qualification.
