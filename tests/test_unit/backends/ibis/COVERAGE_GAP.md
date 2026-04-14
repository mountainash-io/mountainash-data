# Coverage Gap: to_relation() not implemented

**Task 4.9 — to_relation() seam to mountainash-expressions**

The `mountainash-expressions` package does not expose a `Relation.from_ibis(table)`
API or equivalent. The `Relation` class in
`mountainash-expressions/src/mountainash/relations/core/relation_api/relation.py`
has no constructor that takes an ibis Table object.

There is a `_from_ibis` helper in `typespec/extraction.py` but it returns a
`TypeSpec`, not a `Relation`.

**Status:** BLOCKED — to_relation() cannot be implemented without a
`mountainash-expressions` API that creates a Relation from an ibis Table.

**Resolution:** When mountainash-expressions adds `Relation.from_ibis()` or an
equivalent, implement `IbisConnection.to_relation(table_name)` in
`src/mountainash_data/backends/ibis/backend.py`:

```python
def to_relation(self, table_name: str, namespace: str | None = None):
    from mountainash.relations import Relation  # adjust to actual import
    ibis_table = self._ibis_conn.table(table_name)
    return Relation.from_ibis(ibis_table)  # adjust to actual API
```

**Confirmed:** 2026-04-07 during Phase 4 (Task 4.9) refactor.
