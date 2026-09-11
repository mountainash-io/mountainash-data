# Coverage Report — mountainash-data

**Generated:** 2026-06-02
**Source hash:** 4c077666ebc560f82e9bbd7ef419bdfde2c5f62f
**Mode:** initial-profile

## Summary

| Metric | Count |
|--------|-------|
| Modules discovered | 22 |
| Modules profiled | 22 |
| Missing profiles | 0 |
| Stale profiles | 0 |
| Orphan profiles | 0 |
| Facets written | 5 |

## Coverage Gaps

### Modules with no dedicated tests

| Module | Priority | Note |
|--------|----------|------|
| mountainash_data.core.connection | useful | BaseDBConnection has no direct unit tests |
| mountainash_data.core.constants | reference-only | Enums are stable; tested indirectly |
| mountainash_data.core.registry | useful | Stub module — no tests because no wired backends |
| mountainash_data.core.settings.auth | reference-only | Pure re-export shim; tested upstream |
| mountainash_data.core.settings.adapters | internal-note | Tested indirectly via per-backend settings tests |
| mountainash_data.backends.ibis.operations | internal-note | Hook functions tested indirectly via backend tests |
| mountainash_data.backends.iceberg.connection | useful | ABC tested indirectly via REST catalog |
| mountainash_data.backends.iceberg.operations | internal-note | Tested indirectly via connection delegation |
| mountainash_data.backends.iceberg.inspect | reference-only | Converters tested indirectly |
| mountainash_data.backends.iceberg.catalogs | internal-note | REST catalog tested via iceberg backend tests |

### Low-confidence classifications

| Module | Confidence | Open question |
|--------|-----------|---------------|
| mountainash_data.core.registry | medium | Phase 4 placeholder — will it be wired to IbisBackend/IcebergBackend? |

### Public modules absent from users facet

None — all public modules are represented.

### Internal modules absent from maintainers facet

None — all modules are visible in maintainers facet.

### Known risks

| Module | Risk |
|--------|------|
| mountainash_data.core.connection | Dual contract surface: Backend protocol + BaseDBConnection ABC coexist |
| mountainash_data.core.registry | Stub — no backends auto-registered |
| mountainash_data.backends.iceberg.operations | Upsert reads entire table into memory for key matching |
| mountainash_data.backends.iceberg.catalogs | REST catalog _upsert uses broken DuckDB cursor syntax |
| mountainash_data.backends.iceberg.operations | truncate and create_view raise NotImplementedError |

## Next recommended documentation work

1. **User quick start guide** — connect to SQLite/DuckDB in 5 lines, show inspect_table output
2. **Settings reference per backend** — auto-generate from BackendSpec parameters
3. **IbisBackend method reference** — document fluent API with examples
4. **Contributor guide: adding a new backend** — step-by-step with template
5. **Architecture overview** — protocol layer, settings layer, backend dispatch
6. **Backend capability matrix** — what each dialect supports (DDL, indexes, upsert, etc.)
