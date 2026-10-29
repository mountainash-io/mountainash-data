# Editorial brief: mountainash-data

Status: confirmed — 2026-10-29; **revision 2 (ecosystem positioning) confirmed 2026-10-29** (see "Approval and open questions").
Source revision: `2d0782b507141baecd5a120ac0547c7e8af11662` on `develop`, written on `docs/textbook-rebuild`. The working tree was clean under `src/` when the brief was drafted.
Profile basis: `docs-site/profile/manifest.json` at `2d0782b`, validated with `ibook profile validate`, with no review-required warnings. It holds 30 module records and 6 facets: `users`, `maintainers`, `contributors`, `backend-architecture`, `extension-authors`, `broader-hype`. The two low-confidence modules (`core.registry`, `core.constants`) are resolved to code backlog DEBT-25.
Operation: **replacement** of the existing 9-chapter book (merged in #150). Revision 2 is an editorial revision of that book on `docs/textbook-ecosystem`, verified at `6526a1b` (no `src/` changes since `2d0782b`).

## Purpose and reader outcomes

The book documents `mountainash-data` as **the physical data layer of the mountainash ecosystem**. It turns `mountainash-settings` configuration and `mountainash-auth-client` credentials into live database connections. It hands database tables to `mountainash` expressions, relations, rules and contracts, which are written once and run anywhere: the same expression runs on a local dataframe or a warehouse table, and a database table joins with local dataframes and plain Python data in one plan. It writes results back through portable physical operations (upsert, `add_columns`, indexes) under explicit transaction ownership. Over bare Ibis it adds config-driven connections, credential separation, portable writes, transaction ownership and a physical metadata model, all behind the backend-neutral `Backend` protocol across 20 Ibis dialects.

The book answers "why not just use Ibis?" on every reader-facing surface: the value is the ecosystem integration, not a thinner connection wrapper. Ownership boundaries follow the ENFORCED physical/logical principle: `mountainash` owns expressions, relations and cross-source semantics; `mountainash-data` owns connections, physical metadata and physical writes, and does not import `mountainash`. `mountainash[ibis]` in turn depends on `mountainash-data` for SQL transactions and owned-copy work.

After reading, a reader can:

- **Use it in the ecosystem:** configure connections from `mountainash-settings` sources (config files, environment, secret references, `SettingsParameters` recipes that travel across worker boundaries); run `mountainash` expressions, rules and contracts against database tables; join database tables with local dataframes and Python data; and write results back transactionally.
- **Use it directly:** install the right extra, connect three ways (dialect kwargs, URL, `SettingsParameters`), supply auth separately, inspect catalogs, namespaces, tables and indexes, read and write tables, upsert, add columns, manage indexes, run explicit transaction scopes, adopt caller-owned connections, and hand an Ibis table to `mountainash.relation()`.
- **Know the limits:** which operations each dialect supports, and what fails how (`NotImplementedError`, `ValueError`, the transaction errors, missing-driver `ImportError`).
- **Understand it:** how one class serves 20 dialects through `DialectSpec` data, how operations fall back from per-dialect hooks to generic sqlglot-rendered SQL, how transaction ownership and protection work, and how settings and auth become driver kwargs.
- **Extend it:** add a dialect, add a settings backend with auth adapters, or implement the `Backend` protocol for a non-Ibis store.

## Audience coverage

All six facets are covered. Facets inform reading paths; they do not each get their own chapter.

| Facet | How the book serves it |
|---|---|
| `users` | The primary path, which reads on its own: install → connect → inspect → read/write → mutations and indexes → transactions → adoption → support and limits. Each topic is anchored on a runnable `examples/` recipe. |
| `backend-architecture` | An architecture part covering protocol tiers, capability-as-data dispatch, generic-default vs hook implementations, the transaction model, and the settings → driver-kwargs pipeline. Shared with maintainers. |
| `maintainers` | The same architecture part, plus internals notes: invariants (transaction probes, raw-adoption snapshot/restore, credential redaction, spec/adapter consistency), compatibility shims and their Ibis-private coupling, and known debt (linked, not repeated). |
| `extension-authors` | An extending part: protocol conformance (with the `_StubBackend` test as template), dialect hooks and capability fields, and settings/auth registration. It marks the `_`-prefixed modules as the private boundary. |
| `contributors` | Shares the extending part, plus a page on repository layout, test tiers (unit, optional-backend, live-db harness) and tooling. It links to `CONTRIBUTING.md` and `TESTING.md` rather than duplicating them. |
| `broader-hype` | Served by the home and About pages and the ecosystem chapters: write once and run anywhere, join database tables with local data, config-driven dev-to-prod switches, portable writes back. Only claims backed by runnable examples and tests. No dedicated positioning chapter; the ecosystem chapters carry the proof. |

Shared explanations are written once and linked. For example, `Namespace` is explained in the user path and linked from the architecture and extending parts.

## Scope and exclusions

**In scope:**
- Installation and per-dialect extras.
- `IbisBackend` construction and lifecycle.
- `BackendProfile` / `BackendSpec` settings and the 20 backend profiles (as a reference table, not 20 sections).
- `*AuthProfile` usage (owned by `mountainash-auth-client`; the book covers how this package consumes it).
- The connection factory.
- The inspection model, `Namespace` and `IndexInfo`.
- Table, query and view operations; upsert, `add_columns`, rename and truncate; indexes.
- Transactions: `transaction()`, `in_transaction()`, `native_transaction_open()`, raw-handle caveats.
- `from_ibis_connection` / `from_raw_connection` and session-option preservation.
- **Ecosystem integration (revision 2), shown with runnable cross-package examples:** `mountainash-settings` configuration sources and `SettingsParameters` recipes feeding `IbisBackend`; `mountainash` relations and expressions over backend tables (write once, run anywhere); joins between database tables and local dataframes or Python data; rules/contracts/conform evaluated against database tables; writing results back with upsert under a transaction; `mountainash[ibis]` borrowing a connection through this package's public API; an end-to-end pipeline.
- The error types.
- The per-dialect support and limits matrix, generated from `DialectSpec` capabilities.
- Architecture and extension points as listed above.

**Excluded:**
- The Iceberg/lakehouse backend (retired in #103). No lakehouse positioning.
- This package does not itself plan joins across sources; cross-source joins are a `mountainash` capability and are documented as such, at the integration level.
- `core.registry` and the unused `core.constants` enums as public API. They get a maintainer internals note only, until DEBT-25.
- The full APIs of `mountainash-settings`, `mountainash-auth-client` and `mountainash`. The book shows every integration point with a runnable example and links to each package's own documentation for the rest. OAuth acquisition flows stay with `mountainash-auth-client`.
- Claims about hosted services or every operation on every dialect beyond what tests show.
- `docs/dbt_integration.md` (a research transcript, not package capability).
- Quizzes and course material.

**Prerequisites:**
- Users: intermediate Python and basic SQL. Ibis knowledge helps but isn't required; link to the Ibis docs for expression building.
- Architecture, maintainers and extension authors: comfort reading Python dataclasses and protocols, and the user path or equivalent familiarity.

## Depth and reading paths

The progression is concepts → package use → internals. Each reader can enter at the depth they need:

1. **Overview:** the package's place in the mountainash ecosystem, its layers, and why it exists beside Ibis.
2. **Using the package:** task-oriented chapters, each anchored on a verified recipe.
3. **Architecture and internals:** for backend-architecture and maintainers.
4. **Extending:** for extension authors and contributors.

Chapter and section structure is proposed in the chapter-plan step, not here. Later chapters link back to the earlier explanation instead of re-explaining it.

## Register

Package documentation: self-describing pages with no course preamble, no learning objectives, no "Summary"/"Key Takeaways" recaps, and no learner or course vocabulary. The old book's 12 June cleanup (removing the "Concepts Covered" and "Prerequisites" boilerplate) points the same way.

## Reuse

Verify everything against `2d0782b` before reusing it; nothing is inherited structurally.

- **Primary sources:** the `examples/*/example.py` recipes and their READMEs, which are runnable and documented with expected output, plus the README sections "Backend support and limits" and "Transaction ownership". These are the canonical examples; book code blocks are run against the source.
- **Old chapters, reused selectively after checking:**
  - Ch03's inspection-model explanations (update for `IndexInfo`/`location` changes).
  - Ch05's `DialectSpec` overview and connection-builder explanation (update for capability fields and hooks).
  - Ch02's protocol method explanations (add the transaction members).
  - Ch01 foundation material only where it explains a package design choice.
  - Ch06, ch07, ch08 and the Iceberg parts of ch04 and ch09 are **not** reused: their APIs no longer exist.
- **Old FAQ** (46 questions, heading-based, at `docs-site/learning-graph/faq.md`): carry forward accurate entries into the new FAQ. Drop the "Iceberg Backend and Lakehouse" category and every answer that relies on retired APIs.
- **Learning graph:** regenerated in step 3, reconciling stable IDs where concepts survive.
- **Revision 2:** the retired profile README (git `e01f180:docs-site/profile/README.md`) carries the original ecosystem framing ("connects the mountainash expression engine to the world's databases", multi-warehouse contracts, dev-to-prod without rewrites). Reuse its framing after removing Iceberg and retired APIs. Also the `mountainash` README and guides, the `mountainash-settings` README and examples, and the central principles `physical-logical-boundary` and `descriptor-based-settings`.

## Appendices

- **FAQ:** included at `docs/faq.md`, linking to the relevant chapter sections.
- **Glossary:** included at `docs/glossary.md`, linking to the relevant chapter sections.

**Reader-surface changes in the replacement:**
- Retire the published "Learning Graph" nav section and its copies under `docs/learning-graph/`; the canonical graph stays internal.
- Retire the empty "MicroSims" nav section and `docs/course-description.md` ("Package Overview").
- Change the site licence page from CC BY-NC-SA 4.0 to **Apache-2.0**, matching the package `LICENSE`.

## Visual policy

Mermaid first: a layer diagram, dispatch flow (hook → generic → `NotImplementedError`), transaction-scope state, and the settings → kwargs pipeline. No MicroSims are proposed; none add explanatory value beyond these diagrams. No quizzes.

## Approval and open questions

Confirmed by the repository owner in the agent session on 2026-10-29:

1. **Structure:** the four-part depth progression (Overview → Using the package → Architecture and internals → Extending) with all six facets covered. `broader-hype` is served by the home and About pages only, with no dedicated chapter.
2. **Appendices:** the FAQ and glossary are included.
3. **Reader surfaces:** retire the published learning-graph pages, the MicroSims section and the Package Overview page, and relicense the site to Apache-2.0.
4. **Exclusions:** confirmed as listed. Iceberg is dropped entirely, with no "retired" note.

Earlier decisions in the same session that this brief relies on: replacement route chosen over a bounded refresh, and the `core.registry`/`core.constants` review flags resolved to code backlog DEBT-25.

### Revision 2 — ecosystem positioning (confirmed 2026-10-29)

Owner feedback: the book saw the package only in the context of itself. A user who only wanted Ibis connections would use Ibis; the value is integration with `mountainash-settings` and `mountainash` expressions. Confirmed in the agent session:

1. **Positioning:** the physical data layer of the mountainash ecosystem, as stated under Purpose. The owner added the key advantage: true write-once, run-anywhere expressions that join anything, including local dataframes and Python structures, with database connections. Verified at `6526a1b`: a `mountainash` relation over an `IbisBackend.table()` joins a Polars dataframe and a list of dicts, and the same expression filters both a local frame and the database table.
2. **Structure:** add ecosystem chapters and reframe the rest: chapter 1 becomes the ecosystem overview; new chapters on configuring connections with `mountainash-settings`, on running `mountainash` expressions against databases, and an end-to-end pipeline; leads elsewhere reframed.
3. **Depth:** every integration point shown with runnable cross-package examples; each neighbouring package's full API linked out.

Superseded from revision 1: "broader-hype served by home/About only" (now also the ecosystem chapters) and the exclusions of configuration loading and cross-source joins (now in scope at the integration level).

The revised chapter structure is approved separately at the chapter-plan step.
