# Editorial brief: mountainash-data

Status: confirmed — 2026-10-29 (see "Approval and open questions").
Source revision: `2d0782b507141baecd5a120ac0547c7e8af11662` on `develop`, written on `docs/textbook-rebuild`. The working tree was clean under `src/` when the brief was drafted.
Profile basis: `docs-site/profile/manifest.json` at `2d0782b`, validated with `ibook profile validate`, with no review-required warnings. It holds 30 module records and 6 facets: `users`, `maintainers`, `contributors`, `backend-architecture`, `extension-authors`, `broader-hype`. The two low-confidence modules (`core.registry`, `core.constants`) are resolved to code backlog DEBT-25.
Operation: **replacement** of the existing 9-chapter book, which stays published until the replacement is accepted.

## Purpose and reader outcomes

The book documents `mountainash-data`: one typed Python handle (`IbisBackend`) for connecting to, inspecting and managing relational databases across 20 Ibis dialects, behind a backend-neutral `Backend` protocol.

After reading, a reader can:

- **Use it:** install the right extra, connect three ways (dialect kwargs, URL, `SettingsParameters`), supply auth separately, inspect catalogs, namespaces, tables and indexes, read and write tables, upsert, add columns, manage indexes, run explicit transaction scopes, adopt caller-owned connections, and hand an Ibis table to `mountainash.relation()`.
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
| `broader-hype` | Served by the home and About pages ("same code, any engine", dev-to-prod switch, portable writes), using only claims backed by `examples/` and the live-backend tests. **Proposed: no dedicated chapter.** |

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
- The pull-side bridge to `mountainash.relation()`.
- The error types.
- The per-dialect support and limits matrix, generated from `DialectSpec` capabilities.
- Architecture and extension points as listed above.

**Excluded:**
- The Iceberg/lakehouse backend (retired in #103). No lakehouse positioning.
- Automatic joins across connections (the README states the package does not plan them).
- `core.registry` and the unused `core.constants` enums as public API. They get a maintainer internals note only, until DEBT-25.
- Configuration-file loading and secrets resolution (owned by `mountainash-settings`), and auth flows (owned by `mountainash-auth-client`). Both are linked out.
- Claims about hosted services or every operation on every dialect beyond what tests show.
- `docs/dbt_integration.md` (a research transcript, not package capability).
- Quizzes and course material.

**Prerequisites:**
- Users: intermediate Python and basic SQL. Ibis knowledge helps but isn't required; link to the Ibis docs for expression building.
- Architecture, maintainers and extension authors: comfort reading Python dataclasses and protocols, and the user path or equivalent familiarity.

## Depth and reading paths

The progression is concepts → package use → internals. Each reader can enter at the depth they need:

1. **Overview:** what the package is, its layers, and where `mountainash-settings`, `mountainash-auth-client`, Ibis and `mountainash` fit.
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

No open questions remain for this gate. The chapter structure is approved separately at the chapter-plan step.
