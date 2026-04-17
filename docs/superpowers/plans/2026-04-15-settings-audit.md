# Settings Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a per-backend audit report for each of 11 settings classes in `src/mountainash_data/core/settings/`, comparing each class against its authoritative driver spec, the corresponding Ibis backend `do_connect()` passthrough, and any vendor reference docs. Report-only; fixes happen in later per-backend cycles.

**Architecture:** 12 independent audit tasks (one index + 11 backends). Each backend task is self-contained: read settings class → fetch source specs (WebFetch) → read Ibis `do_connect()` signature → produce a markdown report at `docs/superpowers/specs/2026-04-15-settings-audit/<backend>.md` following the schema defined in the spec README. A final task updates the index table with summary counts and commits.

**Tech Stack:** Python 3.12, pydantic settings, Ibis 10.4.0, PyIceberg (for `pyiceberg_rest`). Tooling: Read/Grep for source, WebFetch for spec URLs, Write for reports, git for commits.

---

## Shared context for all audit tasks

**Spec README (authoritative for report format):** `docs/superpowers/specs/2026-04-15-settings-audit/README.md`

**Our settings classes:** `src/mountainash_data/core/settings/<backend>.py`

**Ibis backends (ground truth for passthrough):** `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/<ibis_backend>/__init__.py` — inspect the `do_connect()` method signature.

**Source precedence (when specs disagree):**
1. Driver/client spec — parameter names, types, defaults, validation semantics
2. Ibis backend `do_connect()` — what can actually pass through
3. Vendor docs — context only

**Parameter tiering:**
- `core` — auth, host/endpoint, TLS, timeouts, database/schema/catalog selection
- `advanced` — tuning knobs, deprecated options, esoterica

**Report schema (every per-backend report must contain):**
1. Header: spec URLs with precedence labels, date checked, settings class path, Ibis backend path
2. Stale-link check: WebFetch result per URL — `OK` / `redirect → <final>` / `404` / `blocked`
3. Summary counts: core missing, core mismatch, advanced missing, advanced mismatch, extra, total audited
4. Parameter table with columns: `Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes`
   - Status: `present` / `missing` / `mismatch` / `extra`
   - Type/Default: ✓ / ✗ / N/A
5. Findings narrative: core gaps → core mismatches → advanced gaps → advanced mismatches → stale links
6. Recommended follow-ups: concrete per-backend bullets for downstream plans

**Commit convention:** One commit per backend report, message `docs(audit): <backend> settings audit`. Final index-update commit: `docs(audit): fill audit index summary counts`.

---

## Task 1: SQLite settings audit

**Files:**
- Read: `src/mountainash_data/core/settings/sqlite.py`
- Read: `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/sqlite/__init__.py`
- Create: `docs/superpowers/specs/2026-04-15-settings-audit/sqlite.md`

**Source URLs (from docstring, precedence-tagged):**
- Vendor (context): https://www.sqlite.org/pragma.html
- Driver (authoritative): stdlib `sqlite3` — https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
- Ibis passthrough (authoritative for what we can pass): local file above

- [ ] **Step 1: Read our settings class**

Run: Read `src/mountainash_data/core/settings/sqlite.py`. Note every `Field(...)` declaration and what `get_connection_kwargs()` / `get_connection_string_params()` actually return.

- [ ] **Step 2: Fetch spec URLs and record link health**

Use WebFetch for each URL above. For each: record `OK` / `redirect → <final URL>` / `404` / `blocked`. Capture the parameter list (SQLite PRAGMAs for the vendor URL; `connect()` kwargs for the Python stdlib URL).

- [ ] **Step 3: Read Ibis `do_connect()` signature**

Run: Read `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/sqlite/__init__.py`. Locate `def do_connect(self, ...)`. List every keyword argument — this is the passthrough surface.

- [ ] **Step 4: Build parameter union and classify**

Build a set `U = driver_params ∪ our_fields ∪ ibis_passthrough_kwargs`. For each parameter in `U`:
- Status: `missing` if in driver but not ours; `extra` if in ours but not driver; `present` if in both with same name/semantics; `mismatch` if name/type/default diverges
- Tier: `core` if it's `database`/path/timeout/isolation; else `advanced`
- Type ✓ / Default ✓: compare our Field type/default to driver signature
- Ibis passthrough: ✓ if in `do_connect()` kwargs, ✗ otherwise

- [ ] **Step 5: Write the report**

Write to `docs/superpowers/specs/2026-04-15-settings-audit/sqlite.md` using the 6-section schema in the shared context above. Fill every section — no `TBD`.

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/sqlite.md
git commit -m "docs(audit): sqlite settings audit"
```

---

## Task 2: DuckDB settings audit

**Files:**
- Read: `src/mountainash_data/core/settings/duckdb.py`
- Read: `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/duckdb/__init__.py`
- Create: `docs/superpowers/specs/2026-04-15-settings-audit/duckdb.md`

**Source URLs:**
- Ibis backend (context): https://ibis-project.org/backends/duckdb
- Driver (authoritative): https://duckdb.org/docs/configuration/overview.html
- Vendor (context): https://duckdb.org/docs/extensions/spatial.html
- Ibis passthrough (authoritative): local file above

Follow the same 6 steps as Task 1 with these paths/URLs. Write report to `.../duckdb.md`. Commit message: `docs(audit): duckdb settings audit`.

- [ ] **Step 1: Read `src/mountainash_data/core/settings/duckdb.py`**
- [ ] **Step 2: WebFetch each source URL, record link health and parameter list**
- [ ] **Step 3: Read Ibis duckdb `do_connect()` signature**
- [ ] **Step 4: Build union, classify each parameter (status, tier, type, default, passthrough)**
- [ ] **Step 5: Write `docs/superpowers/specs/2026-04-15-settings-audit/duckdb.md` with all 6 report sections**
- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/duckdb.md
git commit -m "docs(audit): duckdb settings audit"
```

---

## Task 3: MotherDuck settings audit

**Files:**
- Read: `src/mountainash_data/core/settings/motherduck.py`
- Read: `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/duckdb/__init__.py` (motherduck rides on Ibis duckdb)
- Create: `docs/superpowers/specs/2026-04-15-settings-audit/motherduck.md`

**Source URLs (none in docstring — use canonical):**
- Driver (authoritative): https://motherduck.com/docs/getting-started/connect-query-from-python/installation-authentication/
- DuckDB extension (context): https://motherduck.com/docs/
- Ibis passthrough: via Ibis duckdb (record passthrough as `via duckdb`)

Header must note: "MotherDuck has no dedicated Ibis backend; passthrough tracked via Ibis duckdb backend. Source URLs were not present in the settings class docstring and were added by this audit — recommend backfilling into `motherduck.py` docstring in a follow-up."

- [ ] **Step 1: Read `src/mountainash_data/core/settings/motherduck.py`**
- [ ] **Step 2: WebFetch each source URL, record link health and parameter list (TOKEN, `md:` connection-string quirks, `ATTACH` semantics)**
- [ ] **Step 3: Read Ibis duckdb `do_connect()` signature — note which kwargs apply to motherduck connection strings (`md:<db>?motherduck_token=...`)**
- [ ] **Step 4: Build union, classify each parameter. "Ibis passthrough" column value: `via duckdb ✓` or `via duckdb ✗`**
- [ ] **Step 5: Write report to `docs/superpowers/specs/2026-04-15-settings-audit/motherduck.md`**
- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/motherduck.md
git commit -m "docs(audit): motherduck settings audit"
```

---

## Task 4: PostgreSQL settings audit

**Files:**
- Read: `src/mountainash_data/core/settings/postgresql.py`
- Read: `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/postgres/__init__.py`
- Create: `docs/superpowers/specs/2026-04-15-settings-audit/postgresql.md`

**Source URLs:**
- Driver (authoritative): https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
- Driver (authoritative, supplementary): https://www.postgresql.org/docs/current/libpq-connect.html
- Ibis passthrough (authoritative): local file above

This class has many enums (`PostgresTargetSessionAttrs`, `PostgresRequireAuthMethods`, `PostgresSSLCertNegotiation`, `PostgresSSLCertMode`) — enumerate each Enum and check that values match libpq's accepted values.

- [ ] **Step 1: Read `src/mountainash_data/core/settings/postgresql.py` in full; list every Field and every Enum value**
- [ ] **Step 2: WebFetch libpq docs; extract the full PQconnectdbParams keyword list**
- [ ] **Step 3: Read Ibis postgres `do_connect()` signature**
- [ ] **Step 4: Build union, classify. Note: for each Enum, cross-check allowed values against libpq and flag divergences in Notes column**
- [ ] **Step 5: Write report to `docs/superpowers/specs/2026-04-15-settings-audit/postgresql.md`. Include a dedicated "Enum value coverage" subsection under Findings**
- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/postgresql.md
git commit -m "docs(audit): postgresql settings audit"
```

---

## Task 5: MySQL settings audit

**Files:**
- Read: `src/mountainash_data/core/settings/mysql.py`
- Read: `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/mysql/__init__.py`
- Create: `docs/superpowers/specs/2026-04-15-settings-audit/mysql.md`

**Source URLs:**
- Driver (authoritative): https://mysqlclient.readthedocs.io/user_guide.html#functions-and-attributes
- Driver supplementary: https://dev.mysql.com/doc/c-api/8.4/en/mysql-ssl-set.html
- Driver supplementary: https://dev.mysql.com/doc/c-api/8.4/en/mysql-options.html
- Ibis passthrough: local file above

- [ ] **Step 1: Read `src/mountainash_data/core/settings/mysql.py`**
- [ ] **Step 2: WebFetch all three URLs; merge parameter lists (mysqlclient `connect()` kwargs + `mysql_ssl_set` parameters + `mysql_options` flags)**
- [ ] **Step 3: Read Ibis mysql `do_connect()` signature**
- [ ] **Step 4: Build union, classify. Call out deprecated SSL params in Notes**
- [ ] **Step 5: Write report to `docs/superpowers/specs/2026-04-15-settings-audit/mysql.md`**
- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/mysql.md
git commit -m "docs(audit): mysql settings audit"
```

---

## Task 6: MSSQL settings audit

**Files:**
- Read: `src/mountainash_data/core/settings/mssql.py`
- Read: `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/mssql/__init__.py`
- Create: `docs/superpowers/specs/2026-04-15-settings-audit/mssql.md`

**Source URLs:**
- Driver (authoritative): https://learn.microsoft.com/en-us/sql/connect/odbc/connection-string-keywords-and-data-source-names-dsns
- Driver install (context): https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
- pyodbc (authoritative, passthrough semantics): https://github.com/mkleehammer/pyodbc/wiki/The-pyodbc-Module#connect
- Ibis passthrough: local file above

This class has 4 enums (`MSSQLAuthMethod`, `MSSQLAuthEncryption`, `MSSQLAuthProtocol`, `MSSQLDriverType`) — audit enum values against ODBC docs.

- [ ] **Step 1: Read `src/mountainash_data/core/settings/mssql.py`; list Fields and all Enum values**
- [ ] **Step 2: WebFetch all three URLs; extract ODBC connection-string keywords**
- [ ] **Step 3: Read Ibis mssql `do_connect()` signature**
- [ ] **Step 4: Build union, classify. Cross-check each Enum against ODBC accepted values**
- [ ] **Step 5: Write report to `docs/superpowers/specs/2026-04-15-settings-audit/mssql.md` including "Enum value coverage" subsection**
- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/mssql.md
git commit -m "docs(audit): mssql settings audit"
```

---

## Task 7: Snowflake settings audit

**Files:**
- Read: `src/mountainash_data/core/settings/snowflake.py`
- Read: `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/snowflake/__init__.py`
- Create: `docs/superpowers/specs/2026-04-15-settings-audit/snowflake.md`

**Source URLs:**
- Driver (authoritative): https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#label-snowflake-connector-methods-connect
- Driver connect guide: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect
- OAuth (context): https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example#connecting-with-oauth
- Ibis passthrough: local file above

Class has `CONST_SNOWFLAKE_AUTHENTICATOR` enum — audit enum values against Snowflake's documented authenticator options.

- [ ] **Step 1: Read `src/mountainash_data/core/settings/snowflake.py`; list Fields and `CONST_SNOWFLAKE_AUTHENTICATOR` values**
- [ ] **Step 2: WebFetch all three URLs; extract `snowflake.connector.connect()` parameter list + authenticator values**
- [ ] **Step 3: Read Ibis snowflake `do_connect()` signature**
- [ ] **Step 4: Build union, classify. Cross-check authenticator enum values**
- [ ] **Step 5: Write report to `docs/superpowers/specs/2026-04-15-settings-audit/snowflake.md`**
- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/snowflake.md
git commit -m "docs(audit): snowflake settings audit"
```

---

## Task 8: BigQuery settings audit

**Files:**
- Read: `src/mountainash_data/core/settings/bigquery.py`
- Read: `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/bigquery/__init__.py`
- Create: `docs/superpowers/specs/2026-04-15-settings-audit/bigquery.md`

**Source URLs:**
- Driver (authoritative): https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client
- Ibis backend (context): https://ibis-project.org/backends/bigquery
- Auth (context): https://cloud.google.com/sdk/docs/authorizing
- External data sources (context): https://cloud.google.com/bigquery/external-data-sources
- Ibis passthrough (authoritative): local file above — BigQuery is unusual in that most config comes through Ibis backend kwargs (`project_id`, `dataset_id`, `credentials`, `application_default_credentials`, etc.) rather than a driver connection string

- [ ] **Step 1: Read `src/mountainash_data/core/settings/bigquery.py`**
- [ ] **Step 2: WebFetch each URL; extract `google.cloud.bigquery.Client` init parameters and Ibis-specific backend kwargs**
- [ ] **Step 3: Read Ibis bigquery `do_connect()` signature — treat this as the primary authoritative surface for BigQuery**
- [ ] **Step 4: Build union, classify**
- [ ] **Step 5: Write report to `docs/superpowers/specs/2026-04-15-settings-audit/bigquery.md`**
- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/bigquery.md
git commit -m "docs(audit): bigquery settings audit"
```

---

## Task 9: Redshift settings audit

**Files:**
- Read: `src/mountainash_data/core/settings/redshift.py`
- Read: `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/postgres/__init__.py` (Redshift rides on Ibis postgres)
- Create: `docs/superpowers/specs/2026-04-15-settings-audit/redshift.md`

**Source URLs (none in docstring — use canonical):**
- Driver (authoritative): https://docs.aws.amazon.com/redshift/latest/mgmt/python-redshift-driver.html
- Driver connection params: https://github.com/aws/amazon-redshift-python-driver#basic-example
- libpq (supplementary, since most settings reuse postgres): https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
- Ibis passthrough: via Ibis postgres

Header must note: "Redshift has no dedicated Ibis backend; passthrough tracked via Ibis postgres. Source URLs absent from settings class docstring — recommend backfilling in a follow-up."

- [ ] **Step 1: Read `src/mountainash_data/core/settings/redshift.py`**
- [ ] **Step 2: WebFetch all three URLs; extract `redshift_connector.connect()` parameters + libpq overlap**
- [ ] **Step 3: Read Ibis postgres `do_connect()` signature**
- [ ] **Step 4: Build union, classify. "Ibis passthrough" column values: `via postgres ✓` or `via postgres ✗`. Flag Redshift-only params (IAM auth, cluster identifier, region) that libpq/postgres backend doesn't support**
- [ ] **Step 5: Write report to `docs/superpowers/specs/2026-04-15-settings-audit/redshift.md`**
- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/redshift.md
git commit -m "docs(audit): redshift settings audit"
```

---

## Task 10: PySpark settings audit

**Files:**
- Read: `src/mountainash_data/core/settings/pyspark.py`
- Read: `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/pyspark/__init__.py`
- Create: `docs/superpowers/specs/2026-04-15-settings-audit/pyspark.md`

**Source URLs:**
- Driver (authoritative, selective — only Spark properties we actually expose): https://spark.apache.org/docs/3.5.1/configuration.html#available-properties
- Databricks (context): https://docs.databricks.com/en/spark/conf.html
- Ibis passthrough (authoritative): local file above

Per Q3/D, PySpark is the clearest "advanced" case — Spark has hundreds of config properties. Scope this audit to:
1. Parameters we actually declare as Fields
2. Ibis pyspark `do_connect()` kwargs
3. Any Spark property we reference in the settings class body (grep for `spark.` prefixes)

Do NOT audit the full Spark configuration reference; note this scoping decision in the report header.

- [ ] **Step 1: Read `src/mountainash_data/core/settings/pyspark.py`; grep for `spark.*` property strings inside the class**
- [ ] **Step 2: WebFetch both URLs; cross-reference only the Spark properties we declare or reference**
- [ ] **Step 3: Read Ibis pyspark `do_connect()` signature**
- [ ] **Step 4: Build restricted union (our Fields ∪ ibis passthrough ∪ referenced spark.* keys), classify**
- [ ] **Step 5: Write report to `docs/superpowers/specs/2026-04-15-settings-audit/pyspark.md`. Include explicit "Scoping" paragraph in header noting the exhaustive Spark property set is out of scope**
- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/pyspark.md
git commit -m "docs(audit): pyspark settings audit"
```

---

## Task 11: Trino settings audit

**Files:**
- Read: `src/mountainash_data/core/settings/trino.py`
- Read: `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/trino/__init__.py`
- Create: `docs/superpowers/specs/2026-04-15-settings-audit/trino.md`

**Source URLs:**
- Driver (authoritative): https://github.com/trinodb/trino-python-client/blob/master/trino/dbapi.py
- Driver user guide (supplementary): https://github.com/trinodb/trino-python-client#connection-parameters
- Ibis passthrough: local file above

- [ ] **Step 1: Read `src/mountainash_data/core/settings/trino.py`**
- [ ] **Step 2: WebFetch both URLs; extract `trino.dbapi.connect()` parameter list**
- [ ] **Step 3: Read Ibis trino `do_connect()` signature**
- [ ] **Step 4: Build union, classify**
- [ ] **Step 5: Write report to `docs/superpowers/specs/2026-04-15-settings-audit/trino.md`**
- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/trino.md
git commit -m "docs(audit): trino settings audit"
```

---

## Task 12: PyIceberg REST settings audit

**Files:**
- Read: `src/mountainash_data/core/settings/pyiceberg_rest.py`
- Create: `docs/superpowers/specs/2026-04-15-settings-audit/pyiceberg_rest.md`

**Source URLs:**
- Driver (authoritative): https://py.iceberg.apache.org/configuration/
- REST catalog spec (authoritative, supplementary): https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
- R2 (context, since code has R2-specific branch): https://developers.cloudflare.com/r2/data-catalog/config-examples/pyiceberg/
- Ibis passthrough: **N/A** — PyIceberg is not an Ibis backend. Passthrough column should be `N/A` throughout; instead add a column or note for "PyIceberg REST catalog kwarg" matching against `pyiceberg.catalog.rest.RestCatalog` constructor.

Header must note: "No Ibis backend applies to PyIceberg. The `Ibis passthrough` column is repurposed as `PyIceberg RestCatalog kwarg`."

- [ ] **Step 1: Read `src/mountainash_data/core/settings/pyiceberg_rest.py` in full, including R2-specific branch in `_init_provider_specific`**
- [ ] **Step 2: WebFetch all three URLs; extract RestCatalog properties (`uri`, `token`, `credential`, `warehouse`, `s3.*`, `header.*`, signer options, etc.)**
- [ ] **Step 3: Skip Ibis read (not applicable); instead inspect PyIceberg source if installed (`python -c "from pyiceberg.catalog.rest import RestCatalog; import inspect; print(inspect.signature(RestCatalog.__init__))"`) or rely on docs**
- [ ] **Step 4: Build union, classify. Use the repurposed column**
- [ ] **Step 5: Write report to `docs/superpowers/specs/2026-04-15-settings-audit/pyiceberg_rest.md`**
- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/pyiceberg_rest.md
git commit -m "docs(audit): pyiceberg_rest settings audit"
```

---

## Task 13: Fill index summary counts

**Files:**
- Modify: `docs/superpowers/specs/2026-04-15-settings-audit/README.md` (index table rows)

- [ ] **Step 1: For each of the 11 backend reports, read the "Summary counts" section**

Run: Read each `docs/superpowers/specs/2026-04-15-settings-audit/<backend>.md`. Extract the 6 count values (core missing, core mismatch, advanced missing, advanced mismatch, extra, stale links).

- [ ] **Step 2: Update the index table**

Edit `docs/superpowers/specs/2026-04-15-settings-audit/README.md`. Replace every `—` in the index table with the actual count from the corresponding report. Leave backend names and report links unchanged.

- [ ] **Step 3: Verify the table**

Run: Read the updated README. Confirm all 11 rows have numeric values in every count column and that the backend/report columns are unchanged.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-settings-audit/README.md
git commit -m "docs(audit): fill audit index summary counts"
```

---

## Self-review notes (completed during plan authoring)

- **Spec coverage:** Each of the 11 backends listed in the spec scope has a dedicated task (Tasks 1–12, minus pyiceberg numbering); final index task closes the loop on the spec's README index table.
- **Placeholders:** None. Every task has concrete file paths, URLs, and acceptance criteria. The one `TBD`-like element — index counts — is explicitly populated by Task 13.
- **Type/naming consistency:** Report schema (6 sections, 8-column table) is defined once in shared context and referenced identically from every task. Status/Tier vocabularies are identical across tasks.
- **Noted caveats encoded as task instructions:** motherduck/redshift missing source URLs (Tasks 3, 9 add canonical URLs and flag backfill); pyspark scoping (Task 10); pyiceberg_rest not-an-Ibis-backend (Task 12).
