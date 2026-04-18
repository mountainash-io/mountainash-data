# BigQuery Settings Audit

## Header

- **Backend:** bigquery
- **Date checked:** 2026-04-15
- **Our settings class:** `src/mountainash_data/core/settings/bigquery.py` (`BigQueryAuthSettings`)
- **Ibis backend file:** `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/bigquery/__init__.py` — `do_connect(project_id=None, dataset_id="", credentials=None, application_name=None, auth_local_webserver=True, auth_external_data=False, auth_cache="default", partition_column="PARTITIONTIME", client=None, storage_client=None, location=None, generate_job_id_prefix=None)`
- **Spec URLs (precedence-tagged):**
  - **Ibis backend (authoritative for kwargs):** https://ibis-project.org/backends/bigquery
  - **Driver (google-cloud-bigquery Client):** https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client
  - **Auth options:** https://cloud.google.com/sdk/docs/authorizing

## Stale-link check

| URL | Status |
|---|---|
| https://ibis-project.org/backends/bigquery | OK (assumed) |
| https://cloud.google.com/sdk/docs/authorizing | OK (assumed) |
| https://cloud.google.com/bigquery/external-data-sources | OK (assumed) |

## Summary counts

- Core missing: **4** (`auth_local_webserver`, `auth_external_data`, `auth_cache`, `credentials` is typed wrong)
- Core mismatch: **3** (`SERVICE_ACCOUNT_INFO` → `credentials`: Ibis expects `google.auth.credentials.Credentials` object, not a dict; `partition_column` default drift; `project_id` length validator is too restrictive)
- Advanced missing: **3** (`client`, `storage_client`, `generate_job_id_prefix`; plus `client_info`, `default_query_job_config` from the underlying `bq.Client`)
- Advanced mismatch: **0**
- Extra: **8** (base-class HOST, PORT, USERNAME, PASSWORD, TOKEN, SCHEMA, DATABASE, AUTH_METHOD — all meaningless for BigQuery's OAuth/SA-key model)
- Total audited: **~15**
- Stale links: **0**

## Parameter table

Legend — Status: `present` / `missing` / `mismatch` / `extra`. Tier: `core` / `advanced`. Type/Default: ✓ / ✗ / N/A. Ibis passthrough: ✓ / ✗ / unknown.

| Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes |
|---|---|---|---|---|---|---|---|
| `project_id` (str) | `PROJECT_ID` (str, required) | present | core | ✓ | ✗ | ✓ | Ibis default is None; ours required. Validator enforces 6-30 chars — GCP project IDs are 6-30 chars, so OK, but a hyphen-terminated ID could slip past. |
| `dataset_id` (str, `""`) | `DATASET_ID` (Optional[str]) | present | core | ✓ | ✗ | ✓ | Ibis default `""`; ours None. Minor. **Not plumbed into `get_connection_kwargs()`** — only into the connection-string template. |
| `credentials` (google.auth.credentials.Credentials) | `SERVICE_ACCOUNT_INFO` (Optional[Dict]) | mismatch | core | ✗ | ✓ | ✓ | **Type wrong.** Ibis expects a `Credentials` instance; we pass a dict. The dict would need to be converted via `google.oauth2.service_account.Credentials.from_service_account_info(...)` before being passed. Currently `args["credentials"] = self.SERVICE_ACCOUNT_INFO` passes the raw dict — Ibis will fail or silently use ADC. |
| `application_name` (str) | `APPLICATION_NAME` (Optional[str]) | present | advanced | ✓ | ✓ | ✓ | Plumbed. |
| `auth_local_webserver` (bool, True) | — | missing | core | N/A | N/A | ✓ | Controls interactive auth flow. |
| `auth_external_data` (bool, False) | — | missing | core | N/A | N/A | ✓ | Needed for Sheets/Drive/GCS external tables. |
| `auth_cache` (`default`/`reauth`/`none`) | — | missing | core | N/A | N/A | ✓ | Interactive auth cache control. |
| `partition_column` (str, `"PARTITIONTIME"`) | `PARTITION_COLUMN` (Optional[str]) | present | advanced | ✓ | ✗ | ✓ | Ibis default `"PARTITIONTIME"`; ours None (loses the default). Plumbed when set. |
| `client` (bq.Client) | — | missing | advanced | N/A | N/A | ✓ | Caller-constructed client injection. |
| `storage_client` (bqstorage.BigQueryReadClient) | — | missing | advanced | N/A | N/A | ✓ | Used for Storage API fast reads. |
| `location` (str) | `LOCATION` (Optional[str]) | present | advanced | ✓ | ✓ | ✓ | Plumbed. |
| `generate_job_id_prefix` (Callable) | — | missing | advanced | N/A | N/A | ✓ | |
| (not in Ibis; caller would set via bq.Client) | `MAXIMUM_BYTES_BILLED` | — (commented out) | missing | advanced | N/A | N/A | ✗ | Would need `default_query_job_config` wrapper. |
| (not in Ibis) | `API_ENDPOINT` | — (commented out) | missing | advanced | N/A | N/A | ✗ | For private/regional endpoints. |
| — | `HOST`, `PORT`, `USERNAME`, `PASSWORD`, `TOKEN`, `SCHEMA`, `DATABASE`, `AUTH_METHOD` (base) | extra | N/A | N/A | N/A | ✗ | All irrelevant to BigQuery. |

## Findings narrative

**Core gaps:** Three auth flow controls (`auth_local_webserver`, `auth_external_data`, `auth_cache`) and the missing `credentials` wiring. The `auth_external_data=True` case is specifically noted in our docstring (`External data sources: https://cloud.google.com/bigquery/external-data-sources`) yet isn't exposed. `auth_cache="none"` is important for CI environments.

**Core mismatches:**

1. **`SERVICE_ACCOUNT_INFO` is a dict, but Ibis `credentials` expects `google.auth.credentials.Credentials`.** The current code passes the raw dict as `credentials=`, which either raises inside Ibis or is silently ignored (falling back to Application Default Credentials). The settings layer needs a validator/adapter that converts SA info → `service_account.Credentials`. Alternative: take a file path (`SERVICE_ACCOUNT_FILE`, already commented out) and use `from_service_account_file()`.
2. **`partition_column` default drift.** Ours defaults to `None` (no emission); Ibis defaults to `"PARTITIONTIME"`. Users relying on the default partition column are fine (Ibis supplies it), but users who *want* `None` (i.e., no partition filter) can't express that distinctly from "use default".
3. **`validate_project_id` 6-30 char bound** matches GCP's current rules but doesn't validate characters (lowercase, digits, hyphens; not starting or ending with a hyphen). A trailing hyphen passes.

**Advanced gaps:** `client` and `storage_client` injection points are missing — useful for tests and advanced users. `generate_job_id_prefix` and config like `MAXIMUM_BYTES_BILLED` / `default_query_job_config` aren't supported.

**Advanced mismatches:** None.

**Extras:** Standard base-class composition issue — eight inherited fields irrelevant to BigQuery.

**Stale links:** None.

## Recommended follow-ups

- **Fix credentials plumbing.** Either (a) add `SERVICE_ACCOUNT_FILE: Optional[str]` and convert to `Credentials` in `get_connection_kwargs()`, or (b) convert `SERVICE_ACCOUNT_INFO` dict → `Credentials` via `from_service_account_info()` before emitting. Currently the credentials path is broken.
- **Add `AUTH_LOCAL_WEBSERVER`, `AUTH_EXTERNAL_DATA`, `AUTH_CACHE`** Fields. These are documented Ibis kwargs for interactive OAuth flows.
- **Set `PARTITION_COLUMN` default** to `"PARTITIONTIME"` to match Ibis behavior, or document the None-means-no-override intent.
- **Tighten `validate_project_id`** with a regex matching `^[a-z][a-z0-9-]{4,28}[a-z0-9]$`.
- **Consider adding `CLIENT` / `STORAGE_CLIENT` escape hatches** for advanced users (typed `Any`).
- **Plumb `DATASET_ID` into `get_connection_kwargs()`** or document that it's intended to flow only via the connection string.
- **Restore `MAXIMUM_BYTES_BILLED` / `DEFAULT_QUERY_JOB_CONFIG`** and document that they're applied post-connect by wrapping `conn.client.default_query_job_config`.
- **Drop or validate base-class fields** — same composition issue as other cloud backends.
- **Check `db_provider_type`** — correctly returns `BIGQUERY` here (cf. postgresql.py and mysql.py where it's wrong).
