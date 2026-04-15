# Redshift Settings Audit

## Header

- **Backend:** redshift
- **Date checked:** 2026-04-15
- **Our settings class:** `src/mountainash_data/core/settings/redshift.py` (`RedshiftAuthSettings`)
- **Ibis backend file:** `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/postgres/__init__.py` — Redshift rides the **postgres** Ibis backend (Redshift speaks the PostgreSQL wire protocol). `do_connect(host, user, password, port=5432, database, schema, autocommit=True, **kwargs)`.
- **Spec URLs (precedence-tagged):**
  - **Driver (authoritative):** redshift_connector (Amazon) — https://github.com/aws/amazon-redshift-python-driver (also callable via psycopg/libpq for plain wire-protocol mode)
  - **libpq (via Ibis postgres):** https://www.postgresql.org/docs/current/libpq-connect.html
  - **AWS Redshift connection params:** https://docs.aws.amazon.com/redshift/latest/mgmt/python-configuration-options.html

## Stale-link check

| URL | Status |
|---|---|
| https://github.com/aws/amazon-redshift-python-driver | OK (assumed) |
| https://docs.aws.amazon.com/redshift/latest/mgmt/python-configuration-options.html | OK (assumed) |
| https://www.postgresql.org/docs/current/libpq-connect.html | OK (verified in postgresql audit) |

## Summary counts

- Core missing: **1** (`HOST` — set on base but never populated; Redshift endpoint must be resolved from `CLUSTER_IDENTIFIER`/`WORKGROUP_NAME`, and the resolution code is commented out)
- Core mismatch: **5** (`get_connection_kwargs()` returns `{}`; `SSL` bool doesn't map to libpq `sslmode` enum; `FORCE_IAM`/IAM path targets redshift_connector kwargs but connection goes via postgres dialect; AWS region regex rejects 4-digit suffixes like `us-west-2`; serverless path unimplemented)
- Advanced missing: **~15** (cluster auto-resolve, IAM token-exchange, GetClusterCredentials, db_user, db_groups, auto_create, group_federation, connect_retry_count, connect_retry_delay, ssl_insecure, iam_disable_cache, application_name, statement_timeout, most timeouts)
- Advanced mismatch: **2** (`AUTO_CREATE` declared, not plumbed; `PROFILE_NAME` declared, not plumbed; `WORKGROUP_NAME` used only in validator, not plumbed)
- Extra: **3** (base-class `TOKEN` unused; `CLUSTER_READ_ONLY` encoded into connection-string params only; `ENDPOINT_URL` only used in commented boto3 code)
- Total audited: **~25**
- Stale links: **0**

## Parameter table

Legend — Status: `present` / `missing` / `mismatch` / `extra`. Tier: `core` / `advanced`. Type/Default: ✓ / ✗ / N/A. Ibis passthrough (via postgres): ✓ / ✗ / unknown.

| Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes |
|---|---|---|---|---|---|---|---|
| `host` (endpoint) | `HOST` (base) | mismatch | core | ✓ | ✗ | ✓ (via postgres) | Redshift endpoints are `<cluster>.<id>.<region>.redshift.amazonaws.com` — normally resolved from `CLUSTER_IDENTIFIER` + `REGION` via `boto3.redshift.describe_clusters()`. The resolver is **commented out** (`_get_cluster_endpoint`). Users must supply HOST manually, defeating the cluster-identifier abstraction. |
| `port` | `PORT` (override, default `5439`) | present | core | ✓ | ✓ | ✓ | Matches Redshift default (5439, not 5432). |
| `dbname` | `DATABASE` (base) | present | core | ✓ | ✓ | ✓ | |
| `user` | `USERNAME` (base) | present | core | ✓ | ✓ | ✓ | |
| `password` | `PASSWORD` (base, SecretStr) | present | core | ✓ | ✓ | ✓ | |
| `region` | `REGION` (str, required) | present | core | ✓ | ✓ | ✗ | Validator regex `^[a-z]{2}-[a-z]+-\d{1}$` — accepts `us-east-1` but **rejects future 2-digit region suffixes** (AWS has used 2-digit suffixes in some partitions). Also doesn't include region partitions like `us-gov-west-1` (has a hyphen in the middle segment — passes regex but the `{2}-[a-z]+-\d` shape is lucky, not robust). |
| `cluster_identifier` | `CLUSTER_IDENTIFIER` (Optional[str]) | present | core | ✓ | ✓ | ✗ | Used only by the commented-out boto3 resolver. Not plumbed to driver. |
| `iam` (bool) | — (derived from `AUTH_METHOD == IAM` or `FORCE_IAM`) | present | core | ✓ | ✓ | ✓ (libpq `iam=true`) | Encoded into connection string. |
| `iam_role_arn` (assume-role ARN) | `IAM_ROLE_ARN` (Optional[str]) | present | core | ✓ | ✓ | ✓ | Plumbed via `get_connection_string_params()` as `iam_role_arn=`. Validator enforces `arn:aws:iam::` prefix — fine for commercial partition, rejects `arn:aws-us-gov:iam::` or `arn:aws-cn:iam::`. |
| `aws_access_key_id` | `ACCESS_KEY_ID` (Optional[str]) | present | core | ✓ | ✓ | ✓ | Plumbed when IAM path + keys set. |
| `aws_secret_access_key` | `SECRET_ACCESS_KEY` (Optional[SecretStr]) | present | core | ✓ | ✓ | ✓ | Plumbed; SecretStr not unwrapped. |
| `aws_session_token` | `SESSION_TOKEN` (Optional[SecretStr]) | present | core | ✓ | ✓ | ✓ | Plumbed; SecretStr not unwrapped. |
| `sslmode` (libpq) / `ssl` (bool) | `SSL` (bool, default `True`) | mismatch | core | ✓ | ✓ | ✓ | Bool → connection-string `sslmode=verify-full` (hardcoded). Should expose `sslmode` enum to support `require` / `prefer` modes. |
| `serverless` / `workgroup_name` | `SERVERLESS`, `WORKGROUP_NAME` | present | core | ✓ | ✓ | ✗ | Serverless endpoint resolver is **commented out and broken** (line 252 references undefined `client`). Cannot use serverless mode. |
| `profile_name` | `PROFILE_NAME` (Optional[str]) | present | advanced | ✓ | ✓ | ✗ | Only referenced in commented boto3 resolver. Not plumbed. |
| `auto_create` | `AUTO_CREATE` (bool, default `False`) | present | advanced | ✓ | ✓ | ✓ (redshift_connector kwarg) | **Not plumbed**. |
| `db_groups` | — | missing | advanced | N/A | N/A | ✓ | IAM-federated users group membership. |
| `db_user` | — | missing | advanced | N/A | N/A | ✓ | Target DB user for IAM auth. |
| `group_federation` | — | missing | advanced | N/A | N/A | ✓ | |
| `connect_retry_count` | — | missing | advanced | N/A | N/A | ✓ | |
| `connect_retry_delay` | — | missing | advanced | N/A | N/A | ✓ | |
| `ssl_insecure` | — | missing | advanced | N/A | N/A | ✓ | |
| `iam_disable_cache` | — | missing | advanced | N/A | N/A | ✓ | |
| `application_name` (libpq) | — | missing | advanced | N/A | N/A | ✓ | |
| `statement_timeout` | — | missing | advanced | N/A | N/A | ✓ | |
| (our custom) | `ENDPOINT_URL` (Optional[str]) | extra | advanced | ✓ | ✓ | ✗ | Only used in commented boto3 resolver (for custom endpoints / LocalStack). Dead outside resolver. |
| (our custom) | `CLUSTER_READ_ONLY` (bool) | extra | advanced | ✓ | ✓ | ✓ (as `readonly=true` query param) | Redshift supports `readonly` driver property; plumbed via query string. |
| — | `TOKEN` (base) | extra | N/A | N/A | N/A | ✗ | Not used for Redshift. |

## Findings narrative

**Core gaps:**

1. **Host resolution is broken.** Redshift's value prop over naked postgres is that you give it a cluster identifier + region and the settings class fetches the endpoint. That code (`_get_cluster_endpoint`, `_get_serverless_endpoint`) is commented out. Users currently must set `HOST` themselves, which means `CLUSTER_IDENTIFIER`, `WORKGROUP_NAME`, `SERVERLESS`, `ENDPOINT_URL`, `PROFILE_NAME`, and much of the AWS auth surface are only used for validation, not for actual connection.

**Core mismatches:**

1. **`get_connection_kwargs()` returns `{}`.** Everything routes via connection-string params, so any Ibis kwarg (e.g., `autocommit`) must be passed via the caller or via the URL query string. Aligns with postgres audit finding.
2. **`SSL` as bool** collapses the five libpq `sslmode` values to two. Redshift best-practice is `sslmode=verify-full` (which the code hardcodes), so this is defensible — but users needing `require` (e.g., with a self-signed cert for internal testing) can't express it.
3. **IAM / redshift_connector surface area mixed with postgres path.** The IAM path emits `aws_access_key_id` / `aws_secret_access_key` / `iam_role_arn` as top-level params — these are *redshift_connector* kwargs, not libpq. Since Ibis uses psycopg (postgres), these may be silently dropped at the driver boundary. Needs a plumbing test against the actual Ibis path.
4. **`validate_region` regex** `^[a-z]{2}-[a-z]+-\d{1}$` accidentally rejects GovCloud (`us-gov-west-1` — fails `[a-z]+` segment because of hyphens) and may reject future regions with 2-digit indices.
5. **Serverless path is broken.** `_get_serverless_endpoint` references an undefined `client` (line 252) with the boto3 lines commented out around it. Serverless mode validator passes if `WORKGROUP_NAME` is set, but there's no working code to resolve the endpoint.

**Advanced gaps:** The entire redshift_connector advanced surface (db_user, db_groups, group_federation, connect_retry_*, ssl_insecure, iam_disable_cache) is absent. If the intent is to run through Ibis's postgres backend, many of those don't apply anyway — but then `auto_create`, `profile_name`, `endpoint_url` don't apply either.

**Advanced mismatches:** Multiple orphan fields (`AUTO_CREATE`, `PROFILE_NAME`, `WORKGROUP_NAME` outside validator) that validate but do nothing.

**Extras:** `ENDPOINT_URL`, `CLUSTER_READ_ONLY`, `TOKEN` noted in table. The `_init_provider_specific` method is named differently from the `_post_init` convention used elsewhere — this method is never called by the base class (`_post_init` is the hook), so its validators are **dead code**. The serverless/provisioned coherence check doesn't fire.

**Stale links:** Docstring has **no source URLs**. Add them.

## Recommended follow-ups

- **Add source URLs to the class docstring** — it currently has none (unlike most sibling classes).
- **Decide the driver story first.** Either (a) route through Ibis postgres with plain libpq IAM support and drop the redshift_connector-specific fields, or (b) supply `driver_type="redshift_connector"` and route around Ibis. The current mix pretends to support both and delivers neither cleanly.
- **Rename `_init_provider_specific` → `_post_init`** so the validators actually fire. Currently `_post_init` is a no-op (`pass`) and the serverless/cluster checks never run.
- **Implement or remove `_get_cluster_endpoint`/`_get_serverless_endpoint`**. If keeping, add boto3 as a dependency and plumb the resolved HOST into the connection string. If not, rename fields to signal they're only labels.
- **Broaden `validate_region`** to accept GovCloud and other partitions: `^[a-z]{2,4}-[a-z-]+-\d{1,2}$` or use the AWS-published region list.
- **Broaden `validate_role_arn`** to accept partition variants (`arn:aws:`, `arn:aws-us-gov:`, `arn:aws-cn:`).
- **Plumb `AUTO_CREATE`**, **`PROFILE_NAME`**, **`WORKGROUP_NAME`** if the redshift_connector path is kept.
- **Replace `SSL: bool`** with `SSL_MODE: enum` (re-use `PostgresSSLCertMode` or similar) — hardcoded `verify-full` is an opinion, not a default.
- **Unwrap SecretStr** for `PASSWORD`, `SECRET_ACCESS_KEY`, `SESSION_TOKEN` at the kwargs boundary.
- **Remove unreachable extras** (`TOKEN` base field) or document why they're there.
- **Fix the broken `][p9]` comment** at line 129 (looks like an errant keystroke).
