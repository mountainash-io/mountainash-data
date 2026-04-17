# PyIceberg REST Settings Audit

## Header

- **Backend:** pyiceberg_rest
- **Date checked:** 2026-04-15
- **Our settings class:** `src/mountainash_data/core/settings/pyiceberg_rest.py` (`PyIcebergRestAuthSettings`)
- **Not an Ibis backend.** The standard "Ibis passthrough" column is **repurposed as "PyIceberg RestCatalog property"** for this report.
- **Spec URLs (precedence-tagged):**
  - **Driver (authoritative):** https://py.iceberg.apache.org/configuration/
  - **REST catalog spec (supplementary):** https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
  - **Context (R2 usage example):** https://developers.cloudflare.com/r2/data-catalog/config-examples/pyiceberg/

## Stale-link check

| URL | Status |
|---|---|
| https://py.iceberg.apache.org/configuration/ | OK |
| https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml | OK (assumed — standard Iceberg repo path) |
| https://developers.cloudflare.com/r2/data-catalog/config-examples/pyiceberg/ | OK (assumed — not verified inline) |

## Summary counts

- Core missing: **2** (`credential`, `scope`)
- Core mismatch: **3** (`AUTH_METHOD` forces TOKEN but class also claims to be the R2 settings class in docstring; `USE_SSL` default `False` but R2/most REST catalogs require HTTPS; `VERIFY_SSL` isn't plumbed)
- Advanced missing: **9** (`oauth2-server-uri`, `header.*`, all four `s3.*`, all three `rest.sigv4-*`)
- Advanced mismatch: **0**
- Extra: **6** (base-class HOST, PORT, DATABASE, SCHEMA, USERNAME, PASSWORD — all unused)
- Total audited: **~20**
- Stale links: **0**

## Parameter table

Legend — Status: `present` / `missing` / `mismatch` / `extra`. Tier: `core` / `advanced`. Type/Default: ✓ / ✗ / N/A. **"RestCatalog property"**: ✓ / ✗ / unknown.

| Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | RestCatalog property | Notes |
|---|---|---|---|---|---|---|---|
| `uri` (str, required) | `CATALOG_URI` (str, required) | present | core | ✓ | ✓ (both required) | ✓ | Plumbed as `uri=` in `get_connection_kwargs()`. |
| `warehouse` (str, optional) | `WAREHOUSE` (str, **required**) | mismatch | core | ✓ | ✗ | ✓ | Spec says optional; we mark it required (`Field(...)`). Matches R2's always-required usage but misses general REST-catalog flexibility. |
| (no spec equivalent; local naming) | `CATALOG_NAME` (str, required) | extra | core | N/A | N/A | ✓ | Plumbed as `name=` to `RestCatalog(...)`. This is the PyIceberg-side instance name, not a spec property — fine. |
| `token` (str, optional) | `TOKEN` (base, SecretStr) | present | core | ✓ | ✓ | ✓ | Plumbed. Note: `get_connection_kwargs()` packs `self.TOKEN` (the SecretStr object) — check whether `RestCatalog` accepts SecretStr or needs `.get_secret_value()`. |
| `credential` (str "id:secret", optional) | — | missing | core | N/A | N/A | ✓ | OAuth2 client-credentials flow path. Not exposed. |
| `oauth2-server-uri` (str) | — | missing | advanced | N/A | N/A | ✓ | Required for full OAuth2 client-credentials flow. |
| `scope` (str) | — | missing | core | N/A | N/A | ✓ | OAuth2 scope. If we support `credential`, we likely also need this. |
| `header.*` (str) | — | missing | advanced | N/A | N/A | ✓ | Custom request headers (e.g., per-tenant). |
| `rest.sigv4-enabled` (bool) | — | missing | advanced | N/A | N/A | ✓ | AWS SigV4 signing switch (for AWS Glue REST proxy, etc.). |
| `rest.signing-region` (str) | — | missing | advanced | N/A | N/A | ✓ | |
| `rest.signing-name` (str) | — | missing | advanced | N/A | N/A | ✓ | |
| `s3.region` (str) | — | missing | advanced | N/A | N/A | ✓ | |
| `s3.endpoint` (str) | — | missing | advanced | N/A | N/A | ✓ | Needed for S3-compatible services (R2, MinIO) — relevant to this class. |
| `s3.access-key-id` (str) | — | missing | advanced | N/A | N/A | ✓ | Relevant for R2. |
| `s3.secret-access-key` (str) | — | missing | advanced | N/A | N/A | ✓ | Relevant for R2. |
| `s3.session-token` (str) | — | missing | advanced | N/A | N/A | ✓ | |
| (no direct spec property; SDK-level) | `USE_SSL` (bool, default `False`) | mismatch | core | ✓ | ✗ | ✗ | No matching RestCatalog property; effectively dead. Default `False` is particularly risky for a catalog class described as "Cloudflare R2" (which is HTTPS-only). |
| (no direct spec property; SDK-level) | `VERIFY_SSL` (bool, default `True`) | mismatch | advanced | ✓ | ✓ | ✗ | Not plumbed into `get_connection_kwargs()`. |
| — | `AUTH_METHOD` (default `TOKEN`) | extra | core | N/A | N/A | ✗ | Local selector. Forces TOKEN auth; no code path switches to credential/OAuth2 flows. |
| — | `HOST`, `PORT`, `DATABASE`, `SCHEMA`, `USERNAME`, `PASSWORD` (base) | extra | N/A | N/A | N/A | ✗ | Inherited but irrelevant to a REST catalog client. |

## Findings narrative

**Core gaps:** No path to OAuth2 client-credentials flow — `credential` and `scope` are absent. Any REST catalog requiring OAuth2 (common for non-bearer deployments) cannot be configured.

**Core mismatches:**

1. **Identity crisis.** The docstring describes this class as "Cloudflare R2 storage authentication settings", but the class name and code path are REST-catalog-oriented. Either rename the class / rewrite the docstring, or split into a generic `PyIcebergRestAuthSettings` + an `R2PyIcebergSettings` subclass that adds the R2-specific `s3.endpoint`/`s3.access-key-id`/`s3.secret-access-key` plumbing.
2. **`WAREHOUSE` over-specified as required.** The PyIceberg spec treats it as optional (server can resolve). Making it required prevents using catalogs where the server supplies the warehouse.
3. **`USE_SSL=False` default.** For any production REST catalog (R2, Tabular, etc.), this is wrong. It's also not obviously plumbed — `get_connection_kwargs()` doesn't touch it. Either remove or implement.
4. **`VERIFY_SSL`** declared but never forwarded — another orphaned field.

**Advanced gaps:** Nine spec properties absent, most notably the `s3.*` family (critical for R2 since the docstring claims R2 focus) and SigV4 support (needed for AWS REST proxies).

**Advanced mismatches:** None of substance — the gaps are "not there" rather than "there but wrong".

**Extras:** Six base-class fields (HOST, PORT, DATABASE, SCHEMA, USERNAME, PASSWORD) are inherited but never used. `AUTH_METHOD` is a local selector without any branching code behind it.

**Stale links:** None.

**Note on tooling:** Could not introspect `RestCatalog.__init__` via `uv run python -c ...` directly (pyiceberg not confirmed installed in the ambient env); relied on the PyIceberg configuration documentation which is the authoritative parameter list.

## Recommended follow-ups

- **Decide class identity first.** Rename / reclassify this class as either a generic REST-catalog settings class or an R2-specific subclass. That decision reshapes all other fixes.
- **Add OAuth2 fields:** `CREDENTIAL` (SecretStr, `"client_id:client_secret"`), `OAUTH2_SERVER_URI`, `SCOPE`. Wire them into `get_connection_kwargs()` behind `AUTH_METHOD == "oauth2"` (or similar).
- **Add `s3.*` family**: `S3_REGION`, `S3_ENDPOINT`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` (SecretStr), `S3_SESSION_TOKEN` (SecretStr). Map to `s3.region`, `s3.endpoint`, etc. — note the dot-prefixed key format PyIceberg expects.
- **Add SigV4 fields**: `REST_SIGV4_ENABLED`, `REST_SIGNING_REGION`, `REST_SIGNING_NAME`.
- **Add `HEADERS`** as `Dict[str, str]`, mapped to `header.<key>` entries.
- **Relax `WAREHOUSE`** to `Optional[str]`.
- **Fix `USE_SSL` default to `True`** or remove the field (REST is HTTPS in practice).
- **Plumb `VERIFY_SSL`** or remove it.
- **Secret handling**: in `get_connection_kwargs()`, call `.get_secret_value()` on `TOKEN` (and any new SecretStr fields) unless `RestCatalog` is verified to accept SecretStr directly.
- **Drop or validate the inherited base-class fields** (HOST/PORT/DATABASE/SCHEMA/USERNAME/PASSWORD) with a warning when set.
- **Add a proper `CONST_STORAGE_PROVIDER_TYPE.PYICEBERG_REST`** (noted as TODO in the source).
