# PySpark Settings Audit

## Header

- **Backend:** pyspark
- **Date checked:** 2026-04-15
- **Our settings class:** `src/mountainash_data/core/settings/pyspark.py` (`PySparkAuthSettings`)
- **Ibis backend file:** `/home/nathanielramm/git/github/ibis-dev/ibis/ibis/backends/pyspark/__init__.py` — `do_connect(session=None, mode="batch", **kwargs)` (kwargs → `SparkSession.builder.config(**kwargs)`)
- **Scope restriction:** The Spark configuration surface is intentionally out-of-scope for this audit — the docstring already says "Too many options to set. Configure your spark instance directly!" We audit only the declared fields, the Ibis passthrough (`session`, `mode`, `**kwargs`), and spark.* keys explicitly referenced by this class.
- **Spec URLs (precedence-tagged):**
  - **Driver (authoritative, scoped):** Ibis pyspark backend (file above); the only direct kwargs are `session` and `mode`.
  - **Databricks options (context):** https://docs.databricks.com/en/spark/conf.html
  - **Spark config reference (out of scope):** https://spark.apache.org/docs/3.5.1/configuration.html#available-properties

## Stale-link check

| URL | Status |
|---|---|
| https://docs.databricks.com/en/spark/conf.html | OK (assumed) |
| https://spark.apache.org/docs/3.5.1/configuration.html | OK (assumed; version-pinned URL — worth bumping to `latest`) |

## Summary counts

- Core missing: **1** (`session` — the canonical way to pass a caller-built SparkSession is absent)
- Core mismatch: **3** (`MODE` default `None` vs Ibis default `"batch"`; `MODE` stringly-typed despite nominal `PySparkMode` class; `PySparkMode` is a plain class, not a `StrEnum`)
- Advanced missing: **All `spark.*` config keys** (by declared scope; this is deliberate)
- Advanced mismatch: **3** (`PARTITIONS` typed `int` but default is `{}` — a dict; `APPLICATION_NAME`/`SPARK_MASTER`/`WAREHOUSE_DIR` typed `str` with default `None` — not Optional; `AUTH_METHOD` is a string literal `"none"` where other classes use `CONST_DB_AUTH_METHOD`)
- Extra: **8** (all inherited base-class fields — HOST, PORT, DATABASE, SCHEMA, USERNAME, PASSWORD, TOKEN + docstring mislabels class as "SQLite authentication settings")
- Total audited: **~12**
- Stale links: **0**

## Parameter table

Legend — Status: `present` / `missing` / `mismatch` / `extra`. Tier: `core` / `advanced`. Type/Default: ✓ / ✗ / N/A. Ibis passthrough: ✓ / ✗ / unknown.

| Spec name | Our field | Status | Tier | Type ✓ | Default ✓ | Ibis passthrough | Notes |
|---|---|---|---|---|---|---|---|
| `session` (SparkSession) | — | missing | core | N/A | N/A | ✓ (direct Ibis kwarg) | Ibis can accept a pre-built SparkSession. Not exposed — all configuration flows via builder-config kwargs only. |
| `mode` (`batch`/`streaming`) | `MODE` (str, default `None`) | mismatch | core | ✗ | ✗ | ✓ | Ibis default is `"batch"`; ours `None`. `PySparkMode` exists as a plain class (not Enum/StrEnum) with `BATCH`/`STREAMING` constants but **isn't used as the field type**. |
| `spark.app.name` | `APPLICATION_NAME` (str, default `None`) | present | advanced | ✗ | ✓ | ✓ (via **kwargs) | Type is non-Optional `str` with `None` default — pydantic accepts but annotation lies. Plumbed into connection-string params as `spark_app_name`; **not plumbed into `get_connection_kwargs()`**. |
| `spark.master` | `SPARK_MASTER` (str, default `None`) | present | advanced | ✗ | ✓ | ✓ (via **kwargs) | Same type-annotation issue. **Not plumbed into `get_connection_kwargs()`** — only into connection string. |
| `spark.sql.warehouse.dir` | `WAREHOUSE_DIR` (str, default `None`) | present | advanced | ✗ | ✓ | ✓ (via **kwargs) | Same type-annotation issue. **Not plumbed into `get_connection_kwargs()`**. |
| `spark.sql.shuffle.partitions` | `PARTITIONS` (int, default `{}`) | mismatch | advanced | ✗ | ✗ | ✓ (post-connect via `spark.conf.set`) | **Type/default mismatch**: annotation is `int` but default is `{}` (dict). Pydantic will error at model instantiation unless `PARTITIONS` is explicitly provided. Plumbed via `get_post_connection_options()`. |
| — | `AUTH_METHOD` (default `"none"` literal) | extra | core | N/A | N/A | ✗ | String literal where other classes use `CONST_DB_AUTH_METHOD`. No auth concept for Spark anyway. |
| — | `HOST`, `PORT`, `DATABASE`, `SCHEMA`, `USERNAME`, `PASSWORD`, `TOKEN` (base) | extra | N/A | N/A | N/A | ✗ | Inherited but meaningless for PySpark. |

## Findings narrative

**Core gaps:** The `session` injection path is missing. For any non-trivial Spark deployment (Databricks, EMR, existing Spark context), callers construct their own `SparkSession` — and then Ibis just wraps it. Our class forces builder-config creation, which doesn't cover the common case.

**Core mismatches:**

1. **`MODE` default drift.** Ibis defaults to `"batch"`; ours defaults to `None`. When `None`, `get_connection_kwargs()` drops the key, and Ibis's own default kicks in — so this works, but the declared default is misleading.
2. **`MODE` stringly-typed.** `PySparkMode` is a plain class (not `Enum`/`StrEnum`) with constants `BATCH = "batch"` and `STREAMING = "streaming"`. It's not enforced as the field type, and it isn't importable as an Enum for consumers.
3. **Docstring mislabels the class** as "SQLite authentication settings" (line 18 — copy-paste from `sqlite.py`). Also the file comment on line 1 says `#path: .../file/sqlite.py`.

**Core bugs:**

1. **`PARTITIONS: int = Field(default={})`** — type says `int`, default is `{}`. Pydantic will raise a validation error unless someone sets `PARTITIONS` to an int at construction time. Additionally, `if self.PARTITIONS:` treats an empty dict and `0` identically as falsy, then emits `spark.sql.shuffle.partitions = <int>` — passing an int to a config key that expects a string may or may not be accepted by `SparkSession.conf.set`.
2. **Non-Optional fields with `None` defaults** (`APPLICATION_NAME`, `SPARK_MASTER`, `WAREHOUSE_DIR`, `MODE`). Pydantic v2 is lenient but this produces wrong schemas and IDE hints.

**Advanced gaps:** Out of scope by declaration — the docstring punts to direct Spark configuration. Reasonable.

**Advanced mismatches:** `get_connection_kwargs()` only emits `mode`. `SPARK_MASTER`, `APPLICATION_NAME`, `WAREHOUSE_DIR` are declared and plumbed into `get_connection_string_params()` but never into the kwargs that actually reach Ibis — so they don't configure the SparkSession unless the connection-string builder forwards them (behavior unclear). Meanwhile the canonical `SparkSession.builder.config()` kwargs path uses dotted keys like `spark.app.name` — our params use `spark_app_name`, so even if forwarded, the key names are wrong.

**Extras:** Eight inherited base-class fields that don't apply to PySpark plus the docstring/path-comment mislabeling.

**Stale links:** The Spark docs link is version-pinned to 3.5.1 — fine if that's the target, but bump to `latest` or document the pin.

## Recommended follow-ups

- **Fix the docstring** (line 18) — says "SQLite authentication settings". Replace with a correct PySpark description. Fix the `#path:` comment on line 1.
- **Add `SESSION`** field (`Optional[Any]` typed as `SparkSession`) to support the Ibis `session=` injection path.
- **Retype `MODE`** as `Optional[Literal["batch", "streaming"]]` or a proper `StrEnum`. Default to `"batch"` to match Ibis.
- **Convert `PySparkMode` to `StrEnum`** and use it as the type.
- **Fix `PARTITIONS` type/default mismatch**: either `Optional[int] = Field(default=None)` or `Optional[Dict[str, Any]] = Field(default=None)` — pick one and match the annotation.
- **Retype the non-Optional-with-None-default fields** (`APPLICATION_NAME`, `SPARK_MASTER`, `WAREHOUSE_DIR`) → `Optional[str]`.
- **Fix spark.* key naming in `get_connection_kwargs()`**: emit `spark.app.name`, `spark.master`, `spark.sql.warehouse.dir` rather than `spark_app_name` etc.
- **Decide whether settings-driven builder config is worth keeping** given the docstring's own advice to configure Spark directly. If not, collapse the class down to `{session, mode}` + optional `partitions` post-connect and direct users to supply a pre-built SparkSession for everything else.
- **Drop `AUTH_METHOD` default literal** — Spark has no auth concept at this layer.
