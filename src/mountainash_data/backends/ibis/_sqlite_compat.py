"""Compatibility shim for Ibis's SQLite backend.

Ibis's SQLite backend (``ibis.backends.sqlite.Backend._register_in_memory_table``)
always stages an in-memory table via a pandas roundtrip (``op.data.to_frame()``)
before binding rows through the stdlib ``sqlite3`` module -- regardless of
whether the table was built from a dict, a PyArrow table, a Polars/pandas
DataFrame, or an ``ibis.memtable(..., schema=...)`` call with an explicit
temporal schema. A null ``date``/``timestamp`` value becomes pandas ``NaT``
during that roundtrip, and ``sqlite3`` has no adapter for ``NaTType`` --
``cur.executemany()`` raises ``sqlite3.ProgrammingError("Error binding
parameter N: type 'NaTType' is not supported")`` before any of
``create_table``/``insert``/``upsert`` reach the database -- for the entire
portable write surface (DEBT-13).

Verified empirically against ibis 12.0.0 (2026-08-19): this reproduces
identically whether the source frame is Polars, pandas, or an explicit
pyarrow/ibis schema. Tracked upstream as ``IB-DT-19`` in the
``mountainash`` repo's ``registry/upstream-issues.yaml`` (status:
``needs_filing`` as of 2026-08-18 -- no upstream ibis issue exists yet).
Same root cause, same fix shape as the sibling ``mountainash`` package's
``relations/backends/relation_systems/ibis/_sqlite_compat.py`` (item 112,
PR #303) -- kept as a separate, self-contained module here rather than a
hard dependency on ``mountainash``, since ``mountainash_data`` treats
``mountainash`` as optional (see ``operations.py::_coerce_dtype``).

This module registers a single process-global ``sqlite3`` adapter that binds
``NaT`` as ``NULL``, matching how every other backend already treats a
missing temporal value. It does not touch Ibis's own type inference or
compiled SQL, so schema fidelity (including ``Boolean`` columns, which a
naive ``pandas.DataFrame.to_sql()`` bypass would silently degrade to
``Int64``) is unaffected.
"""
from __future__ import annotations

_NAT_ADAPTER_INSTALLED = False


def ensure_sqlite_nat_adapter() -> None:
    """Register a ``sqlite3`` adapter so pandas ``NaT`` binds as ``NULL``.

    Idempotent and process-global: ``sqlite3.register_adapter`` just
    overwrites one dict entry, so repeated calls are a cheap no-op. Called
    unconditionally (no dialect check needed -- registering the adapter is
    a no-op for every other dialect's connection) at the top of every
    ``IbisBackend`` write path that can reach
    ``ibis.Backend._register_in_memory_table``: ``create_table``, ``insert``,
    and ``compiled_source`` (the shared staging step behind every
    ``upsert`` renderer).

    Silently returns if pandas is not importable: Ibis's own SQLite roundtrip
    requires pandas too (``ibis-framework[sqlite]`` depends on it), so if
    pandas is missing here, the crash this guards against cannot occur either.
    """
    global _NAT_ADAPTER_INSTALLED
    if _NAT_ADAPTER_INSTALLED:
        return

    import sqlite3

    try:
        import pandas as pd
    except ImportError:
        return

    sqlite3.register_adapter(type(pd.NaT), lambda _: None)
    _NAT_ADAPTER_INSTALLED = True
