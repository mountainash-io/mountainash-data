"""Keep Ibis from completing an explicit transaction() scope.

Inside a registered scope on an ownership dialect, package operations run in
protected_call(). On SQLite and PostgreSQL Ibis commits, rolls back, or opens
its own psycopg transaction() around internal work; for the length of the
call the Ibis backend's driver handle is temporarily replaced by a view that
forwards everything except those completion requests. DuckDB needs no view.
Any exception escaping a protected call poisons the scope, and a call into an
already-poisoned scope is rejected before doing any database work.
"""

from __future__ import annotations

import contextlib
import typing as t

from mountainash_data.backends.ibis._transaction import mark_poisoned, protection_required

# Ibis backends that complete transactions on their own internal paths.
_COMPLETING_IBIS_BACKENDS = frozenset({"sqlite", "postgres"})


class _NonOwningDriver:
    """Driver view that never completes the caller's transaction."""

    def __init__(self, raw: t.Any) -> None:
        self.raw = raw

    def __getattr__(self, name: str) -> t.Any:
        return getattr(self.raw, name)

    def commit(self) -> None:
        """Completion belongs to the transaction() scope, not to Ibis."""

    def rollback(self) -> None:
        """Completion belongs to the transaction() scope, not to Ibis.

        The escaping error poisons the scope, whose owner rolls back."""

    @contextlib.contextmanager
    def transaction(self, *args: t.Any, **kwargs: t.Any) -> t.Iterator[None]:
        # Participate in the open unit: no savepoint, errors propagate.
        yield


def real_driver_handle(ibis_conn: t.Any, raw_handle_attr: str) -> t.Any:
    """The native driver behind `ibis_conn`, unwrapping a protection view."""
    handle = getattr(ibis_conn, raw_handle_attr, None)
    return handle.raw if isinstance(handle, _NonOwningDriver) else handle


@contextlib.contextmanager
def protected_call(ibis_conn: t.Any, raw_handle_attr: str) -> t.Iterator[bool]:
    """Run one package operation; yields whether it is protected."""
    raw = real_driver_handle(ibis_conn, raw_handle_attr)
    if raw is None or not protection_required(raw):
        yield False
        return
    current = getattr(ibis_conn, raw_handle_attr)
    swap = (
        getattr(ibis_conn, "name", None) in _COMPLETING_IBIS_BACKENDS
        and not isinstance(current, _NonOwningDriver)
    )
    if swap:
        setattr(ibis_conn, raw_handle_attr, _NonOwningDriver(raw))
    try:
        yield True
    except BaseException:
        mark_poisoned(raw)
        raise
    finally:
        if swap:
            setattr(ibis_conn, raw_handle_attr, current)
