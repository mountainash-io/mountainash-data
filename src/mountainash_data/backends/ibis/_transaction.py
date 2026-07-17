"""Reentrant, cross-dialect unit-of-work machinery (Gap 3).

Ambient registry keyed on id(raw_handle) under a module lock: the outermost
transaction() issues the dialect's begin statement, nested calls join it, the
outermost COMMITs, and any exception (or a poisoned-by-caught-nested-failure
state) ROLLBACKs the whole unit. Flat semantics — no savepoints. Never toggles
the driver's autocommit flag. BEGIN/COMMIT/ROLLBACK go through the shared
`_raw.raw_execute` transport (honouring `raw_execute_hook`) because .execute()
is not uniform across DBAPI drivers.
"""

from __future__ import annotations

import contextlib
import threading
import typing as t
from dataclasses import dataclass

from mountainash_data.backends.ibis._raw import raw_execute
from mountainash_data.backends.ibis.dialects._registry import TransactionSupport
from mountainash_data.core._warn import warn_once
from mountainash_data.core.errors import (
    TransactionUnsupportedError,
    TransactionPoisonedError,
    TransactionIntegrityError,
)


@dataclass
class _TxState:
    depth: int = 0
    poisoned: bool = False


_ACTIVE: dict[int, _TxState] = {}
_LOCK = threading.Lock()


@contextlib.contextmanager
def run_transaction(
    raw_handle: t.Any,
    *,
    support: TransactionSupport,
    begin_statement: t.Optional[str],
    dialect: str,
    required: bool,
    autocommit_probe: t.Optional[t.Callable[[t.Any], t.Optional[bool]]] = None,
    in_transaction_probe: t.Optional[t.Callable[[t.Any], t.Optional[bool]]] = None,
    raw_execute_hook: t.Optional[t.Callable[[t.Any, str], None]] = None,
) -> t.Iterator[None]:
    if support is TransactionSupport.NONE:
        if required:
            raise TransactionUnsupportedError(
                f"{dialect!r} has no transaction concept; call transaction("
                f"required=False) to run as a best-effort no-op."
            )
        warn_once(dialect, f"{dialect!r} has no transaction support; transaction() is a no-op.")
        yield
        return

    def _exec(sql: str) -> None:
        raw_execute(raw_handle, sql, hook=raw_execute_hook)

    key = id(raw_handle)
    with _LOCK:
        state = _ACTIVE.get(key)
        is_outer = state is None

    # NOTE: the outer-entry check above and the _ACTIVE[key] insert below are
    # deliberately two separate critical sections, not one — the BEGIN must run
    # between them (register-after-BEGIN, so a failed BEGIN leaves no stale
    # entry). This is safe because a single raw driver connection is not safe
    # for concurrent use across threads at the DBAPI level, so two threads
    # racing to open the outer transaction on ONE handle is already
    # unsupported; the registry's job is reentrancy for sequential/nested
    # reuse of one connection, not cross-thread arbitration.
    if is_outer:
        # Entry precondition (finding 1): ibis interleaves commits on autocommit-off
        # connections, so a transaction() that cannot guarantee atomicity refuses.
        if autocommit_probe is not None and autocommit_probe(raw_handle) is False:
            raise TransactionIntegrityError(
                f"{dialect!r} connection has autocommit disabled; ibis would interleave "
                f"commits inside transaction(). Enable autocommit on the driver."
            )
        # Register AFTER a successful BEGIN so a failed BEGIN leaves no stale entry.
        if begin_statement is not None:
            _exec(begin_statement)
        state = _TxState(depth=1)
        with _LOCK:
            _ACTIVE[key] = state
        try:
            yield
        except BaseException as original:
            try:
                _exec("ROLLBACK")
            except Exception as rollback_error:
                original.__context__ = rollback_error
            raise
        else:
            if state.poisoned:
                _exec("ROLLBACK")
                raise TransactionPoisonedError(
                    "unit of work was poisoned by a caught nested failure; rolled back"
                )
            # Commit-time integrity (finding 1): if ibis rolled the server tx back
            # underneath us, refuse rather than commit nothing.
            if in_transaction_probe is not None and in_transaction_probe(raw_handle) is False:
                raise TransactionIntegrityError(
                    "server transaction vanished before COMMIT (ibis interleaved a "
                    "commit/rollback inside the unit of work)"
                )
            _exec("COMMIT")
        finally:
            with _LOCK:
                _ACTIVE.pop(key, None)
        return

    # Nested: join the in-flight unit of work (all state mutations under the lock).
    assert state is not None  # is_outer is False here, so _ACTIVE.get(key) was not None
    with _LOCK:
        if state.poisoned:
            raise TransactionPoisonedError(
                "transaction is poisoned by a prior failure in this unit of work"
            )
        state.depth += 1
    try:
        yield
    except BaseException:
        with _LOCK:
            state.poisoned = True
        raise
    finally:
        with _LOCK:
            state.depth -= 1


def is_active(raw_handle: t.Any) -> bool:
    """True if a unit of work is registered on this raw handle (any depth).

    Read-only companion to run_transaction(): reads the ambient registry under
    the same lock the register/unregister critical sections take, so the read
    observes a fully-committed registry mutation. Never mutates _ACTIVE. Keyed
    on id(raw_handle), matching run_transaction's reentrancy key, so distinct
    IbisBackend wrappers of one raw connection agree.
    """
    with _LOCK:
        return id(raw_handle) in _ACTIVE
