"""Reentrant, cross-dialect unit-of-work machinery.

Ambient registry keyed on id(raw_handle) under a module lock. The outermost
transaction() decides who owns completion; nested calls on the same raw
handle join it. Flat semantics - no savepoints. Never toggles the driver's
autocommit flag. BEGIN/COMMIT/ROLLBACK go through the shared
`_raw.raw_execute` transport (honouring `raw_execute_hook`) because .execute()
is not uniform across DBAPI drivers.

Ownership dialects (OWNERSHIP_DIALECTS) observe native state on entry: idle ->
begin and own the unit; already open (caller's transaction) -> join without
ever completing it. Other transactional dialects keep the original
begin/commit/rollback behaviour.
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

OWNERSHIP_DIALECTS = frozenset({"sqlite", "duckdb", "postgres"})

Probe = t.Callable[[t.Any], t.Optional[bool]]


@dataclass
class _TxState:
    depth: int = 0
    poisoned: bool = False
    owns: bool = True
    # True on ownership dialects: package calls in this scope are protected.
    selected: bool = False


_ACTIVE: dict[int, _TxState] = {}
_LOCK = threading.Lock()


def _attach(primary: BaseException, secondary: BaseException) -> None:
    """Keep `primary` as the raised error while preserving `secondary`."""
    if primary.__context__ is None:
        primary.__context__ = secondary
    else:
        primary.add_note(f"additionally: {secondary!r}")


@contextlib.contextmanager
def run_transaction(
    raw_handle: t.Any,
    *,
    support: TransactionSupport,
    begin_statement: t.Optional[str],
    dialect: str,
    required: bool,
    in_transaction_probe: t.Optional[Probe] = None,
    raw_execute_hook: t.Optional[t.Callable[[t.Any, str], None]] = None,
    selected: bool = False,
    begin_hook: t.Optional[t.Callable[[t.Any], None]] = None,
    precommit_probe: t.Optional[Probe] = None,
    on_commit_failure: t.Optional[t.Callable[[], None]] = None,
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

    if state is not None:
        yield from _join(state)
        return

    # Entry and registration are two critical sections with BEGIN between them
    # (register-after-BEGIN, so a failed BEGIN leaves no stale entry). Safe
    # because one raw driver connection is not usable concurrently anyway; the
    # registry provides sequential reentrancy, not cross-thread arbitration.
    owns = True
    if selected:
        owns = not _observe_entry(raw_handle, dialect, in_transaction_probe)
    if owns:
        if begin_hook is not None:
            begin_hook(raw_handle)
        elif begin_statement is not None:
            _exec(begin_statement)

    state = _TxState(depth=1, owns=owns, selected=selected)
    with _LOCK:
        _ACTIVE[key] = state
    try:
        try:
            yield
        except BaseException as original:
            if state.owns:
                try:
                    _exec("ROLLBACK")
                except Exception as rollback_error:
                    _attach(original, rollback_error)
            raise
        if state.poisoned:
            if state.owns:
                _exec("ROLLBACK")
            raise TransactionPoisonedError(
                "unit of work was poisoned by a caught failure"
                + ("; rolled back" if state.owns else "; caller owns completion")
            )
        if selected:
            _finish_selected(
                raw_handle, state.owns, _exec,
                in_transaction_probe, precommit_probe, on_commit_failure,
            )
        else:
            _exec("COMMIT")
    finally:
        with _LOCK:
            _ACTIVE.pop(key, None)


def _join(state: _TxState) -> t.Iterator[None]:
    """Nested entry on an already-registered handle: flat join, no BEGIN."""
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


def _observe_entry(raw_handle: t.Any, dialect: str, probe: t.Optional[Probe]) -> bool:
    """Return True when a native transaction is already open (join it)."""
    if probe is None:
        raise TransactionIntegrityError(f"{dialect!r} has no native transaction-state probe")
    try:
        present = probe(raw_handle)
    except Exception as exc:
        raise TransactionIntegrityError(
            f"could not observe {dialect!r} native transaction state"
        ) from exc
    if present is None:
        raise TransactionIntegrityError(
            f"{dialect!r} native transaction state is unknown; refusing to start or join"
        )
    return present


def _finish_selected(
    raw_handle: t.Any,
    owns: bool,
    _exec: t.Callable[[str], None],
    in_transaction_probe: t.Optional[Probe],
    precommit_probe: t.Optional[Probe],
    on_commit_failure: t.Optional[t.Callable[[], None]],
) -> None:
    """Clean outermost exit on an ownership dialect (spec section 2 table)."""
    probe = precommit_probe or in_transaction_probe
    assert probe is not None  # _observe_entry already required a presence probe
    try:
        eligible = probe(raw_handle)
    except Exception as exc:
        error = TransactionIntegrityError(
            "could not determine native transaction state before COMMIT"
        )
        error.__cause__ = exc
        _rollback_owned_best_effort(owns, _exec, error)
        raise error

    if eligible:
        if not owns:
            return
        try:
            _exec("COMMIT")
        except BaseException as commit_error:
            # Ambiguous: never retry and never claim a rollback.
            if on_commit_failure is not None:
                try:
                    on_commit_failure()
                except Exception as cleanup_error:
                    _attach(commit_error, cleanup_error)
            raise
        return

    if _vanished(raw_handle, eligible, in_transaction_probe, precommit_probe):
        raise TransactionIntegrityError(
            "native transaction ended before scope exit (for example a COMMIT "
            "or ROLLBACK through a raw escape hatch); nothing was committed here"
        )
    error = TransactionIntegrityError(
        "native transaction is not eligible to commit (aborted or unknown state)"
    )
    _rollback_owned_best_effort(owns, _exec, error)
    raise error


def _vanished(
    raw_handle: t.Any,
    eligible: t.Optional[bool],
    in_transaction_probe: t.Optional[Probe],
    precommit_probe: t.Optional[Probe],
) -> bool:
    if eligible is not False:
        return False
    if precommit_probe is None:
        return True  # the eligibility probe is the presence probe
    try:
        return in_transaction_probe is not None and in_transaction_probe(raw_handle) is False
    except Exception:
        return False


def _rollback_owned_best_effort(
    owns: bool, _exec: t.Callable[[str], None], error: BaseException,
) -> None:
    if not owns:
        return
    try:
        _exec("ROLLBACK")
    except Exception as rollback_error:
        error.add_note(f"rollback also failed: {rollback_error!r}")


def protection_required(raw_handle: t.Any) -> bool:
    """True when a package call on this handle must run protected.

    That is the case inside a registered scope on an ownership dialect.
    Raises TransactionPoisonedError if that scope is already poisoned, so no
    further database work runs in a unit that can no longer commit.
    """
    with _LOCK:
        state = _ACTIVE.get(id(raw_handle))
        if state is None or not state.selected:
            return False
        if state.poisoned:
            raise TransactionPoisonedError(
                "transaction is poisoned by a prior failure in this unit of work"
            )
        return True


def mark_poisoned(raw_handle: t.Any) -> None:
    """Poison a registered scope on this handle; no-op when none is registered."""
    with _LOCK:
        state = _ACTIVE.get(id(raw_handle))
        if state is not None:
            state.poisoned = True


def is_active(raw_handle: t.Any) -> bool:
    """True if a unit of work is registered on this raw handle (any depth).

    Read-only: never mutates _ACTIVE. Keyed on id(raw_handle), matching
    run_transaction's reentrancy key, so distinct IbisBackend wrappers of one
    raw connection agree. A registered scope may be joined caller work; this
    is not a statement that Mountainash owns completion.
    """
    with _LOCK:
        return id(raw_handle) in _ACTIVE
