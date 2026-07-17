"""Shared backend exceptions."""

from __future__ import annotations


class TransactionError(RuntimeError):
    """Base for transaction() failures."""


class TransactionUnsupportedError(TransactionError):
    """transaction() called on a backend with no transaction concept."""


class TransactionPoisonedError(TransactionError):
    """The unit of work was aborted by a caught nested failure; it cannot commit."""


class TransactionIntegrityError(TransactionError):
    """Atomicity cannot be guaranteed: the driver is autocommit-off at entry, or the
    server-side transaction vanished (ibis interleaved a commit/rollback) before COMMIT."""
