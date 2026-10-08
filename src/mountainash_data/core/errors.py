"""Shared backend exceptions."""

from __future__ import annotations


class TransactionError(RuntimeError):
    """Base for transaction() failures."""


class TransactionUnsupportedError(TransactionError):
    """transaction() called on a backend with no transaction concept."""


class TransactionPoisonedError(TransactionError):
    """The unit of work was aborted by a caught nested failure; it cannot commit."""


class TransactionIntegrityError(TransactionError):
    """The native transaction state cannot be trusted: unknown when entering a
    scope, or not eligible to commit at exit (ended early, aborted, or unknown).
    Nothing is committed on the scope's behalf when this is raised."""
