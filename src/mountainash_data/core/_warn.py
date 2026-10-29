"""Process-wide "warn at most once per key" helper (Gap 3, fable finding 6).

Shared by the ibis transaction machinery so a no-op transaction() on an
unsupported backend warns once per dialect, not per call. Lives in core/ so
backends don't need to import each other.
"""
from __future__ import annotations

import threading
import warnings

_WARNED: set[str] = set()
_LOCK = threading.Lock()


def warn_once(key: str, message: str) -> None:
    """Emit ``message`` via ``warnings.warn`` the first time ``key`` is seen."""
    with _LOCK:
        first = key not in _WARNED
        if first:
            _WARNED.add(key)
    if first:
        warnings.warn(message, stacklevel=3)
