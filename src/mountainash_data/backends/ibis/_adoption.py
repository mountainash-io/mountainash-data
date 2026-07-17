"""Session-option snapshot / restore / apply for adoption (Gap 1)."""

from __future__ import annotations

import typing as t
import warnings

from mountainash_data.backends.ibis._raw import raw_execute, raw_fetch_scalar
from mountainash_data.backends.ibis.dialects._registry import SessionOption
from mountainash_data.core._warn import warn_once


def snapshot_options(
    raw_handle: t.Any, options: tuple[SessionOption, ...]
) -> dict[str, t.Any]:
    """Read the current value of each option that has a read_sql, via the shared
    _raw transport (finding 3 — cursor-safe across drivers).

    An option that cannot be read is NOT silently skipped — it WARNS, because a
    value we cannot snapshot cannot be restored, and "faithful" preservation must
    signal when it can't be faithful (Codex review). Options with read_sql=None
    are skipped without a warning (nothing to snapshot by design)."""
    snap: dict[str, t.Any] = {}
    for opt in options:
        if opt.read_sql is None:
            continue
        try:
            value = raw_fetch_scalar(raw_handle, opt.read_sql)
        except Exception as exc:  # noqa: BLE001 — warn, don't fail adoption
            warnings.warn(
                f"could not snapshot session option {opt.name!r}; it will not be "
                f"restored ({exc!r})",
                stacklevel=2,
            )
            continue
        snap[opt.name] = value
    return snap


def restore_options(
    raw_handle: t.Any,
    options: tuple[SessionOption, ...],
    snapshot: dict[str, t.Any],
) -> None:
    """Replay each captured value via its render_set statement (shared transport)."""
    by_name = {o.name: o for o in options}
    for name, value in snapshot.items():
        opt = by_name.get(name)
        if opt is not None:
            raw_execute(raw_handle, opt.render_set(value))


def apply_options(
    raw_handle: t.Any,
    options: tuple[SessionOption, ...],
    values: dict[str, t.Any],
) -> None:
    """Apply caller-declared end-state values. Unknown names are ignored, with a
    warning (each name warns once — see warn_once)."""
    by_name = {o.name: o for o in options}
    for name, value in values.items():
        opt = by_name.get(name)
        if opt is not None:
            raw_execute(raw_handle, opt.render_set(value))
        else:
            warn_once(
                f"apply_options:{name}",
                f"session option {name!r} is not a declared adoption mutation "
                f"for this backend; ignored",
            )
