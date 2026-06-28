"""BigQuery auth adapter functions."""
from __future__ import annotations
import typing as t


def service_account(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    from google.oauth2 import service_account as _sa
    out = dict(base)
    if auth.INFO is not None:
        out["credentials"] = _sa.Credentials.from_service_account_info(auth.INFO)
    elif auth.FILE is not None:
        out["credentials"] = _sa.Credentials.from_service_account_file(str(auth.FILE))
    return out
