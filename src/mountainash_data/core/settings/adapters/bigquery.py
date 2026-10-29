"""BigQuery auth adapter functions."""
from __future__ import annotations
import json
import typing as t


def _parse_info(auth: t.Any) -> dict[str, t.Any]:
    """INFO is a secret JSON string (SecretStr); unwrap and parse without echoing it."""
    invalid = False
    try:
        info = json.loads(auth.INFO.get_secret_value())
    except ValueError:  # JSONDecodeError carries the whole document in .doc
        invalid = True
    if invalid:
        # Raised outside the handler so the decode error is not chained.
        raise ValueError("ServiceAccountAuthProfile.INFO is not valid JSON (value redacted)")
    if not isinstance(info, dict):
        raise ValueError("ServiceAccountAuthProfile.INFO must be a JSON object (value redacted)")
    return info


def service_account(auth: t.Any, base: dict[str, t.Any]) -> dict[str, t.Any]:
    from google.oauth2 import service_account as _sa
    out = dict(base)
    if auth.INFO is not None:
        out["credentials"] = _sa.Credentials.from_service_account_info(_parse_info(auth))
    elif auth.FILE is not None:
        out["credentials"] = _sa.Credentials.from_service_account_file(str(auth.FILE))
    return out
