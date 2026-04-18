"""BigQuery adapter: convert ServiceAccountAuth → google Credentials."""

from __future__ import annotations

import typing as t

from mountainash_settings.auth import NoAuth, ServiceAccountAuth

if t.TYPE_CHECKING:
    from mountainash_data.core.settings.bigquery import BigQueryAuthSettings


def build_driver_kwargs(profile: "BigQueryAuthSettings") -> dict[str, t.Any]:
    kwargs = profile._default_kwargs()

    auth = profile.auth
    if isinstance(auth, ServiceAccountAuth):
        from google.oauth2 import service_account as _sa

        if auth.info is not None:
            kwargs["credentials"] = _sa.Credentials.from_service_account_info(auth.info)
        elif auth.file is not None:
            kwargs["credentials"] = _sa.Credentials.from_service_account_file(
                str(auth.file)
            )
    elif isinstance(auth, NoAuth):
        pass  # Application Default Credentials
    return kwargs
