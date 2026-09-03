from google.oauth2 import service_account
from mountainash_auth_client import ServiceAccountAuthProfile

from mountainash_data.core.settings.adapters import bigquery as _bigquery


def test_bigquery_service_account(monkeypatch) -> None:
    sentinel = object()
    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        classmethod(lambda cls, info: sentinel),
    )

    assert _bigquery.service_account(
        ServiceAccountAuthProfile(INFO={"k": "v"}),
        {},
    ) == {"credentials": sentinel}


def test_bigquery_service_account_file(monkeypatch) -> None:
    sentinel = object()
    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_file",
        classmethod(lambda cls, path: sentinel),
    )

    assert _bigquery.service_account(
        ServiceAccountAuthProfile(FILE="/path/sa.json"),
        {},
    ) == {"credentials": sentinel}
