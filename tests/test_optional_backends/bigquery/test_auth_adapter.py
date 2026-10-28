from google.oauth2 import service_account
from mountainash_auth_client import ServiceAccountAuthProfile

from mountainash_data.core.settings.adapters import bigquery as _bigquery


def test_bigquery_service_account(monkeypatch) -> None:
    sentinel = object()
    seen = {}

    def fake_from_info(cls, info):
        seen["info"] = info
        return sentinel

    monkeypatch.setattr(service_account.Credentials, "from_service_account_info", classmethod(fake_from_info))

    assert _bigquery.service_account(
        ServiceAccountAuthProfile(INFO='{"k": "v"}'),
        {},
    ) == {"credentials": sentinel}
    # INFO is a secret JSON string: the adapter must unwrap the SecretStr and parse it
    assert seen["info"] == {"k": "v"}


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


def test_bigquery_service_account_invalid_info_error_is_value_free() -> None:
    import pytest

    with pytest.raises(ValueError) as info:
        _bigquery.service_account(ServiceAccountAuthProfile(INFO="SENTINEL-not-json"), {})

    exc = info.value
    assert "SENTINEL" not in f"{exc!s}{exc!r}{exc.__dict__!r}{exc.args!r}"
    assert exc.__cause__ is None and exc.__context__ is None
