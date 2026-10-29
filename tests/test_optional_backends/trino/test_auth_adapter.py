from mountainash_auth_client import PasswordAuthProfile
from trino.auth import BasicAuthentication

from mountainash_data.core.settings.adapters import trino as _trino


def test_trino_password_builds_basic_auth() -> None:
    result = _trino.password(
        PasswordAuthProfile(USERNAME="u", PASSWORD="p"),
        {"host": "h"},
    )

    assert result["host"] == "h"
    assert result["user"] == "u"
    assert isinstance(result["auth"], BasicAuthentication)
