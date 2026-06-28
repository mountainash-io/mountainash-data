import pytest
from dataclasses import dataclass

from mountainash_auth_client import (
    PasswordAuthProfile, TokenAuthProfile, NoAuthProfile, WindowsAuthProfile,
)
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from mountainash_data.core.settings.profile import UrlParts
from mountainash_data.core.factories.connection_factory import (
    build_driver_kwargs, build_connection_string, _normalize_and_validate_auth,
    apply_auth_adapter, provider_for_dialect,
)


@dataclass
class _Spec:
    provider_type: object
    supported_auth: tuple
    name: str = "stub"


class _Stub:
    def __init__(self, pt, sa, base, url=None):
        self.__spec__ = _Spec(pt, sa)
        self._base, self._url = base, url or UrlParts(scheme="stub", host="h", port=1, database="db")
    @property
    def backend(self): return self.__spec__.name
    def emit(self, target):
        assert target is self.__spec__.provider_type
        return dict(self._base)
    def to_url_parts(self): return self._url


def test_noauth_short_circuits():
    assert build_driver_kwargs(_Stub(P.SQLITE, (NoAuthProfile,), {"database": ":memory:"}), None) == {"database": ":memory:"}


def test_password_dispatch():
    out = build_driver_kwargs(_Stub(P.POSTGRESQL, (PasswordAuthProfile, NoAuthProfile), {"host": "h"}),
                              PasswordAuthProfile(USERNAME="u", PASSWORD="p"))
    assert out == {"host": "h", "user": "u", "password": "p"}


def test_unsupported_auth_valueerror():
    with pytest.raises(ValueError, match="does not support auth"):
        build_driver_kwargs(_Stub(P.SQLITE, (NoAuthProfile,), {}), PasswordAuthProfile(USERNAME="u", PASSWORD="p"))


def test_supported_but_no_adapter_fails_closed():
    with pytest.raises(ValueError, match="no auth adapter"):
        build_driver_kwargs(_Stub(P.POSTGRESQL, (WindowsAuthProfile,), {"host": "h"}), WindowsAuthProfile(USERNAME="u"))


def test_none_normalizes_when_supported():
    assert isinstance(_normalize_and_validate_auth(_Stub(P.SQLITE, (NoAuthProfile,), {}), None), NoAuthProfile)


def test_none_rejected_when_noauth_unsupported():
    with pytest.raises(ValueError, match="does not support auth"):
        _normalize_and_validate_auth(_Stub(P.MYSQL, (PasswordAuthProfile,), {}), None)


def test_apply_auth_adapter_non_profile():
    out = apply_auth_adapter(P.POSTGRESQL, {"host": "h"}, PasswordAuthProfile(USERNAME="u", PASSWORD="p"))
    assert out == {"host": "h", "user": "u", "password": "p"}
    assert apply_auth_adapter(P.POSTGRESQL, {"host": "h"}, None) == {"host": "h"}


def test_provider_for_dialect_collision_resolves_to_base():
    # postgres + duckdb dialects are shared by rider providers; resolve canonically.
    assert provider_for_dialect("postgres") is P.POSTGRESQL
    assert provider_for_dialect("duckdb") is P.DUCKDB


def test_url_password():
    s = _Stub(P.POSTGRESQL, (PasswordAuthProfile,), {}, url=UrlParts(scheme="postgresql", host="db", port=5432, database="app"))
    assert build_connection_string(s, PasswordAuthProfile(USERNAME="u", PASSWORD="p@s")) == "postgresql://u:p%40s@db:5432/app"


def test_url_token_authority_less():
    s = _Stub(P.MOTHERDUCK, (TokenAuthProfile,), {}, url=UrlParts(scheme="md", database="mydb"))
    assert build_connection_string(s, TokenAuthProfile(TOKEN="T")) == "md:mydb?motherduck_token=T"


def test_url_noauth_authority_less():
    s = _Stub(P.DUCKDB, (NoAuthProfile,), {}, url=UrlParts(scheme="duckdb", database="my.db"))
    assert build_connection_string(s, NoAuthProfile()) == "duckdb:my.db"


@pytest.mark.parametrize("auth", [WindowsAuthProfile(USERNAME="u"), TokenAuthProfile(TOKEN="T")])
def test_url_unsupported_auth_not_implemented(auth):
    s = _Stub(P.POSTGRESQL, (type(auth),), {}, url=UrlParts(scheme="postgresql", host="db"))
    with pytest.raises(NotImplementedError):
        build_connection_string(s, auth)
