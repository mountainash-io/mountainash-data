import pytest
from mountainash_auth_client import PasswordAuthProfile, TokenAuthProfile, NoAuthProfile
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from mountainash_data.core.settings.adapters import sql as _sql, snowflake as _snow
from mountainash_data.core.settings.adapters.registry import auth_adapter, _AUTH_ADAPTERS


def test_exact_lookup():
    assert auth_adapter(P.SNOWFLAKE, TokenAuthProfile) is _snow.token


def test_flat_userpass_shared():
    assert auth_adapter(P.POSTGRESQL, PasswordAuthProfile) is _sql.userpass


def test_miss_returns_none():
    assert auth_adapter(P.SQLITE, PasswordAuthProfile) is None


def test_noauth_not_in_table():
    assert all(k[1] is not NoAuthProfile for k in _AUTH_ADAPTERS)


def test_subclass_resolves_to_base():
    class MyPw(PasswordAuthProfile): pass
    assert auth_adapter(P.POSTGRESQL, MyPw) is _sql.userpass


def test_specialization_wins():
    fn = lambda a, b: b
    class Special(PasswordAuthProfile): pass
    _AUTH_ADAPTERS[(P.POSTGRESQL, Special)] = fn
    try:
        assert auth_adapter(P.POSTGRESQL, Special) is fn
    finally:
        del _AUTH_ADAPTERS[(P.POSTGRESQL, Special)]


def test_sibling_ambiguity_raises():
    fn = lambda a, b: b
    _AUTH_ADAPTERS[(P.POSTGRESQL, TokenAuthProfile)] = fn
    class Hybrid(PasswordAuthProfile, TokenAuthProfile): pass
    try:
        with pytest.raises(TypeError, match="ambiguous"):
            auth_adapter(P.POSTGRESQL, Hybrid)
    finally:
        del _AUTH_ADAPTERS[(P.POSTGRESQL, TokenAuthProfile)]
