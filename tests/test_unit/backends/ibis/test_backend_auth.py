import pytest
from mountainash_auth_client import NoAuthProfile, PasswordAuthProfile
from mountainash_data.backends.ibis.backend import IbisBackend


def test_sqlite_dialect_connect_noauth(tmp_path):
    be = IbisBackend(dialect="sqlite", database=str(tmp_path / "t.db")).connect(auth_profile=NoAuthProfile())
    assert be is not None


def test_dialect_path_applies_password(monkeypatch):
    # direct-dialect + explicit auth: auth adapter must run for the dialect's provider.
    seen = {}
    import mountainash_data.backends.ibis.backend as mod
    def fake_apply(pt, base, auth):
        seen["pt"], seen["auth"] = pt, auth
        return {**base, "user": auth.USERNAME}
    monkeypatch.setattr(mod, "apply_auth_adapter", fake_apply)
    monkeypatch.setattr(mod, "provider_for_dialect", lambda d: "PG")
    IbisBackend(dialect="postgres", host="h", database="db")._resolve_dialect_auth(
        PasswordAuthProfile(USERNAME="u", PASSWORD="p")
    )
    assert seen["pt"] == "PG" and seen["auth"].USERNAME == "u"


def test_url_and_explicit_auth_conflict_raises():
    with pytest.raises(ValueError, match="both"):
        IbisBackend("postgresql://u:p@host/db").connect(
            auth_profile=PasswordAuthProfile(USERNAME="x", PASSWORD="y"))
