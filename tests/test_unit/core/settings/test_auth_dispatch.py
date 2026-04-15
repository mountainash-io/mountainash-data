"""Default AUTH_TO_DRIVER_KWARGS coverage tests."""

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import (
    AuthSpec,
    IAMAuth,
    JWTAuth,
    NoAuth,
    OAuth2Auth,
    PasswordAuth,
    TokenAuth,
)
from mountainash_data.core.settings.auth.dispatch import auth_to_driver_kwargs


@pytest.mark.unit
class TestAuthToDriverKwargs:
    def test_noauth_returns_empty(self):
        assert auth_to_driver_kwargs(NoAuth()) == {}

    def test_password_unwraps_secret(self):
        auth = PasswordAuth(username="alice", password=SecretStr("hunter2"))
        assert auth_to_driver_kwargs(auth) == {
            "user": "alice",
            "password": "hunter2",
        }

    def test_token_unwraps_secret(self):
        auth = TokenAuth(token=SecretStr("t"))
        assert auth_to_driver_kwargs(auth) == {"token": "t"}

    def test_jwt_unwraps_secret(self):
        auth = JWTAuth(token=SecretStr("j"))
        assert auth_to_driver_kwargs(auth) == {"token": "j"}

    def test_oauth2_with_token(self):
        auth = OAuth2Auth(token=SecretStr("bearer"))
        assert auth_to_driver_kwargs(auth) == {"token": "bearer"}

    def test_oauth2_with_client_credentials(self):
        auth = OAuth2Auth(
            client_id="cid", client_secret=SecretStr("csec")
        )
        assert auth_to_driver_kwargs(auth) == {"credential": "cid:csec"}

    def test_oauth2_token_wins_over_client_credentials(self):
        """Policy: if both token and client_credentials are set, token wins."""
        auth = OAuth2Auth(
            token=SecretStr("t"),
            client_id="c",
            client_secret=SecretStr("s"),
        )
        assert auth_to_driver_kwargs(auth) == {"token": "t"}

    def test_oauth2_empty_returns_empty(self):
        """OAuth2 with neither token nor client-credentials yields no kwargs."""
        assert auth_to_driver_kwargs(OAuth2Auth()) == {}

    def test_iam_with_keys(self):
        auth = IAMAuth(
            access_key_id="AKIA...",
            secret_access_key=SecretStr("sk"),
            session_token=SecretStr("st"),
        )
        assert auth_to_driver_kwargs(auth) == {
            "aws_access_key_id": "AKIA...",
            "aws_secret_access_key": "sk",
            "aws_session_token": "st",
        }

    def test_iam_with_role_arn(self):
        auth = IAMAuth(role_arn="arn:aws:iam::123:role/x")
        assert auth_to_driver_kwargs(auth) == {
            "iam_role_arn": "arn:aws:iam::123:role/x"
        }

    def test_iam_empty_returns_empty(self):
        """IAM with no explicit fields falls through to ambient credentials."""
        assert auth_to_driver_kwargs(IAMAuth()) == {}

    def test_unknown_auth_type_raises(self):
        class WeirdAuth(AuthSpec):
            kind: str = "weird"  # type: ignore[assignment]

        with pytest.raises(KeyError):
            auth_to_driver_kwargs(WeirdAuth())
