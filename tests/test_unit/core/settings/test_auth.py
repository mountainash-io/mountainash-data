"""Unit tests for AuthSpec discriminated-union members."""

import pytest
from pydantic import SecretStr, ValidationError

from mountainash_data.core.settings.auth import (
    AzureADAuth,
    CertificateAuth,
    IAMAuth,
    JWTAuth,
    KerberosAuth,
    NoAuth,
    OAuth2Auth,
    PasswordAuth,
    ServiceAccountAuth,
    TokenAuth,
    WindowsAuth,
)


@pytest.mark.unit
class TestAuthDiscriminator:
    @pytest.mark.parametrize(
        "cls, kind",
        [
            (NoAuth, "none"),
            (PasswordAuth, "password"),
            (TokenAuth, "token"),
            (JWTAuth, "jwt"),
            (OAuth2Auth, "oauth2"),
            (ServiceAccountAuth, "service_account"),
            (IAMAuth, "iam"),
            (WindowsAuth, "windows"),
            (AzureADAuth, "azure_ad"),
            (KerberosAuth, "kerberos"),
            (CertificateAuth, "certificate"),
        ],
    )
    def test_every_auth_has_discriminator_kind(self, cls, kind):
        if cls is PasswordAuth:
            instance = cls(username="u", password=SecretStr("p"))
        elif cls in (TokenAuth, JWTAuth):
            instance = cls(token=SecretStr("t"))
        else:
            instance = cls()
        assert instance.kind == kind

    def test_password_auth_requires_username_and_password(self):
        with pytest.raises(ValidationError):
            PasswordAuth()  # type: ignore[call-arg]

    def test_password_auth_wraps_password_as_secretstr(self):
        auth = PasswordAuth(username="alice", password="hunter2")
        assert isinstance(auth.password, SecretStr)
        assert auth.password.get_secret_value() == "hunter2"

    def test_noauth_has_no_fields(self):
        auth = NoAuth()
        assert auth.kind == "none"

    def test_auth_is_frozen(self):
        """Mutation of an AuthSpec instance must raise."""
        auth = NoAuth()
        with pytest.raises(ValidationError):
            auth.kind = "password"  # type: ignore[misc]

    def test_auth_rejects_unknown_fields(self):
        """Unknown kwargs must raise because model_config.extra == 'forbid'."""
        with pytest.raises(ValidationError):
            NoAuth(bogus="x")  # type: ignore[call-arg]


@pytest.mark.unit
class TestOAuth2Auth:
    def test_all_fields_optional(self):
        auth = OAuth2Auth()
        assert auth.client_id is None
        assert auth.client_secret is None
        assert auth.token is None

    def test_token_is_secret(self):
        auth = OAuth2Auth(token="t")
        assert isinstance(auth.token, SecretStr)
