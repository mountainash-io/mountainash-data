"""ConnectionFactory: compose BackendProfile config + AuthProfile creds."""
from __future__ import annotations
import typing as t
from urllib.parse import quote, urlencode

from mountainash_auth_client import NoAuthProfile, PasswordAuthProfile, TokenAuthProfile, AuthProfile
from mountainash_data.core.constants import CONST_DB_PROVIDER_TYPE as P
from mountainash_data.core.settings.profile import UrlParts
from mountainash_data.core.settings.adapters.registry import auth_adapter


def _iter_specs() -> t.Iterator[t.Any]:
    """Registered BackendSpecs, regardless of whether the registry stores
    specs or profile classes."""
    from mountainash_data.core.settings.registry import REGISTRY
    for v in REGISTRY.values():
        yield v.__spec__ if hasattr(v, "__spec__") else v


def provider_for_dialect(dialect: str) -> t.Any:
    matches = [
        spec.provider_type
        for spec in _iter_specs()
        if getattr(spec, "ibis_dialect", None) == dialect
    ]
    if not matches:
        raise KeyError(f"no provider_type for ibis dialect {dialect!r}")
    # Built-in profiles map one-to-one to dialects; extra registered profiles may
    # share a dialect, in which case the first-registered (built-in) wins.
    return matches[0]


def provider_for_scheme(scheme: str) -> t.Any:
    """Provider for a URL scheme: ``_SCHEME_TO_DIALECT`` (sole table) then the dialect's profile."""
    # Lazy: backend.py imports this module at load time.
    from mountainash_data.backends.ibis.backend import _SCHEME_TO_DIALECT

    dialect = _SCHEME_TO_DIALECT.get(scheme.rstrip(":/").lower())
    if dialect is None:
        raise KeyError(f"no provider_type for URL scheme {scheme!r}")
    return provider_for_dialect(dialect)


def _normalize_and_validate_auth(profile: t.Any, auth_profile: AuthProfile | None) -> AuthProfile:
    auth = NoAuthProfile() if auth_profile is None else auth_profile
    if not isinstance(auth, tuple(profile.__spec__.supported_auth)):
        raise ValueError(f"{profile.profile_name} does not support auth: {type(auth).__name__}")
    return auth


def apply_auth_adapter(provider_type: t.Any, base: dict, auth_profile: AuthProfile | None) -> dict:
    """Apply auth WITHOUT a profile (ibis dialect / URL paths). No supported_auth gate."""
    if auth_profile is None or isinstance(auth_profile, NoAuthProfile):
        return base
    fn = auth_adapter(provider_type, type(auth_profile))
    if fn is None:
        raise ValueError(f"{provider_type}: no auth adapter for {type(auth_profile).__name__}")
    base = dict(base)
    return fn(auth_profile, base)


def build_driver_kwargs(profile: t.Any, auth_profile: AuthProfile | None = None) -> dict:
    auth = _normalize_and_validate_auth(profile, auth_profile)
    target = profile.__spec__.provider_type
    base = profile.emit(target)
    if isinstance(auth, NoAuthProfile):
        return base
    fn = auth_adapter(target, type(auth))
    if fn is None:
        raise ValueError(f"{profile.profile_name}: no auth adapter for {type(auth).__name__}")
    return fn(auth, base)


# --- URL appliers (L3 for the URL target) ---------------------------------

def _url_tail(parts: UrlParts) -> str:
    """``/path`` (or ``/database``) plus URL-encoded query, after the authority."""
    tail = parts.path or (f"/{parts.database}" if parts.database is not None else "")
    if parts.query:
        tail += "?" + urlencode(parts.query)
    return tail


def _url_password(parts: UrlParts, auth: t.Any) -> str:
    if parts.host is None:
        raise NotImplementedError("password URL form requires a host authority")
    user, pw = quote(str(auth.USERNAME), safe=""), quote(auth.PASSWORD.get_secret_value(), safe="")
    url = f"{parts.scheme}://{user}:{pw}@{parts.host}"
    if parts.port is not None:
        url += f":{parts.port}"
    return url + _url_tail(parts)


def _url_noauth(parts: UrlParts) -> str:
    if parts.host is None:
        # authority-less form (e.g. an in-process/file backend): scheme:database
        base = f"{parts.scheme}:{parts.database}" if parts.database is not None else f"{parts.scheme}:"
        return base + (("?" + urlencode(parts.query)) if parts.query else "")
    url = f"{parts.scheme}://{parts.host}"
    if parts.port is not None:
        url += f":{parts.port}"
    return url + _url_tail(parts)


def _url_motherduck_token(parts: UrlParts, auth: t.Any) -> str:
    token = quote(auth.TOKEN.get_secret_value(), safe="")
    return f"{parts.scheme}:{parts.database}?motherduck_token={token}"


_URL_APPLIERS: dict[t.Any, dict[type, t.Callable]] = {
    P.MOTHERDUCK: {TokenAuthProfile: _url_motherduck_token},
}


def build_connection_string(profile: t.Any, auth_profile: AuthProfile | None = None) -> str:
    auth = _normalize_and_validate_auth(profile, auth_profile)
    parts = profile.to_url_parts()                       # L1
    if isinstance(auth, NoAuthProfile):
        return _url_noauth(parts)
    if isinstance(auth, PasswordAuthProfile):
        return _url_password(parts, auth)                # L3
    applier = _URL_APPLIERS.get(profile.__spec__.provider_type, {}).get(type(auth))
    if applier is None:
        raise NotImplementedError(f"{profile.profile_name}: no URL form for {type(auth).__name__}")
    return applier(parts, auth)
