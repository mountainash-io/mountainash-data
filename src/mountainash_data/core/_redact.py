"""Redaction of URL credentials from text and exceptions (W4 / F-4).

Connection URLs can reach exception messages through ibis and the DB drivers
(``ibis.connect`` echoes the whole URL in "Don't know how to connect to ...").
This module strips ``user:pass@`` userinfo, credential-bearing query values
and any explicitly supplied secret values from such text.
"""
from __future__ import annotations

import re
import typing as t
from urllib.parse import quote, quote_plus

from mountainash_data.core.errors import BackendConnectionError

REDACTED = "***"

# scheme://<anything up to the LAST '@' of the whitespace-free token>@
# Greedy on purpose: an unencoded '/', ':' or '@' inside a password must not
# end the match early and leave a password fragment behind.
_USERINFO = re.compile(r"(?P<scheme>[A-Za-z][A-Za-z0-9+.\-]*://)[^\s'\"<>]*@")
_QUERY_SECRET = re.compile(
    r"(?P<key>(?:motherduck_)?token|access_token|password|passwd|pwd|secret|api_?key)=[^&\s'\"<>]*",
    re.IGNORECASE,
)


def redact_text(text: str, secrets: t.Iterable[str] = ()) -> str:
    """Return ``text`` with URL userinfo, secret query values and ``secrets`` removed."""
    text = _USERINFO.sub(lambda m: f"{m.group('scheme')}{REDACTED}@", text)
    text = _QUERY_SECRET.sub(lambda m: f"{m.group('key')}={REDACTED}", text)
    for secret in sorted({s for s in secrets if isinstance(s, str) and s}, key=len, reverse=True):
        for form in (secret, quote(secret, safe=""), quote_plus(secret)):
            text = text.replace(form, REDACTED)
    return text


def redacted_exception(exc: BaseException, secrets: t.Iterable[str] = ()) -> BaseException | None:
    """Return a credential-free replacement for ``exc``, or ``None`` if it is clean.

    Builtin exception types are rebuilt as the same type so ``except ValueError``
    handlers keep working; any other (driver) type becomes
    :class:`BackendConnectionError`. The replacement carries only redacted text.
    """
    secrets = tuple(secrets)
    original = f"{exc!s}\x00{exc!r}"
    if redact_text(original, secrets) == original:
        return None
    message = redact_text(str(exc), secrets)
    if type(exc).__module__ == "builtins":
        try:
            return type(exc)(message)
        except Exception:  # exotic builtin constructor (e.g. UnicodeError)
            pass
    return BackendConnectionError(f"{type(exc).__name__}: {message}")


def connect_redacted(
    connect: t.Callable[..., t.Any], *args: t.Any, secrets: t.Iterable[str] = (), **kwargs: t.Any
) -> t.Any:
    """Call ``connect(*args, **kwargs)``; re-raise any failure with credentials removed.

    The original exception is replaced (and not chained) only when it carried
    credentials; otherwise it propagates untouched.
    """
    clean: BaseException | None
    try:
        return connect(*args, **kwargs)
    except Exception as exc:
        clean = redacted_exception(exc, secrets)
        if clean is None:
            raise
    # Raised outside the handler so the credential-bearing original is not
    # attached as __context__ either.
    raise clean
