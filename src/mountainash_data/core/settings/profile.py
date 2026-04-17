"""ConnectionProfile — database-flavored subclass of DescriptorProfile.

Adds ``to_driver_kwargs()`` and ``to_connection_string()`` on top of the
generic mechanism provided by
:class:`mountainash_settings.profiles.DescriptorProfile`.
"""

from __future__ import annotations

import typing as t
from urllib.parse import quote

from pydantic import SecretStr

from mountainash_settings.profiles import DescriptorProfile

__all__ = ["ConnectionProfile"]


class ConnectionProfile(DescriptorProfile):
    """Database connection settings.

    Public API:
        - :meth:`to_driver_kwargs` — dict ready for the Ibis driver.
        - :meth:`to_connection_string` — URL form, or ``NotImplementedError``
          if the descriptor has no ``connection_string_scheme`` metadata.

    Subclasses set ``__descriptor__`` (a :class:`ProfileDescriptor`) and
    optionally ``__adapter__``. Field installation, auth union, and template
    wiring are inherited from :class:`DescriptorProfile`.
    """

    def to_driver_kwargs(self) -> dict[str, t.Any]:
        """Build the final driver kwargs dict.

        If ``__adapter__`` is set, it owns the full pipeline — typically it
        calls :meth:`_default_kwargs` and :meth:`_auth_kwargs` and layers
        composite mappings on top. Otherwise defaults to descriptor
        ``driver_key`` mappings + default auth dispatch.
        """
        adapter = type(self).__dict__.get("__adapter__")
        if adapter is None:
            for base in type(self).__mro__[1:]:
                candidate = base.__dict__.get("__adapter__")
                if candidate is not None:
                    adapter = candidate
                    break
        if adapter is not None:
            return adapter(self)
        kwargs = self._default_kwargs()
        kwargs.update(self._auth_kwargs())
        return kwargs

    def to_connection_string(self) -> str:
        """Build ``scheme://user:pass@host:port/database`` from the descriptor.

        Reads the scheme from ``descriptor.metadata['connection_string_scheme']``
        (or a typed ``connection_string_scheme`` attribute if the descriptor
        subclass provides one). Raises :class:`NotImplementedError` if absent.
        """
        desc = self.__descriptor__
        scheme = getattr(desc, "connection_string_scheme", None)
        if scheme is None:
            scheme = desc.metadata.get("connection_string_scheme")
        if scheme is None:
            raise NotImplementedError(
                f"Profile {self.backend!r} has no connection string scheme"
            )
        host = getattr(self, "HOST", None)
        port = getattr(self, "PORT", None)
        database = getattr(self, "DATABASE", None)
        url = scheme
        auth = getattr(self, "auth", None)
        if auth is not None:
            username = getattr(auth, "username", None)
            if username:
                url += quote(str(username), safe="")
                pw = getattr(auth, "password", None)
                if isinstance(pw, SecretStr):
                    url += ":" + quote(pw.get_secret_value(), safe="")
                url += "@"
        if host is not None:
            url += str(host)
        if port is not None:
            url += f":{port}"
        if database is not None:
            url += f"/{database}"
        return url
