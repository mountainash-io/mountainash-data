"""Generic ConnectionProfile base for all backend settings.

A subclass declares ``__descriptor__`` (a :class:`BackendDescriptor`); this
base uses pydantic v2's ``__pydantic_init_subclass__`` hook to materialize the
descriptor into pydantic fields, compose the :class:`AuthSpec` union into the
``auth`` field, and install the generic :meth:`to_driver_kwargs` /
:meth:`to_connection_string` API.
"""

from __future__ import annotations

import typing as t

from pydantic import SecretStr
from pydantic.fields import FieldInfo

from mountainash_settings import MountainAshBaseSettings

from .auth.dispatch import auth_to_driver_kwargs
from .descriptor import MISSING, BackendDescriptor

__all__ = ["ConnectionProfile"]


class ConnectionProfile(MountainAshBaseSettings):
    """Declarative settings base — subclasses set ``__descriptor__`` only.

    Public API:
        - :attr:`backend` — descriptor name.
        - :attr:`provider_type` — descriptor provider_type.
        - :meth:`to_driver_kwargs` — dict ready for the underlying driver.
        - :meth:`to_connection_string` — URL form (or ``NotImplementedError``
          if the backend has no URL scheme).
    """

    __descriptor__: t.ClassVar[BackendDescriptor]
    __adapter__: t.ClassVar[
        t.Callable[["ConnectionProfile"], dict[str, t.Any]] | None
    ] = None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: t.Any) -> None:
        """Install fields described by ``__descriptor__`` on the subclass."""
        super().__pydantic_init_subclass__(**kwargs)
        desc = cls.__dict__.get("__descriptor__")
        if desc is None:
            return  # intermediate subclass without its own descriptor

        # Build the field additions
        new_fields: dict[str, tuple[t.Any, FieldInfo]] = {}

        # 1. Descriptor parameters → pydantic fields
        for spec in desc.parameters:
            ptype: t.Any = SecretStr if spec.secret else spec.type
            if spec.default is MISSING:
                info = FieldInfo(
                    annotation=ptype,
                    default=...,
                    description=spec.description,
                )
            else:
                info = FieldInfo(
                    annotation=ptype,
                    default=spec.default,
                    description=spec.description,
                )
            new_fields[spec.name] = (ptype, info)

        # 2. auth field as discriminated union of descriptor.auth_modes
        if desc.auth_modes:
            # Dynamic Union type from the descriptor's auth_modes list.
            auth_union: t.Any
            if len(desc.auth_modes) == 1:
                auth_union = desc.auth_modes[0]
                auth_info = FieldInfo(annotation=auth_union, default=...)
            else:
                auth_union = t.Union[tuple(desc.auth_modes)]  # type: ignore[valid-type]
                auth_info = FieldInfo(
                    annotation=auth_union,
                    default=...,
                    discriminator="kind",
                )
            new_fields["auth"] = (auth_union, auth_info)

        # Install fields and rebuild the model
        for name, (annotation, info) in new_fields.items():
            cls.model_fields[name] = info
            cls.__annotations__[name] = annotation

        cls.model_rebuild(force=True)

    # --- Public properties ---------------------------------------------------

    @property
    def backend(self) -> str:
        return self.__descriptor__.name

    @property
    def provider_type(self) -> t.Any:
        return self.__descriptor__.provider_type

    # --- Driver kwargs --------------------------------------------------------

    def _default_driver_kwargs(self) -> dict[str, t.Any]:
        """Emit 1:1 driver_key mappings from the descriptor.

        - Skips ``None`` values.
        - Unwraps :class:`SecretStr` via ``.get_secret_value()``.
        - Applies ``ParameterSpec.transform`` if set.
        """
        out: dict[str, t.Any] = {}
        for spec in self.__descriptor__.parameters:
            if spec.driver_key is None:
                continue
            val = getattr(self, spec.name, None)
            if val is None:
                continue
            if isinstance(val, SecretStr):
                val = val.get_secret_value()
            if spec.transform is not None:
                val = spec.transform(val)
            out[spec.driver_key] = val
        return out

    def _auth_to_driver_kwargs(self) -> dict[str, t.Any]:
        auth = getattr(self, "auth", None)
        if auth is None:
            return {}
        return auth_to_driver_kwargs(auth)

    def to_driver_kwargs(self) -> dict[str, t.Any]:
        """Build the final driver kwargs dict.

        Order:
            1. 1:1 parameter mappings from descriptor.
            2. Auth dispatch (may overwrite 1:1 outputs).
            3. Per-backend adapter, if any (may overwrite auth outputs).
        """
        kwargs = self._default_driver_kwargs()
        kwargs.update(self._auth_to_driver_kwargs())
        if self.__adapter__ is not None:
            kwargs = self.__adapter__(self)
        return kwargs

    # --- Connection string ----------------------------------------------------

    def to_connection_string(self) -> str:
        """Build ``scheme://...`` form from the descriptor.

        Raises :class:`NotImplementedError` if the descriptor has no scheme.
        Backends with non-standard URL shapes override this method.
        """
        scheme = self.__descriptor__.connection_string_scheme
        if scheme is None:
            raise NotImplementedError(
                f"Backend {self.backend!r} has no connection string scheme"
            )
        host = getattr(self, "HOST", None)
        port = getattr(self, "PORT", None)
        database = getattr(self, "DATABASE", None)
        url = scheme
        auth = getattr(self, "auth", None)
        if auth is not None and getattr(auth, "username", None):
            url += str(auth.username)
            pw = getattr(auth, "password", None)
            if isinstance(pw, SecretStr):
                url += f":{pw.get_secret_value()}"
            url += "@"
        if host is not None:
            url += str(host)
        if port is not None:
            url += f":{port}"
        if database is not None:
            url += f"/{database}"
        return url
