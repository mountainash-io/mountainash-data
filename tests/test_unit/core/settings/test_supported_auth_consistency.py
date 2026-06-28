"""Structural invariant: every non-NoAuth supported_auth entry has an auth adapter."""

import pytest
from mountainash_auth_client import NoAuthProfile
from mountainash_data.core.settings.adapters.registry import auth_adapter
from mountainash_data.core.factories.connection_factory import _iter_specs


@pytest.mark.unit
def test_every_supported_pair_has_an_adapter():
    """Every non-NoAuth supported_auth entry in a BackendSpec must have a registered adapter."""
    for spec in _iter_specs():
        for auth_cls in spec.supported_auth:
            if auth_cls is NoAuthProfile:
                continue
            assert auth_adapter(spec.provider_type, auth_cls) is not None, (
                f"{spec.name}: supported {auth_cls.__name__} has no adapter"
            )
