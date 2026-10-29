"""Parametric spec invariants for all registered database backends.

Generated from the shared ``spec_invariants_for`` helper in
``mountainash-settings``. Every spec in ``DATABASES_REGISTRY`` gets
checked against the invariants for free — no per-backend test additions
required.
"""

from __future__ import annotations

# Ensure every backend module's @register decorator has fired before we
# snapshot the registry for the parametrize decorator.
import mountainash_data.core.settings  # noqa: F401

from mountainash_data.core.settings.registry import DATABASES_REGISTRY
from mountainash_settings.profiles import spec_invariants_for

TestDatabaseInvariants = spec_invariants_for(DATABASES_REGISTRY)
