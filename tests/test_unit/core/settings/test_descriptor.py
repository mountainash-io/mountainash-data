"""Tests for the database-flavored BackendSpec subclass."""

import pytest

from mountainash_data.core.settings.auth import NoAuth
from mountainash_data.core.settings.descriptor import (
    BackendSpec,
    ParameterSpec,
)


@pytest.mark.unit
class TestBackendSpec:
    def test_default_port_field(self):
        d = BackendSpec(
            name="x", provider_type="x",
            parameters=[], auth_modes=[NoAuth],
            default_port=5432,
        )
        assert d.default_port == 5432

    def test_connection_string_scheme_field(self):
        d = BackendSpec(
            name="x", provider_type="x",
            parameters=[], auth_modes=[NoAuth],
            connection_string_scheme="postgresql://",
        )
        assert d.connection_string_scheme == "postgresql://"

    def test_rides_on_field(self):
        d = BackendSpec(
            name="motherduck", provider_type="motherduck",
            parameters=[], auth_modes=[NoAuth],
            rides_on="duckdb",
        )
        assert d.rides_on == "duckdb"

    def test_frozen(self):
        d = BackendSpec(
            name="x", provider_type="x",
            parameters=[], auth_modes=[NoAuth],
        )
        with pytest.raises(Exception):
            d.name = "y"  # type: ignore
