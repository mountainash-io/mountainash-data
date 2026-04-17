# tests/test_unit/core/settings/backends/test_motherduck.py
from __future__ import annotations

import pytest
from pydantic import SecretStr

from mountainash_data.core.settings.auth import TokenAuth
from mountainash_data.core.settings.motherduck import MotherDuckAuthSettings


@pytest.mark.unit
class TestMotherDuckAuthSettings:
    def test_minimal(self):
        s = MotherDuckAuthSettings(
            DATABASE="mydb", auth=TokenAuth(token=SecretStr("t"))
        )
        assert s.DATABASE == "mydb"

    def test_no_database_ok(self):
        """Audit regression: previously validator rejected None, field was Optional."""
        s = MotherDuckAuthSettings(auth=TokenAuth(token=SecretStr("t")))
        assert s.DATABASE is None

    def test_to_driver_kwargs_unwraps_token(self):
        s = MotherDuckAuthSettings(
            DATABASE="mydb", auth=TokenAuth(token=SecretStr("tok"))
        )
        kwargs = s.to_driver_kwargs()
        assert kwargs["token"] == "tok"
        assert isinstance(kwargs["token"], str)  # not SecretStr
