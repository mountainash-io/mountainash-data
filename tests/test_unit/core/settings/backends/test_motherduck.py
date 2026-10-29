# tests/test_unit/core/settings/backends/test_motherduck.py
from __future__ import annotations

import pytest

from mountainash_data.core.settings.motherduck import MotherDuckBackendProfile


@pytest.mark.unit
class TestMotherDuckBackendProfile:
    def test_minimal(self):
        s = MotherDuckBackendProfile(DATABASE="mydb")
        assert s.DATABASE == "mydb"

    def test_no_database_ok(self):
        """Audit regression: previously validator rejected None, field was Optional."""
        s = MotherDuckBackendProfile()
        assert s.DATABASE is None

    def test_to_url_parts_uses_md_scheme(self):
        s = MotherDuckBackendProfile(DATABASE="mydb")
        parts = s.to_url_parts()
        assert parts.scheme == "md"
        assert parts.database == "mydb"
