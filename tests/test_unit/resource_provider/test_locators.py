from __future__ import annotations

from mountainash_data.resource_provider.locators import normalize_database_url


def test_normalize_database_url_canonicalizes_scheme_host_and_default_port() -> None:
    identity = normalize_database_url("POSTGRES://DB.EXAMPLE.COM:5432/sales")
    assert identity.backend == "postgresql"
    assert identity.host == "db.example.com"
    assert identity.port is None
    assert identity.database == "sales"
