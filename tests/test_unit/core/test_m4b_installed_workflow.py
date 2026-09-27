"""Portable candidate checks using only installed public APIs and local databases."""

from pathlib import Path

import pytest
from mountainash_auth_client import NoAuthProfile
from mountainash_settings import SettingsParameters

from mountainash_data import IbisBackend
from mountainash_data.core.settings import get_settings_class


@pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
@pytest.mark.parametrize("settings_driven", [False, True])
def test_local_candidate_workflow(
    tmp_path: Path, dialect: str, settings_driven: bool,
) -> None:
    path = tmp_path / f"{dialect}.db"
    if settings_driven:
        params = SettingsParameters.create(
            settings_class=get_settings_class(dialect), DATABASE=str(path),
        )
        backend = IbisBackend(params)
    else:
        backend = IbisBackend(dialect=dialect, database=str(path))
    try:
        backend.connect(auth_profile=NoAuthProfile())
        backend.create_table("m4b_probe", {"id": [1, 2], "name": ["first", "second"]})
        assert "m4b_probe" in backend.list_tables()
        rows = backend.table("m4b_probe").order_by("id").execute()
        assert rows["id"].tolist() == [1, 2]
        assert rows["name"].tolist() == ["first", "second"]
        info = backend.inspect_table("m4b_probe")
        assert info.name == "m4b_probe"
        assert {column.name for column in info.columns} == {"id", "name"}
    finally:
        backend.close()
