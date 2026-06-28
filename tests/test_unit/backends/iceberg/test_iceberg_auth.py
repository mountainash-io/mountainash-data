import pytest

pytest.importorskip("pyiceberg", reason="pyiceberg not installed")
pytest.importorskip("mountainash_dataframes", reason="mountainash_dataframes not installed")

from types import SimpleNamespace  # noqa: E402
from unittest.mock import patch  # noqa: E402

from mountainash_auth_client import TokenAuthProfile  # noqa: E402
from mountainash_data.backends.iceberg.connection import IcebergConnectionBase  # noqa: E402


class _ConcreteIceberg(IcebergConnectionBase):
    @property
    def catalog_backend(self):
        return getattr(self, "_catalog_backend", None)


# test double: bypass the remaining ABC methods we don't exercise
_ConcreteIceberg.__abstractmethods__ = frozenset()


def test_build_catalog_kwargs_threads_auth_and_merges():
    obj_settings = object()
    params = SimpleNamespace(
        settings_class=SimpleNamespace(get_settings=lambda settings_parameters: obj_settings)
    )
    conn = _ConcreteIceberg.__new__(_ConcreteIceberg)
    conn.db_auth_settings_parameters = params

    auth = TokenAuthProfile(TOKEN="T")
    with patch(
        "mountainash_data.backends.iceberg.connection.build_driver_kwargs",
        return_value={"uri": "http://x", "token": "T", "name": "c"},
    ) as bk:
        out = conn._build_catalog_kwargs(auth, warehouse="w")

    bk.assert_called_once_with(obj_settings, auth)   # profile + auth_profile threaded
    assert out["warehouse"] == "w"                   # explicit kwargs win
    assert out["uri"] == "http://x"
