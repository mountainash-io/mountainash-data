"""W8 safe display helpers (shared rendering table, safe list)."""
from __future__ import annotations

from pathlib import PurePosixPath

import pytest

from mountainash_data.core._safe_errors import raise_safe, safe_cause, safe_detail, safe_location
from mountainash_data.core.errors import BackendConnectionError
from tests.helpers.error_surfaces import exception_surfaces

# Shared across packages (spec section 5.1): keep identical to the other packages' copies.
RENDERING = [
    # scheme:// — scheme, host, port, path only
    ("https://SENTU:SENTP@h:8443/a/x.csv?sig=SENTQ#SENTF", "https://h:8443/a/x.csv"),
    ("https://SENTU:pa@SENTP@host/x.csv", "https://host/x.csv"),
    ("https://SENTU:SENTP@[::1]:8443/d/x.csv", "https://[::1]:8443/d/x.csv"),
    ("https://host/a@b/x.csv", "https://host/a@b/x.csv"),
    ("https://host/x.csv#access_token=SENTF", "https://host/x.csv"),
    ("https://host/x.csv?SENTQ", "https://host/x.csv"),
    ("https://host/x.csv#f?tok=SENTQ", "https://host/x.csv"),
    ("https://h#SENTF@x/y", "https://h"),
    ("https://h?SENTQ@x/y", "https://h"),
    ("https://host", "https://host"),
    ("s3://bucket/x.csv", "s3://bucket/x.csv"),
    ("file:///tmp/x.csv", "file:///tmp/x.csv"),
    ("HTTPS://SENTU:SENTP@host/x.csv", "HTTPS://host/x.csv"),
    ("postgresql+psycopg://SENTU:SENTP@db/name", "postgresql+psycopg://db/name"),
    # //network path
    ("//SENTU:SENTP@host/x.csv", "//host/x.csv"),
    # whitespace / control characters, as urllib reads them
    (" \thttps://SENTU:SENTP@host/x.csv", "https://host/x.csv"),
    ("https://SENTU:SENTP@ho\nst/x.csv", "https://host/x.csv"),
    ("https://SENTU:SENTP@ho\rst/x.csv", "https://host/x.csv"),
    ("ht\ttps://SENTU:SENTP@host/x.csv", "https://host/x.csv"),
    ("\x00https://SENTU:SENTP@host/x.csv", "https://host/x.csv"),
    ("\x1fhttps://SENTU:SENTP@host/x.csv", "https://host/x.csv"),
    # other scheme strings
    ("https:/SENTU:SENTP@host/x.csv", "***"),
    ("s3:SENTU:SENTP@bucket/x.csv", "***"),
    ("SENTU:SENTP@host/x.csv", "***"),
    ("a:SENTP@host/x.csv", "***"),
    # local paths
    ("/data/x.csv", "/data/x.csv"),
    ("data/x.csv", "data/x.csv"),
    ("/srv/team@corp/x.csv", "/srv/team@corp/x.csv"),
    ("relative/a:b@c/x.csv", "relative/a:b@c/x.csv"),
    ("C:/d/x.csv", "C:/d/x.csv"),
    ("C:\\d\\x.csv", "C:\\d\\x.csv"),
]


@pytest.mark.parametrize(("location", "shown"), RENDERING)
def test_safe_location_rendering(location, shown):
    assert safe_location(location) == shown
    assert "SENT" not in safe_location(location)


def test_safe_location_accepts_pathlike():
    assert safe_location(PurePosixPath("/data/x.csv")) == "/data/x.csv"


def test_safe_detail_keeps_errno_and_drops_filename():
    err = FileNotFoundError(2, "No such file or directory", "/secret/SENTINEL.key")
    assert safe_detail(err) == "[Errno 2] No such file or directory"


@pytest.mark.parametrize(
    "native", [ValueError("SENTINEL"), KeyError("SENTINEL"), OSError("SENTINEL no errno"), RuntimeError("SENTINEL")]
)
def test_safe_detail_other_types_are_class_name_only(native):
    assert safe_detail(native) == type(native).__name__
    assert safe_cause(native) is None


def test_raise_safe_keeps_only_a_fresh_errno_cause_and_clears_context():
    native = ConnectionRefusedError(111, "Connection refused", "SENTINEL-file")
    with pytest.raises(BackendConnectionError) as info:
        try:
            raise ValueError("SENTINEL-ambient")
        except ValueError:
            raise_safe(BackendConnectionError("x: connection failed"), native)
    err = info.value
    assert err.__context__ is None
    assert isinstance(err.__cause__, ConnectionRefusedError)
    assert err.__cause__ is not native
    assert "SENTINEL" not in exception_surfaces(err)
