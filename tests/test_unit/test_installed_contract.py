"""Resolver-facing contracts must survive editable and built installations."""

from importlib.metadata import metadata

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
import pytest


@pytest.mark.parametrize(
    ("name", "accepted", "rejected"),
    [
        ("mountainash-settings", "0.1.0", ("0.2", "26.5.0")),
        ("mountainash-auth-client", "26.6.1", ("26.6.0", "27")),
    ],
)
def test_internal_dependencies_are_unconditional(name, accepted, rejected):
    requirements = [Requirement(raw) for raw in metadata("mountainash-data").get_all("Requires-Dist", [])]
    matches = [r for r in requirements if canonicalize_name(r.name) == name]
    assert len(matches) == 1
    requirement = matches[0]
    assert requirement.marker is None
    assert accepted in requirement.specifier
    for version in rejected:
        assert version not in requirement.specifier


def test_supported_python_floor():
    python = SpecifierSet(metadata("mountainash-data")["Requires-Python"])
    assert "3.11" not in python
    assert "3.12" in python
    assert "3.13" in python


def test_retired_secrets_is_not_a_runtime_dependency():
    requirements = [Requirement(raw) for raw in metadata("mountainash-data").get_all("Requires-Dist", [])]
    assert all(canonicalize_name(r.name) != "mountainash-secrets" for r in requirements)
