"""Tests for the Namespace value object (core.namespace)."""

import pytest

from mountainash_data.core.namespace import Namespace


class TestCoercion:
    def test_none_is_default(self):
        assert Namespace.coerce(None) == Namespace()

    def test_str_becomes_single_level_path(self):
        assert Namespace.coerce("sales") == Namespace(path=("sales",))

    def test_tuple_is_pure_path_never_catalog(self):
        # A bare tuple is NEVER read positionally as (catalog, database).
        assert Namespace.coerce(("a", "b")) == Namespace(path=("a", "b"))
        assert Namespace.coerce(("a", "b")).catalog is None

    def test_namespace_passthrough(self):
        ns = Namespace(catalog="wh", path=("sales",))
        assert Namespace.coerce(ns) is ns

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError):
            Namespace.coerce(123)


class TestValidation:
    def test_empty_segment_rejected(self):
        with pytest.raises(ValueError):
            Namespace(path=("",))

    def test_non_string_segment_rejected(self):
        with pytest.raises(ValueError):
            Namespace(path=(1,))  # type: ignore[arg-type]


class TestProperties:
    def test_is_default_true_only_when_empty(self):
        assert Namespace().is_default is True
        assert Namespace(path=("x",)).is_default is False
        assert Namespace(catalog="c").is_default is False

    def test_dotted_joins_catalog_then_path(self):
        assert Namespace(catalog="wh", path=("a", "b")).dotted == "wh.a.b"
        assert Namespace(path=("a", "b")).dotted == "a.b"
        assert Namespace().dotted == ""

    def test_frozen(self):
        ns = Namespace(path=("x",))
        with pytest.raises(Exception):
            ns.path = ("y",)  # type: ignore[misc]


def test_exported_from_public_surface():
    import mountainash_data
    from mountainash_data.core import Namespace as CoreNamespace

    assert mountainash_data.Namespace is CoreNamespace
