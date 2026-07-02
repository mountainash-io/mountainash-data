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


from mountainash_data.backends.ibis.backend import (
    _render_ibis_database,
    _render_ibis_namespace_single,
)


class TestRenderIbisDatabase:
    def test_default_renders_none(self):
        assert _render_ibis_database(Namespace()) is None

    def test_single_level_renders_str(self):
        assert _render_ibis_database(Namespace(path=("sales",))) == "sales"

    def test_catalog_qualified_renders_tuple(self):
        ns = Namespace(catalog="wh", path=("sales",))
        assert _render_ibis_database(ns) == ("wh", "sales")

    def test_depth_greater_than_one_raises(self):
        with pytest.raises(ValueError, match="single namespace level"):
            _render_ibis_database(Namespace(path=("a", "b")))

    def test_catalog_without_level_raises(self):
        with pytest.raises(ValueError, match="requires one path level"):
            _render_ibis_database(Namespace(catalog="wh"))


class TestRenderIbisNamespaceSingle:
    def test_single_level_ok(self):
        assert _render_ibis_namespace_single(Namespace(path=("sales",)), op="upsert") == "sales"

    def test_default_ok(self):
        assert _render_ibis_namespace_single(Namespace(), op="upsert") is None

    def test_catalog_qualified_rejected(self):
        with pytest.raises(ValueError, match="does not support catalog-qualified"):
            _render_ibis_namespace_single(Namespace(catalog="wh", path=("sales",)), op="upsert")

    def test_depth_over_one_rejected(self):
        with pytest.raises(ValueError, match="single namespace level"):
            _render_ibis_namespace_single(Namespace(path=("a", "b")), op="create_index")
