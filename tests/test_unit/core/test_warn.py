import warnings
from mountainash_data.core import _warn


def test_warn_once_emits_first_time_only():
    _warn._WARNED.discard("k-alpha")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _warn.warn_once("k-alpha", "first")
        _warn.warn_once("k-alpha", "second")
    assert len(w) == 1
    assert "first" in str(w[0].message)


def test_warn_once_distinct_keys_each_warn():
    _warn._WARNED.discard("k-beta")
    _warn._WARNED.discard("k-gamma")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _warn.warn_once("k-beta", "b")
        _warn.warn_once("k-gamma", "g")
    assert len(w) == 2
