from mountainash_data.backends.ibis._adoption import (
    snapshot_options, restore_options, apply_options,
)
from mountainash_data.backends.ibis.dialects._registry import SessionOption


class FakeResult:
    def __init__(self, value):
        self._value = value
    def fetchone(self):
        return (self._value,)


class FakeHandle:
    def __init__(self, values=None):
        self.values = values or {}
        self.calls = []
    def execute(self, sql):
        self.calls.append(sql)
        # read SQL returns a canned value keyed by substring match
        for k, v in self.values.items():
            if k in sql:
                return FakeResult(v)
        return FakeResult(None)


OPT = SessionOption(
    "python_enable_replacements",
    "SELECT current_setting('python_enable_replacements')",
    lambda v: f"SET python_enable_replacements={'true' if v else 'false'}",
)


def test_snapshot_reads_values():
    h = FakeHandle({"python_enable_replacements": True})
    snap = snapshot_options(h, (OPT,))
    assert snap == {"python_enable_replacements": True}


def test_restore_replays_captured():
    h = FakeHandle()
    restore_options(h, (OPT,), {"python_enable_replacements": True})
    assert "SET python_enable_replacements=true" in h.calls


def test_apply_renders_declared_values():
    h = FakeHandle()
    apply_options(h, (OPT,), {"python_enable_replacements": True})
    assert "SET python_enable_replacements=true" in h.calls


def test_apply_ignores_unknown_option_names():
    import warnings

    h = FakeHandle()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        apply_options(h, (OPT,), {"not_a_real_option": 1})
    assert h.calls == []  # nothing rendered for unknown names
    assert any("not_a_real_option" in str(w.message) for w in caught)


def test_snapshot_skips_options_without_read_sql():
    opt = SessionOption("x", None, lambda v: f"SET x={v}")
    h = FakeHandle()
    assert snapshot_options(h, (opt,)) == {}


def test_snapshot_warns_when_read_raises():
    import warnings as _w
    class Boom:
        def execute(self, sql):
            raise RuntimeError("cannot read setting")
    with _w.catch_warnings(record=True) as rec:
        _w.simplefilter("always")
        snap = snapshot_options(Boom(), (OPT,))
    assert snap == {}
    assert any("python_enable_replacements" in str(x.message) for x in rec)
