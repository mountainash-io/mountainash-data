import warnings
import pytest
from mountainash_data.backends.ibis._transaction import run_transaction, _ACTIVE
from mountainash_data.backends.ibis.dialects._registry import TransactionSupport
from mountainash_data.core.errors import (
    TransactionUnsupportedError, TransactionPoisonedError, TransactionIntegrityError,
)


class FakeHandle:
    def __init__(self):
        self.calls = []
    def execute(self, sql):
        self.calls.append(sql)


def _tx(h, **kw):
    kw.setdefault("support", TransactionSupport.FULL)
    kw.setdefault("begin_statement", "BEGIN")
    kw.setdefault("dialect", "duckdb")
    kw.setdefault("required", True)
    return run_transaction(h, **kw)


def test_autocommit_off_entry_raises():
    h = FakeHandle()
    with pytest.raises(TransactionIntegrityError):
        with _tx(h, autocommit_probe=lambda _c: False):
            pass
    assert h.calls == []            # refused before BEGIN
    assert id(h) not in _ACTIVE


def test_commit_time_integrity_probe_raises_if_tx_vanished():
    h = FakeHandle()
    with pytest.raises(TransactionIntegrityError):
        with _tx(h, in_transaction_probe=lambda _c: False):
            pass
    assert "COMMIT" not in h.calls   # integrity failure instead of a false commit
    assert id(h) not in _ACTIVE


def test_outermost_commit():
    h = FakeHandle()
    with _tx(h):
        pass
    assert h.calls == ["BEGIN", "COMMIT"]
    assert id(h) not in _ACTIVE


def test_exception_rolls_back():
    h = FakeHandle()
    with pytest.raises(ValueError):
        with _tx(h):
            raise ValueError("boom")
    assert h.calls == ["BEGIN", "ROLLBACK"]
    assert id(h) not in _ACTIVE


def test_nested_joins_no_second_begin():
    h = FakeHandle()
    with _tx(h):
        with _tx(h):
            pass
    assert h.calls == ["BEGIN", "COMMIT"]  # inner joined; only one BEGIN/COMMIT


def test_nested_exception_rolls_back_whole_unit():
    h = FakeHandle()
    with pytest.raises(ValueError):
        with _tx(h):
            with _tx(h):
                raise ValueError("boom")
    assert h.calls == ["BEGIN", "ROLLBACK"]


def test_none_support_required_raises():
    h = FakeHandle()
    with pytest.raises(TransactionUnsupportedError):
        with _tx(h, support=TransactionSupport.NONE, begin_statement=None):
            pass
    assert h.calls == []


def test_none_support_not_required_warns_once_and_noops():
    from mountainash_data.core import _warn as _warnmod; _warnmod._WARNED.discard("clickhouse")
    h = FakeHandle()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        with _tx(h, support=TransactionSupport.NONE, begin_statement=None,
                 required=False, dialect="clickhouse"):
            pass
    assert h.calls == []
    assert any("clickhouse" in str(x.message) for x in w)


def test_begin_statement_none_skips_begin():
    h = FakeHandle()
    with _tx(h, begin_statement=None, dialect="oracle"):
        pass
    assert h.calls == ["COMMIT"]  # implicit begin; commit still issued


def test_begin_failure_leaves_no_registry_entry():
    class Boom(FakeHandle):
        def execute(self, sql):
            if sql == "BEGIN":
                raise RuntimeError("begin failed")
            super().execute(sql)
    h = Boom()
    with pytest.raises(RuntimeError, match="begin failed"):
        with _tx(h):
            pass
    assert id(h) not in _ACTIVE  # register-after-begin: no stale entry


def test_poison_via_caught_nested_exception_does_not_commit():
    # caller CATCHES the nested failure inside the outer block; outer must NOT commit
    h = FakeHandle()
    with pytest.raises(TransactionPoisonedError):
        with _tx(h):
            try:
                with _tx(h):
                    raise ValueError("inner")
            except ValueError:
                pass  # swallow — but the unit of work is poisoned
    assert h.calls == ["BEGIN", "ROLLBACK"]  # rolled back, never committed
    assert id(h) not in _ACTIVE


def test_transport_uses_cursor_when_no_execute():
    # a DBAPI connection without .execute() must go through .cursor().execute()
    class Cursor:
        def __init__(self, log): self.log = log
        def execute(self, sql): self.log.append(("cur", sql))
        def close(self): self.log.append(("close", None))
    class ConnNoExecute:
        def __init__(self): self.log = []
        def cursor(self): return Cursor(self.log)
    h = ConnNoExecute()
    with _tx(h):
        pass
    assert ("cur", "BEGIN") in h.log and ("cur", "COMMIT") in h.log
    assert ("close", None) in h.log


from mountainash_data.backends.ibis._transaction import is_active


def test_is_active_false_when_not_registered():
    h = FakeHandle()
    assert is_active(h) is False


def test_is_active_true_inside_outer_transaction():
    h = FakeHandle()
    with _tx(h):
        assert is_active(h) is True
    assert is_active(h) is False


def test_is_active_true_at_nested_depth():
    h = FakeHandle()
    with _tx(h):
        with _tx(h):
            assert is_active(h) is True
        assert is_active(h) is True  # still open at depth 1
    assert is_active(h) is False


def test_is_active_false_after_rollback():
    h = FakeHandle()
    with pytest.raises(ValueError):
        with _tx(h):
            assert is_active(h) is True
            raise ValueError("boom")
    assert is_active(h) is False


def test_is_active_true_while_poisoned_before_unwind():
    # A caught nested failure poisons the unit; while the outer block is still
    # open the handle stays registered, so is_active must report True.
    h = FakeHandle()
    with pytest.raises(TransactionPoisonedError):
        with _tx(h):
            try:
                with _tx(h):
                    raise ValueError("inner")
            except ValueError:
                pass
            assert is_active(h) is True  # poisoned but still an open unit of work
    assert is_active(h) is False  # cleared after outer unwind


def test_is_active_does_not_mutate_registry():
    h = FakeHandle()
    before = dict(_ACTIVE)
    assert is_active(h) is False
    assert _ACTIVE == before  # pure read
