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


# --- Ownership path (sqlite/duckdb/postgres) -------------------------------

class NativeHandle(FakeHandle):
    """Fake driver whose native transaction state follows BEGIN/COMMIT/ROLLBACK."""

    def __init__(self, open_=False, fail_on=None):
        super().__init__()
        self.open = open_
        self.fail_on = fail_on or {}

    def execute(self, sql):
        if sql in self.fail_on:
            raise self.fail_on[sql]
        super().execute(sql)
        if sql == "BEGIN":
            self.open = True
        elif sql in ("COMMIT", "ROLLBACK"):
            self.open = False


def _owned_tx(h, **kw):
    kw.setdefault("selected", True)
    kw.setdefault("in_transaction_probe", lambda c: c.open)
    return _tx(h, **kw)


def test_idle_entry_owns_and_commits():
    h = NativeHandle()
    with _owned_tx(h):
        assert h.open is True
    assert h.calls == ["BEGIN", "COMMIT"]


def test_open_entry_joins_without_completion():
    h = NativeHandle(open_=True)
    with _owned_tx(h):
        assert is_active(h) is True
    assert h.calls == []
    assert h.open is True
    assert is_active(h) is False


def test_joined_scope_error_propagates_without_rollback():
    h = NativeHandle(open_=True)
    with pytest.raises(ValueError):
        with _owned_tx(h):
            raise ValueError("boom")
    assert h.calls == []
    assert h.open is True


def test_joined_scope_poisoned_raises_without_completion():
    h = NativeHandle(open_=True)
    with pytest.raises(TransactionPoisonedError):
        with _owned_tx(h):
            try:
                with _owned_tx(h):
                    raise ValueError("inner")
            except ValueError:
                pass
    assert h.calls == []
    assert h.open is True


@pytest.mark.parametrize("probe", [lambda c: None, lambda c: (_ for _ in ()).throw(OSError("probe"))])
def test_unknown_entry_state_refuses_before_any_sql(probe):
    h = NativeHandle()
    with pytest.raises(TransactionIntegrityError):
        with _owned_tx(h, in_transaction_probe=probe):
            pass
    assert h.calls == []
    assert id(h) not in _ACTIVE


def test_begin_hook_replaces_begin_statement():
    h = NativeHandle()
    def hook(c):
        c.calls.append("hook")
        c.open = True
    with _owned_tx(h, begin_hook=hook):
        pass
    assert h.calls == ["hook", "COMMIT"]


def test_owned_vanished_transaction_raises_without_completion():
    h = NativeHandle()
    with pytest.raises(TransactionIntegrityError):
        with _owned_tx(h):
            h.open = False  # e.g. a raw COMMIT through an escape hatch
    assert h.calls == ["BEGIN"]


def test_owned_ineligible_but_open_rolls_back():
    # PostgreSQL INERROR: present, but not eligible to commit.
    h = NativeHandle()
    with pytest.raises(TransactionIntegrityError):
        with _owned_tx(h, precommit_probe=lambda c: False):
            pass
    assert h.calls == ["BEGIN", "ROLLBACK"]


def test_owned_failed_exit_probe_rolls_back_best_effort():
    # e.g. DuckDB: an aborted transaction rejects even the state probe.
    h = NativeHandle()
    def failing_precommit(c):
        raise OSError("aborted, cannot probe")
    with pytest.raises(TransactionIntegrityError) as caught:
        with _owned_tx(h, precommit_probe=failing_precommit):
            pass
    assert isinstance(caught.value.__cause__, OSError)
    assert h.calls == ["BEGIN", "ROLLBACK"]


def test_joined_ineligible_raises_without_completion():
    h = NativeHandle(open_=True)
    with pytest.raises(TransactionIntegrityError):
        with _owned_tx(h, precommit_probe=lambda c: False):
            pass
    assert h.calls == []


def test_owned_rollback_failure_keeps_original_error():
    rollback_error = OSError("rollback failed")
    h = NativeHandle(fail_on={"ROLLBACK": rollback_error})
    with pytest.raises(ValueError) as caught:
        with _owned_tx(h):
            raise ValueError("primary")
    assert str(caught.value) == "primary"
    assert caught.value.__context__ is rollback_error
    assert id(h) not in _ACTIVE


def test_commit_failure_calls_callback_once_without_retry():
    commit_error = OSError("commit lost")
    h = NativeHandle(fail_on={"COMMIT": commit_error})
    discarded = []
    with pytest.raises(OSError) as caught:
        with _owned_tx(h, on_commit_failure=lambda: discarded.append(True)):
            pass
    assert caught.value is commit_error
    assert discarded == [True]
    assert h.calls == ["BEGIN"]  # no retry, no ROLLBACK after an ambiguous COMMIT
    assert id(h) not in _ACTIVE


def test_protection_required_and_mark_poisoned():
    from mountainash_data.backends.ibis._transaction import mark_poisoned, protection_required
    h = NativeHandle()
    mark_poisoned(h)                          # not registered: no-op, creates nothing
    assert id(h) not in _ACTIVE
    assert protection_required(h) is False    # no scope
    with _tx(h):                              # other dialects: scope, but unprotected
        assert protection_required(h) is False
    with pytest.raises(TransactionPoisonedError):
        with _owned_tx(h):
            assert protection_required(h) is True
            mark_poisoned(h)
            with pytest.raises(TransactionPoisonedError):
                protection_required(h)
    assert h.calls == ["BEGIN", "COMMIT", "BEGIN", "ROLLBACK"]
