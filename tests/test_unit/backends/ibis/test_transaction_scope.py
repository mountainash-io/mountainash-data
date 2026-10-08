"""Explicit transactions on real SQLite and DuckDB through the public API."""

from __future__ import annotations

import pytest

from mountainash_data import IbisBackend
from tests.helpers import transaction_cases as cases

pytestmark = pytest.mark.unit


@pytest.fixture(params=["sqlite", "duckdb"])
def backend(request):
    b = IbisBackend(dialect=request.param, database=":memory:").connect()
    try:
        yield b
    finally:
        if b.in_transaction():
            pytest.fail("test left a registered transaction scope open")
        b.close()


def test_predicate_tracks_caller(backend):
    cases.case_predicate_tracks_caller(backend)


def test_predicate_false_when_unconnected_or_closed():
    b = IbisBackend(dialect="sqlite", database=":memory:")
    assert b.native_transaction_open() is False
    b.connect().close()
    assert b.native_transaction_open() is False


def test_owned_commit_and_rollback(backend):
    cases.case_owned_commit_and_rollback(backend)


@pytest.mark.parametrize("completion", ["COMMIT", "ROLLBACK"])
def test_joined_scope_leaves_completion_to_caller(backend, completion):
    cases.case_joined_scope_leaves_completion_to_caller(backend, completion)


def test_two_wrappers_share_one_scope(backend):
    other = IbisBackend.from_ibis_connection(backend.ibis_connection(), dialect=backend.dialect)
    cases.case_two_wrappers_share_one_scope(backend, other)


def test_close_inside_scope_refused(backend):
    cases.case_close_inside_scope_refused(backend)
