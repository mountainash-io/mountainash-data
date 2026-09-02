from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from mountainash_data.backends.ibis._exasol_compat import patch_exasol_connection


class _FakeExasolConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[object, tuple[object, ...], dict[str, object]]] = []
        self.result = object()

    @contextmanager
    def _safe_raw_sql(
        self,
        query: object,
        *args: object,
        **kwargs: object,
    ) -> Iterator[object]:
        self.calls.append((query, args, kwargs))
        yield self.result


def test_patch_exposes_committed_raw_sql_and_is_idempotent() -> None:
    connection = _FakeExasolConnection()

    assert patch_exasol_connection(connection) is connection
    raw_sql = connection.raw_sql  # type: ignore[attr-defined]
    assert raw_sql("RENAME TABLE old TO new", 7, flag=True) is connection.result
    assert connection.calls == [
        ("RENAME TABLE old TO new", (7,), {"flag": True})
    ]

    assert patch_exasol_connection(connection) is connection
    assert connection.raw_sql is raw_sql  # type: ignore[attr-defined]
