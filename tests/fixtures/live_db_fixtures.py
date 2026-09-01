"""Pytest fixtures for live databases selected by the harness runner."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from mountainash_data import IbisBackend

from scripts.live_db_harness.config import (
    BackendSelection,
    build_backend_selection,
    load_unresolved_harness,
)


_NO_TARGET_MESSAGE = "no live backend target selected"


def _required_run() -> bool:
    return os.environ.get("MOUNTAINASH_REQUIRE_LIVE_DB") == "1"


def _skip_or_fail(message: str) -> None:
    if _required_run():
        pytest.fail(message)
    pytest.skip(message)


def _config_files_from_environment() -> tuple[Path, ...]:
    raw = os.environ.get("MOUNTAINASH_LIVE_DB_CONFIG")
    if raw is None:
        raise ValueError(
            "MOUNTAINASH_LIVE_DB_CONFIG must be a JSON array of absolute paths"
        )
    try:
        values = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "MOUNTAINASH_LIVE_DB_CONFIG must be a JSON array of absolute paths"
        ) from exc
    if (
        not isinstance(values, list)
        or not values
        or any(not isinstance(value, str) for value in values)
    ):
        raise ValueError(
            "MOUNTAINASH_LIVE_DB_CONFIG must be a JSON array of absolute paths"
        )
    paths = tuple(Path(value) for value in values)
    if any(not path.is_absolute() for path in paths):
        raise ValueError(
            "MOUNTAINASH_LIVE_DB_CONFIG must be a JSON array of absolute paths"
        )
    return paths


def _selected_context() -> tuple[str, str]:
    target = os.environ.get("MOUNTAINASH_LIVE_DB_TARGET")
    if not target:
        _skip_or_fail(_NO_TARGET_MESSAGE)

    backend = os.environ.get("MOUNTAINASH_LIVE_DB_BACKEND")
    if not backend:
        _skip_or_fail("no live backend selected")
    return target, backend


def load_fixture_selection_from_environment() -> BackendSelection:
    """Load and resolve the selected backend in the current pytest process."""
    target, backend = _selected_context()
    config_files = _config_files_from_environment()
    loaded = load_unresolved_harness(
        config_files,
        selected_target=target,
        selected_backend=backend,
    )
    return build_backend_selection(loaded)


def _selection_for_fixture(
    fixture_name: str,
    expected_backend: str,
) -> BackendSelection:
    target, selected_backend = _selected_context()
    if selected_backend != expected_backend:
        _skip_or_fail(
            f"{fixture_name} does not match selected backend {selected_backend!r}"
        )

    config_files = _config_files_from_environment()
    loaded = load_unresolved_harness(
        config_files,
        selected_target=target,
        selected_backend=selected_backend,
    )
    return build_backend_selection(loaded)


def _connected_backend(
    fixture_name: str,
    expected_backend: str,
) -> Iterator[IbisBackend]:
    selection = _selection_for_fixture(fixture_name, expected_backend)
    backend = IbisBackend(selection.settings_parameters)
    try:
        backend.connect(auth_profile=selection.auth_profile)
        yield backend
    finally:
        backend.close()


@pytest.fixture
def live_backend_selection() -> BackendSelection:
    """Return the resolved backend selection passed from the harness runner."""
    return load_fixture_selection_from_environment()


@pytest.fixture
def postgres_backend() -> Iterator[IbisBackend]:
    yield from _connected_backend("postgres_backend", "postgres")


@pytest.fixture
def mysql_backend() -> Iterator[IbisBackend]:
    yield from _connected_backend("mysql_backend", "mysql")


@pytest.fixture
def oracle_backend() -> Iterator[IbisBackend]:
    yield from _connected_backend("oracle_backend", "oracle")


@pytest.fixture
def singlestore_backend() -> Iterator[IbisBackend]:
    yield from _connected_backend("singlestore_backend", "singlestoredb")
