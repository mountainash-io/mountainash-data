from __future__ import annotations

from pathlib import Path

import pytest

from scripts.live_db_harness.cli import build_parser


def test_every_command_requires_target() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["status"])


def test_backend_and_all_are_mutually_exclusive() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["check", "--target", "docker", "postgres", "--all"])


def test_config_option_is_accepted_before_command(tmp_path: Path) -> None:
    parser = build_parser()
    args = parser.parse_args(["--config", str(tmp_path / "config.toml"), "status", "--target", "docker"])
    assert args.config == [str(tmp_path / "config.toml")]
