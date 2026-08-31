from __future__ import annotations

from pathlib import Path

import pytest

import scripts.live_db_harness.cli as cli
from scripts.live_db_harness.cli import build_parser, main
from scripts.live_db_harness.runner import LiveDbRunner

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
 
 
CONFIG = """
[backends.postgres]
settings_profile = "postgresql"
selector = "postgres"
runnable = true

[backends.mysql]
settings_profile = "mysql"
selector = "mysql"
runnable = true

[backends.mssql]
settings_profile = "mssql"
selector = "mssql"
runnable = false
backlog = "DEBT-17"
[secret_providers.unreadable]
type = "filesystem"
path = "/this/path/does/not/exist"

[targets.local]
transport = "direct"
secrets_provider = "unreadable"

[targets.local.backends.postgres.connection]
HOST = "127.0.0.1"
PORT = 5432

[targets.local.backends.postgres.auth]
profile = "password"

[targets.local.backends.postgres.auth.values]
USERNAME = "postgres"
PASSWORD = "secret:password"

[targets.local.backends.mysql.connection]
HOST = "127.0.0.1"
PORT = 3306

[targets.local.backends.mysql.auth]
profile = "password"

[targets.local.backends.mysql.auth.values]
USERNAME = "mysql"
PASSWORD = "mysql"

[targets.local.backends.mssql.connection]
HOST = "127.0.0.1"
PORT = 1433

[targets.local.backends.mssql.auth]
profile = "password"

[targets.local.backends.mssql.auth.values]
USERNAME = "mssql"
PASSWORD = "mssql"
"""


def _config(path: Path, body: str = CONFIG) -> Path:
    path.write_text(body, encoding="utf-8")
    return path
class _FakeRunner:
    instances: list["_FakeRunner"] = []

    def __init__(self, config_files: tuple[Path, ...]) -> None:
        self.config_files = config_files
        self.redactor = lambda value: value
        self.calls: list[tuple[str, str]] = []
        self.instances.append(self)

    def status(self, target: str) -> int:
        self.calls.append(("status", target))
        return 0


def test_up_and_down_reject_all() -> None:
    parser = build_parser()
    for command in ("up", "down"):
        args = parser.parse_args([command, "--target", "docker", "--all"])
        assert cli._validate_args(parser, args) == f"{command} does not support --all"


def test_compose_only_commands_reject_external_target_without_secrets_provider(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = _config(tmp_path / "config.toml", CONFIG.replace('secrets_provider = "unreadable"\n', ""))
    for command in ("up", "down", "run"):
        backend_args = [command, "--target", "local", "postgres"]
        code = main(["--config", str(config), *backend_args])
        assert code == 2
        assert "no secrets provider" in capsys.readouterr().out


def test_direct_unavailable_backend_is_usage_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = _config(tmp_path / "config.toml")
    code = main(["--config", str(config), "check", "--target", "local", "mssql"])
    assert code == 2
    assert "unavailable" in capsys.readouterr().out.lower()


def test_status_does_not_resolve_secrets_or_connect(tmp_path: Path) -> None:
    config = _config(tmp_path / "config.toml")
    connect_attempted = False

    def forbidden(*args: object, **kwargs: object) -> None:
        nonlocal connect_attempted
        connect_attempted = True
        raise AssertionError("status must not connect")

    runner = LiveDbRunner((config,), backend_factory=forbidden)
    assert runner.status("local") == 0
    assert not connect_attempted


def test_wait_lock_is_available_on_backend_operations() -> None:
    parser = build_parser()
    for command in ("check", "test", "up", "down", "run"):
        args = parser.parse_args([command, "--target", "docker", "postgres", "--wait-lock", "2"])
        assert args.wait_lock == 2


def test_jobs_and_fail_fast_require_all() -> None:
    parser = build_parser()
    for option in (["--jobs", "2"], ["--fail-fast"]):
        args = parser.parse_args(["test", "--target", "docker", "postgres", *option])
        assert cli._validate_args(parser, args) == "--jobs and --fail-fast require --all"


def test_config_options_replace_default_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = _config(tmp_path / "explicit.toml")
    _FakeRunner.instances.clear()
    monkeypatch.setattr(cli, "LiveDbRunner", _FakeRunner)
    monkeypatch.setattr(cli, "default_config_files", lambda *args: (_ for _ in ()).throw(AssertionError("defaults used")))
    assert main(["--config", str(config), "status", "--target", "docker"]) == 0
    assert _FakeRunner.instances[-1].config_files == (config.resolve(),)


def test_repeated_config_preserves_argument_order(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    first = _config(tmp_path / "first.toml")
    second = _config(tmp_path / "second.toml")
    _FakeRunner.instances.clear()
    monkeypatch.setattr(cli, "LiveDbRunner", _FakeRunner)
    assert main(["--config", str(second), "--config", str(first), "status", "--target", "docker"]) == 0
    assert _FakeRunner.instances[-1].config_files == (second.resolve(), first.resolve())


def test_config_paths_become_absolute(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = _config(tmp_path / "config.toml")
    monkeypatch.chdir(tmp_path)
    _FakeRunner.instances.clear()
    monkeypatch.setattr(cli, "LiveDbRunner", _FakeRunner)
    assert main(["--config", "config.toml", "status", "--target", "docker"]) == 0
    assert _FakeRunner.instances[-1].config_files == (config.resolve(),)


def test_missing_explicit_config_is_usage_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    missing = tmp_path / "missing.toml"
    assert main(["--config", str(missing), "status", "--target", "docker"]) == 2
    assert "Missing explicit settings file" in capsys.readouterr().out
