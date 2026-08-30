from __future__ import annotations

from pathlib import Path

from scripts.live_db_harness.models import HarnessSettings


POSTGRES_SUITE = '''[backends.postgres]
settings_profile = "postgresql"
selector = "postgres"
runnable = true
compose = { profile = "postgres", service = "postgres" }
'''

DOCKER_TARGET = '''[targets.docker]
transport = "compose"
max_parallel = 1
test_timeout_seconds = 30

[targets.docker.backends.postgres.connection]
HOST = "127.0.0.1"
PORT = 5432

[targets.docker.backends.postgres.auth]
profile = "password"
'''

MPNAS_TARGET = '''[targets.mpnas]
transport = "ssh-tunnel"
max_parallel = 2
test_timeout_seconds = 30

[targets.mpnas.backends.postgres.connection]
HOST = "127.0.0.1"
PORT = 5432

[targets.mpnas.backends.postgres.auth]
profile = "password"

[targets.mpnas.backends.postgres.tunnel]
launchd_label = "com.example.postgres"
ssh_destination = "db.example"
local_host = "127.0.0.1"
local_port = 5432
remote_host = "postgres.internal"
remote_port = 5432
process_ancestry = ["launchd"]
'''


def test_tracked_and_user_files_add_targets(tmp_path: Path):
    tracked = tmp_path / "tracked.toml"
    user = tmp_path / "user.toml"
    tracked.write_text(POSTGRES_SUITE + DOCKER_TARGET)
    user.write_text(MPNAS_TARGET)

    settings = HarnessSettings(config_files=[tracked, user])

    assert set(settings.targets) == {"docker", "mpnas"}


def test_user_scalar_and_list_values_replace_tracked_values(tmp_path: Path):
    tracked = tmp_path / "tracked.toml"
    user = tmp_path / "user.toml"
    tracked.write_text(POSTGRES_SUITE + DOCKER_TARGET + MPNAS_TARGET)
    user.write_text(
        '''[targets.docker]
max_parallel = 3

[targets.mpnas.backends.postgres.tunnel]
process_ancestry = ["launchd", "autossh", "ssh"]
'''
    )

    settings = HarnessSettings(config_files=[tracked, user])

    assert settings.targets["docker"].max_parallel == 3
    assert settings.targets["mpnas"].backends["postgres"].tunnel.process_ancestry == (
        "launchd", "autossh", "ssh"
    )


def test_keyword_target_selection_wins_over_file_values(tmp_path: Path):
    tracked = tmp_path / "tracked.toml"
    user = tmp_path / "user.toml"
    tracked.write_text(POSTGRES_SUITE + DOCKER_TARGET)
    user.write_text('selected_target = "mpnas"\nselected_backend = "postgres"\n' + MPNAS_TARGET)

    settings = HarnessSettings(
        config_files=[tracked, user], selected_target="docker", selected_backend="postgres"
    )

    assert settings.selected_target == "docker"
    assert settings.selected_backend == "postgres"
