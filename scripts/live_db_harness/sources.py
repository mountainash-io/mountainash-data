"""Invocation-local, non-resolving sources for harness control configuration."""

from pathlib import Path
from typing import Any

from mountainash_settings.settings_parameters.filehandler import SettingsFileHandler
from pydantic_settings import (
    DotEnvSettingsSource,
    JsonConfigSettingsSource,
    TomlConfigSettingsSource,
    YamlConfigSettingsSource,
)

from .models import HarnessSettings


def load_harness_settings(
    config_files: tuple[Path, ...],
    *,
    selected_target: str | None = None,
    selected_backend: str | None = None,
) -> HarnessSettings:
    """Load control values without interpreting any secret references."""
    paths = tuple(str(path) for path in config_files)
    SettingsFileHandler.validate_config_files_exist(paths)
    files = SettingsFileHandler.separate_config_files(paths)
    # pydantic-settings sources take str/Path; the inputs are already local path strings.
    env_files = tuple(str(path) for path in files.env_files)
    yaml_files = tuple(str(path) for path in files.yaml_files)
    toml_files = tuple(str(path) for path in files.toml_files)
    json_files = tuple(str(path) for path in files.json_files)

    class ConfiguredHarnessSettings(HarnessSettings):
        @classmethod
        def settings_customise_sources(
            cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings,
        ):
            return (
                init_settings,
                env_settings,
                dotenv_settings,
                # The primary source retains unknown keys for extra="forbid".
                # This fallback must not re-emit its consumed prefixed keys.
                DotEnvSettingsSource(
                    settings_cls, env_file=env_files, env_prefix="", env_file_encoding="utf-8",
                    case_sensitive=True, env_ignore_empty=True, env_parse_none_str="None",
                    dotenv_filtering="only_existing",
                ),
                YamlConfigSettingsSource(settings_cls, yaml_file=yaml_files, deep_merge=True),
                TomlConfigSettingsSource(settings_cls, toml_file=toml_files, deep_merge=True),
                JsonConfigSettingsSource(settings_cls, json_file=json_files, deep_merge=True),
                file_secret_settings,
            )

    selections: dict[str, Any] = {}
    if selected_target is not None:
        selections["selected_target"] = selected_target
    if selected_backend is not None:
        selections["selected_backend"] = selected_backend
    return ConfiguredHarnessSettings(
        **selections,
        _env_file=env_files,
        _env_file_encoding="utf-8",
        _case_sensitive=True,
        _env_ignore_empty=True,
        _env_parse_none_str="None",
    )
