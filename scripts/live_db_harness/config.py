from __future__ import annotations

from collections.abc import Mapping
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from pydantic import SecretStr

from mountainash_auth_client import AUTH_REGISTRY, AuthProfile
from mountainash_data.core.settings import DATABASES_REGISTRY
from mountainash_settings import (
    FilesystemBackend,
    MountainAshBaseSettings,
    Profile,
    SecretReader,
    SettingsParameters,
    lookup_class_var,
)
from pydantic_settings import SettingsConfigDict

from .models import (
    BackendDefinition,
    HarnessError,
    HarnessSettings,
    Phase,
    SecretProviderDefinition,
    TargetDefinition,
)
from .sources import load_harness_settings


@dataclass(frozen=True)
class LoadedHarnessSettings:
    settings: HarnessSettings
    config_files: tuple[Path, ...]


@dataclass(frozen=True)
class BackendSelection:
    target_name: str
    backend_name: str
    target: TargetDefinition
    suite: BackendDefinition
    config_files: tuple[Path, ...]
    settings_parameters: SettingsParameters
    auth_profile: AuthProfile
    secret_values: frozenset[str]


def default_config_files(repo_root: Path, home: Path) -> tuple[Path, ...]:
    tracked = repo_root / "tests/config/live-db.toml"
    user = home / ".config/mountainash-data/live-db.toml"
    if not tracked.is_file():
        raise HarnessError(
            target=None,
            backend=None,
            phase=Phase.CONFIGURATION,
            detail=f"Missing tracked settings file: {tracked}",
            corrective_action="Restore tests/config/live-db.toml.",
        )
    paths = (tracked, user) if user.is_file() else (tracked,)
    return tuple(path.resolve() for path in paths)


def _validate_config_files(config_files: tuple[Path, ...]) -> tuple[Path, ...]:
    paths = tuple(Path(path) for path in config_files)
    for path in paths:
        if not path.is_file():
            raise HarnessError(
                target=None,
                backend=None,
                phase=Phase.CONFIGURATION,
                detail=f"Missing explicit settings file: {path}",
                corrective_action="Provide an existing configuration file.",
            )
    return paths


def load_unresolved_harness(
    config_files: tuple[Path, ...],
    *,
    selected_target: str,
    selected_backend: str | None,
) -> LoadedHarnessSettings:
    paths = _validate_config_files(config_files)
    try:
        settings = load_harness_settings(
            paths, selected_target=selected_target, selected_backend=selected_backend,
        )
    except HarnessError:
        raise
    except Exception:
        settings = None
    if settings is None:
        raise HarnessError(
            target=selected_target,
            backend=selected_backend,
            phase=Phase.CONFIGURATION,
            detail="Unable to load harness settings.",
            corrective_action=(
                "Fix the selected target and configuration files. For operator-managed "
                "endpoints use transport='direct' and remove all tunnel metadata."
            ),
        )
    return LoadedHarnessSettings(settings=settings, config_files=paths)


def walk_secret_scalars(value: object) -> Iterator[str]:
    if isinstance(value, SecretStr):
        yield value.get_secret_value()
    elif isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from walk_secret_scalars(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from walk_secret_scalars(item)


class TrackingSecretsBackend:
    def __init__(self, definition: SecretProviderDefinition, delegate: SecretReader) -> None:
        self.definition = definition
        self.delegate = delegate
        self.secret_values: set[str] = set()

    def get(self, key: str) -> dict[str, object] | None:
        record = self.delegate.get(key)
        if record is not None:
            self.secret_values.update(walk_secret_scalars(record))
        return record


def _normalized_provider_definition(definition: SecretProviderDefinition) -> SecretProviderDefinition:
    return definition.model_copy(update={"path": definition.path.expanduser().resolve()})


def selected_definition(
    settings: HarnessSettings,
    target_name: str,
    backend_name: str,
) -> SecretProviderDefinition:
    target = settings.targets[target_name]
    provider_name = target.secrets_provider
    if provider_name is None:
        raise HarnessError(
            target=target_name,
            backend=backend_name,
            phase=Phase.CONFIGURATION,
            detail="The selected target has no secrets provider.",
            corrective_action="Set secrets_provider to a declared filesystem provider.",
        )

    definition = settings.secret_providers.get(provider_name)
    if definition is None:
        raise HarnessError(
            target=target_name,
            backend=backend_name,
            phase=Phase.CONFIGURATION,
            detail=f"Unknown secrets provider: {provider_name}",
            corrective_action="Declare the selected secrets provider.",
        )
    return _normalized_provider_definition(definition)


def profile_parameter_names(profile_class: type[Profile]) -> frozenset[str]:
    profile_spec = lookup_class_var(profile_class, "__spec__")
    return frozenset(parameter.name for parameter in profile_spec.parameters)


def reject_unknown_keys(
    values: Mapping[str, object],
    profile_class: type[Profile],
    *,
    target: str,
    backend: str,
    section: str,
) -> None:
    unknown = sorted(set(values) - profile_parameter_names(profile_class))
    if unknown:
        raise HarnessError(
            target=target,
            backend=backend,
            phase=Phase.CONFIGURATION,
            detail=f"Unknown {section} field: {unknown[0]}",
            corrective_action=f"Use a registered {section} field.",
        )


def _plaintext_auth_field(value: object, *, path: str = "") -> str | None:
    if isinstance(value, SecretStr):
        raw = value.get_secret_value()
        return path if raw and not raw.startswith("secret:") else None
    if isinstance(value, str):
        return path if value and not value.startswith("secret:") else None
    if isinstance(value, Mapping):
        for key, item in value.items():
            field = _plaintext_auth_field(item, path=f"{path}.{key}" if path else str(key))
            if field is not None:
                return field
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            field = _plaintext_auth_field(item, path=f"{path}[{index}]")
            if field is not None:
                return field
    return None


def _validate_external_auth_values(
    target: TargetDefinition,
    values: Mapping[str, object],
    *,
    target_name: str,
    backend_name: str,
) -> None:
    if target.transport == "compose":
        return
    if target.secrets_provider is None:
        raise _selection_error(
            target=target_name,
            backend=backend_name,
            detail="The selected target has no secrets provider.",
            corrective_action="Set secrets_provider to a declared filesystem provider.",
        )
    field = _plaintext_auth_field(values)
    if field is not None:
        raise _selection_error(
            target=target_name,
            backend=backend_name,
            detail=f"External authentication field {field!r} must use a secret: reference.",
            corrective_action="Replace the literal authentication value with a secret: reference.",
        )


class SelectedBackendSettings(MountainAshBaseSettings):
    model_config = SettingsConfigDict(extra="forbid")

    connection: dict[str, object]
    auth_values: dict[str, object]


def _selection_error(
    *,
    target: str,
    backend: str,
    detail: str,
    corrective_action: str,
) -> HarnessError:
    return HarnessError(
        target=target,
        backend=backend,
        phase=Phase.CONFIGURATION,
        detail=detail,
        corrective_action=corrective_action,
    )


def build_backend_selection(loaded: LoadedHarnessSettings) -> BackendSelection:
    settings = loaded.settings
    target_name = settings.selected_target
    backend_name = settings.selected_backend
    if target_name is None:
        raise _selection_error(
            target="<none>",
            backend=backend_name or "<none>",
            detail="No target was selected.",
            corrective_action="Select a target before running a backend.",
        )
    if backend_name is None:
        raise _selection_error(
            target=target_name,
            backend="<none>",
            detail="No backend was selected.",
            corrective_action="Select a backend before running.",
        )

    target = settings.targets[target_name]
    target_backend = target.backends.get(backend_name)
    suite = settings.backends.get(backend_name)
    if target_backend is None or suite is None:
        raise _selection_error(
            target=target_name,
            backend=backend_name,
            detail="The selected backend is not configured for the target.",
            corrective_action="Select a backend listed for the target.",
        )
    if not suite.runnable:
        raise _selection_error(
            target=target_name,
            backend=backend_name,
            detail=f"Backend {backend_name!r} is unavailable.",
            corrective_action=f"Complete backlog item {suite.backlog or '<none>'} first.",
        )
    try:
        backend_class = DATABASES_REGISTRY.get_settings_class(suite.settings_profile)
        backend_spec = DATABASES_REGISTRY.get_spec(suite.settings_profile)
    except KeyError:
        backend_class = None
        backend_spec = None
    if backend_class is None or backend_spec is None:
        raise _selection_error(
            target=target_name,
            backend=backend_name,
            detail=f"Unknown database profile: {suite.settings_profile}",
            corrective_action="Use a registered database profile.",
        )
    try:
        auth_class = AUTH_REGISTRY.get_settings_class(target_backend.auth.profile)
    except KeyError:
        auth_class = None
    if auth_class is None:
        raise _selection_error(
            target=target_name,
            backend=backend_name,
            detail=f"Unknown authentication profile: {target_backend.auth.profile}",
            corrective_action="Use a registered authentication profile.",
        )

    if auth_class not in backend_spec.supported_auth:
        raise _selection_error(
            target=target_name,
            backend=backend_name,
            detail=(
                f"Database profile {suite.settings_profile!r} does not support "
                f"authentication profile {target_backend.auth.profile!r}."
            ),
            corrective_action="Use a supported authentication profile.",
        )

    reject_unknown_keys(
        target_backend.connection,
        backend_class,
        target=target_name,
        backend=backend_name,
        section="connection",
    )
    reject_unknown_keys(
        target_backend.auth.values,
        auth_class,
        target=target_name,
        backend=backend_name,
        section="authentication",
    )
    _validate_external_auth_values(
        target,
        target_backend.auth.values,
        target_name=target_name,
        backend_name=backend_name,
    )

    definition = (
        selected_definition(settings, target_name, backend_name)
        if target.secrets_provider is not None else None
    )
    detail = "Unable to resolve credentials for the selected backend."
    corrective_action = "Check the selected secrets provider and secret records."
    try:
        with ExitStack() as stores:
            tracker = None
            if definition is not None:
                raw_store = stores.enter_context(FilesystemBackend(definition.path))
                tracker = TrackingSecretsBackend(definition, raw_store)
            selection_parameters = SettingsParameters.create(
                settings_class=SelectedBackendSettings,
                secret_store=tracker,
                connection=target_backend.connection,
                auth_values=target_backend.auth.values,
            )
            resolved_selection = selection_parameters.get_settings()

            detail = "Unable to construct the selected authentication profile."
            corrective_action = "Fix the registered authentication fields."
            auth_profile = auth_class(**resolved_selection.auth_values)

            detail = "Unable to construct the selected connection profile."
            corrective_action = "Fix the registered connection fields."
            parameters = SettingsParameters.create(
                settings_class=backend_class,
                env_prefix="MOUNTAINASH_LIVE_DB_PROFILE_",
                **resolved_selection.connection,
            )
            parameters.get_settings()
            selection = BackendSelection(
                target_name=target_name,
                backend_name=backend_name,
                target=target,
                suite=suite,
                config_files=loaded.config_files,
                settings_parameters=parameters,
                auth_profile=auth_profile,
                secret_values=frozenset(tracker.secret_values if tracker else ()),
            )
    except Exception:
        selection = None
    # Raise after leaving the handler so secret-bearing exceptions cannot become
    # the cause or context of the fixed user-facing configuration diagnostic.
    if selection is None:
        raise _selection_error(
            target=target_name,
            backend=backend_name,
            detail=detail,
            corrective_action=corrective_action,
        )
    return selection


def require_destructive_consent(selection: BackendSelection) -> None:
    if selection.target.transport != "compose" and not selection.target.allow_destructive_tests:
        raise HarnessError(
            selection.target_name, selection.backend_name, Phase.CONFIGURATION,
            "The external target does not allow destructive tests.",
            "Set allow_destructive_tests = true only for an authorized disposable target.",
        )
