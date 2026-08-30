from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Iterator
from dataclasses import dataclass

from pydantic import SecretStr

from mountainash_auth_client import AUTH_REGISTRY, AuthProfile
from mountainash_data.core.settings import DATABASES_REGISTRY
from mountainash_secrets import FilesystemSecretStore
from mountainash_settings import (
    MountainAshBaseSettings,
    Profile,
    SecretsBackend,
    SettingsParameters,
    get_secrets_backend,
    lookup_class_var,
    register_secrets_backend,
)
from pydantic_settings import SettingsConfigDict

from .models import (
    BackendDefinition,
    HarnessError,
    HarnessSettings,
    Phase,
    SecretProviderDefinition,
    TargetBackendDefinition,
    TargetDefinition,
)


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
    parameters = SettingsParameters.create(
        settings_class=HarnessSettings,
        config_files=paths,
        selected_target=selected_target,
        selected_backend=selected_backend,
    )
    try:
        settings = parameters.get_settings()
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
            corrective_action="Fix the selected target and configuration files.",
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
    def __init__(self, definition: SecretProviderDefinition, delegate: SecretsBackend) -> None:
        self.definition = definition
        self.delegate = delegate
        self.secret_values: set[str] = set()

    def get(self, key: str) -> dict[str, object] | None:
        record = self.delegate.get(key)
        if record is not None:
            self.secret_values.update(walk_secret_scalars(record))
        return record

    def set(self, key: str, data: dict[str, object]) -> None:
        self.delegate.set(key, data)

    def delete(self, key: str) -> None:
        self.delegate.delete(key)

    def transaction(self, key: str) -> AbstractContextManager[None]:
        return self.delegate.transaction(key)


def _normalized_provider_definition(definition: SecretProviderDefinition) -> SecretProviderDefinition:
    return definition.model_copy(update={"path": definition.path.expanduser().resolve()})


def _provider_backend(
    settings: HarnessSettings,
    *,
    target_name: str,
    backend_name: str,
) -> TrackingSecretsBackend:
    target = settings.targets[target_name]
    provider_name = target.secrets_provider
    if provider_name is None:
        raise HarnessError(
            target=target_name,
            backend=backend_name,
            phase=Phase.CONFIGURATION,
            detail="The selected target has no secrets provider.",
            corrective_action="Set secrets_provider to a registered filesystem provider.",
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
    normalized = _normalized_provider_definition(definition)
    try:
        existing = get_secrets_backend(provider_name)
    except KeyError:
        existing = None
    if existing is not None:
        existing_definition = getattr(existing, "definition", None)
        if isinstance(existing_definition, SecretProviderDefinition):
            existing_definition = _normalized_provider_definition(existing_definition)
        if existing_definition != normalized:
            raise HarnessError(
                target=target_name,
                backend=backend_name,
                phase=Phase.CONFIGURATION,
                detail=f"Secrets provider {provider_name!r} has a different definition.",
                corrective_action="Use one definition for each secrets provider name.",
            )
        if isinstance(existing, TrackingSecretsBackend):
            return existing
        raise HarnessError(
            target=target_name,
            backend=backend_name,
            phase=Phase.CONFIGURATION,
            detail=f"Secrets provider {provider_name!r} is already registered without its definition.",
            corrective_action="Clear the provider registry and register the declared filesystem provider.",
        )

    delegate = FilesystemSecretStore(normalized.path)
    backend = TrackingSecretsBackend(normalized, delegate)
    register_secrets_backend(provider_name, backend)
    return backend


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
        backend_spec = DATABASES_REGISTRY.get_descriptor(suite.settings_profile)
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

    provider_name = target.secrets_provider
    tracking_backend: TrackingSecretsBackend | None = None
    if provider_name is not None:
        tracking_backend = _provider_backend(
            settings,
            target_name=target_name,
            backend_name=backend_name,
        )

    selection_parameters = SettingsParameters.create(
        settings_class=SelectedBackendSettings,
        secrets_provider=provider_name,
        connection=target_backend.connection,
        auth_values=target_backend.auth.values,
    )
    try:
        resolved_selection = selection_parameters.get_settings()
    except Exception:
        resolved_selection = None
    if resolved_selection is None:
        raise _selection_error(
            target=target_name,
            backend=backend_name,
            detail="Unable to resolve credentials for the selected backend.",
            corrective_action="Check the selected secrets provider and secret records.",
        )

    parameters = SettingsParameters.create(
        settings_class=backend_class,
        env_prefix="MOUNTAINASH_LIVE_DB_PROFILE_",
        **resolved_selection.connection,
    )
    try:
        auth_profile = auth_class(**resolved_selection.auth_values)
    except Exception:
        auth_profile = None
    if auth_profile is None:
        raise _selection_error(
            target=target_name,
            backend=backend_name,
            detail="Unable to construct the selected authentication profile.",
            corrective_action="Fix the registered authentication fields.",
        )

    return BackendSelection(
        target_name=target_name,
        backend_name=backend_name,
        target=target,
        suite=suite,
        config_files=loaded.config_files,
        settings_parameters=parameters,
        auth_profile=auth_profile,
        secret_values=frozenset(tracking_backend.secret_values if tracking_backend else ()),
    )
