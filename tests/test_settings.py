from __future__ import annotations

from dataclasses import dataclass

import pytest

from runsmith.settings import RunsmithSettings


def test_settings_use_dataclass_defaults_when_env_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RUNSMITH_SUPERVISION_INTERVAL", raising=False)
    monkeypatch.delenv("RUNSMITH_WORKER_RESTART_QUOTA", raising=False)
    monkeypatch.delenv("RUNSMITH_SUPERVISOR_RESTART_QUOTA", raising=False)

    settings = RunsmithSettings()

    assert settings.supervision_interval == 0.25
    assert settings.worker_restart_quota == 3
    assert settings.supervisor_restart_quota == 3


def test_settings_read_values_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNSMITH_SUPERVISION_INTERVAL", "0.25")
    monkeypatch.setenv("RUNSMITH_WORKER_RESTART_QUOTA", "5")
    monkeypatch.setenv("RUNSMITH_SUPERVISOR_RESTART_QUOTA", "7")

    settings = RunsmithSettings()

    assert settings.supervision_interval == 0.25
    assert settings.worker_restart_quota == 5
    assert settings.supervisor_restart_quota == 7


def test_explicit_overrides_take_precedence_over_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RUNSMITH_SUPERVISION_INTERVAL", "0.25")
    monkeypatch.setenv("RUNSMITH_WORKER_RESTART_QUOTA", "5")
    monkeypatch.setenv("RUNSMITH_SUPERVISOR_RESTART_QUOTA", "7")

    settings = RunsmithSettings(
        supervision_interval=1.5,
        worker_restart_quota=9,
        supervisor_restart_quota=11,
    )

    assert settings.supervision_interval == 1.5
    assert settings.worker_restart_quota == 9
    assert settings.supervisor_restart_quota == 11


@dataclass(frozen=True, slots=True, init=False)
class ExtendedRunsmithSettings(RunsmithSettings):
    is_enabled: bool = False
    label: str = "default"


def test_settings_coerce_bool_and_string_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNSMITH_IS_ENABLED", "true")
    monkeypatch.setenv("RUNSMITH_LABEL", "workers")

    settings = ExtendedRunsmithSettings()

    assert settings.is_enabled is True
    assert settings.label == "workers"


@pytest.mark.parametrize(("raw_value", "expected"), [("true", True), ("false", False)])
def test_settings_parse_boolean_literals(
    monkeypatch: pytest.MonkeyPatch,
    raw_value: str,
    expected: bool,
) -> None:
    monkeypatch.setenv("RUNSMITH_IS_ENABLED", raw_value)

    settings = ExtendedRunsmithSettings()

    assert settings.is_enabled is expected


def test_invalid_boolean_literal_raises_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNSMITH_IS_ENABLED", "maybe")

    with pytest.raises(ValueError, match="Cannot coerce 'maybe' to bool for setting 'is_enabled'"):
        ExtendedRunsmithSettings()


def test_unknown_override_raises_type_error() -> None:
    with pytest.raises(TypeError, match="Unknown settings: mystery"):
        RunsmithSettings(mystery=1)


@dataclass(frozen=True, slots=True, init=False)
class UnsupportedSettings(RunsmithSettings):
    tags: tuple[str, ...] = ()


def test_unsupported_annotation_type_raises_type_error() -> None:
    with pytest.raises(TypeError, match="Unsupported setting type"):
        UnsupportedSettings()
