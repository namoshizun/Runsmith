from __future__ import annotations

from dataclasses import dataclass

import pytest

from pycrew.settings import CrewSettings


def test_settings_use_dataclass_defaults_when_env_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYCREW_SUPERVISION_INTERVAL", raising=False)
    monkeypatch.delenv("PYCREW_WORKER_RESTART_QUOTA", raising=False)
    monkeypatch.delenv("PYCREW_SUPERVISOR_RESTART_QUOTA", raising=False)

    settings = CrewSettings()

    assert settings.supervision_interval == 0.25
    assert settings.worker_restart_quota == 3
    assert settings.supervisor_restart_quota == 3


def test_settings_read_values_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYCREW_SUPERVISION_INTERVAL", "0.25")
    monkeypatch.setenv("PYCREW_WORKER_RESTART_QUOTA", "5")
    monkeypatch.setenv("PYCREW_SUPERVISOR_RESTART_QUOTA", "7")

    settings = CrewSettings()

    assert settings.supervision_interval == 0.25
    assert settings.worker_restart_quota == 5
    assert settings.supervisor_restart_quota == 7


def test_explicit_overrides_take_precedence_over_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYCREW_SUPERVISION_INTERVAL", "0.25")
    monkeypatch.setenv("PYCREW_WORKER_RESTART_QUOTA", "5")
    monkeypatch.setenv("PYCREW_SUPERVISOR_RESTART_QUOTA", "7")

    settings = CrewSettings(
        supervision_interval=1.5,
        worker_restart_quota=9,
        supervisor_restart_quota=11,
    )

    assert settings.supervision_interval == 1.5
    assert settings.worker_restart_quota == 9
    assert settings.supervisor_restart_quota == 11


@dataclass(frozen=True, slots=True, init=False)
class ExtendedCrewSettings(CrewSettings):
    is_enabled: bool = False
    label: str = "default"


def test_settings_coerce_bool_and_string_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYCREW_IS_ENABLED", "true")
    monkeypatch.setenv("PYCREW_LABEL", "workers")

    settings = ExtendedCrewSettings()

    assert settings.is_enabled is True
    assert settings.label == "workers"


@pytest.mark.parametrize(("raw_value", "expected"), [("true", True), ("false", False)])
def test_settings_parse_boolean_literals(
    monkeypatch: pytest.MonkeyPatch,
    raw_value: str,
    expected: bool,
) -> None:
    monkeypatch.setenv("PYCREW_IS_ENABLED", raw_value)

    settings = ExtendedCrewSettings()

    assert settings.is_enabled is expected


def test_invalid_boolean_literal_raises_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYCREW_IS_ENABLED", "maybe")

    with pytest.raises(ValueError, match="Cannot coerce 'maybe' to bool for setting 'is_enabled'"):
        ExtendedCrewSettings()


def test_unknown_override_raises_type_error() -> None:
    with pytest.raises(TypeError, match="Unknown settings: mystery"):
        CrewSettings(mystery=1)


@dataclass(frozen=True, slots=True, init=False)
class UnsupportedSettings(CrewSettings):
    tags: tuple[str, ...] = ()


def test_unsupported_annotation_type_raises_type_error() -> None:
    with pytest.raises(TypeError, match="Unsupported setting type"):
        UnsupportedSettings()
