from __future__ import annotations

import os
from dataclasses import MISSING, dataclass, field, fields
from typing import Any, ClassVar, get_args, get_type_hints

PrimitiveValue = bool | int | float | str
PrimitiveType = type[bool] | type[int] | type[float] | type[str]

_PRIMITIVE_TYPES: tuple[type, ...] = (bool, int, float, str)
_BOOL_TRUE: frozenset[str] = frozenset({"true", "1", "yes"})
_BOOL_FALSE: frozenset[str] = frozenset({"false", "0", "no"})


def _extract_primitive_type(annotation: Any) -> PrimitiveType:
    if annotation in _PRIMITIVE_TYPES:
        return annotation

    non_none = [t for t in get_args(annotation) if t is not type(None)]
    if len(non_none) == 1 and non_none[0] in _PRIMITIVE_TYPES:
        return non_none[0]

    raise TypeError(f"Unsupported setting type: {annotation}")


def _coerce(value: PrimitiveValue, expected: PrimitiveType, name: str) -> PrimitiveValue:
    if expected is bool:
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in _BOOL_TRUE:
            return True
        if normalized in _BOOL_FALSE:
            return False
        raise ValueError(f"Cannot coerce {value!r} to bool for setting '{name}'")

    try:
        return expected(value)
    except (ValueError, TypeError) as err:
        raise ValueError(
            f"Cannot coerce {value!r} to {expected.__name__} for setting '{name}'"
        ) from err


@dataclass(frozen=True, slots=True, init=False)
class Settings:
    """Base settings resolved from: overrides → environment → field defaults."""

    _env_prefix: ClassVar[str] = ""

    def __init__(self, **overrides: PrimitiveValue) -> None:
        known_fields = fields(self)
        _reject_unknown(overrides, known_fields)
        hints = get_type_hints(type(self))

        for setting_field in known_fields:
            expected_type = _extract_primitive_type(hints[setting_field.name])
            raw = _resolve_value(setting_field, overrides, type(self)._env_prefix)
            object.__setattr__(
                self,
                setting_field.name,
                _coerce(raw, expected_type, setting_field.name),
            )


def _reject_unknown(
    overrides: dict[str, PrimitiveValue],
    known_fields: tuple[Any, ...],
) -> None:
    known_names = {f.name for f in known_fields}
    unknown = sorted(set(overrides) - known_names)
    if unknown:
        raise TypeError(f"Unknown settings: {', '.join(unknown)}")


def _resolve_value(
    field: Any,
    overrides: dict[str, PrimitiveValue],
    env_prefix: str,
) -> PrimitiveValue:
    if field.name in overrides:
        return overrides[field.name]

    env_key = f"{env_prefix}{field.name}".upper()
    env_value = os.getenv(env_key)
    if env_value is not None:
        return env_value

    if field.default is MISSING:
        raise TypeError(f"No value provided for required setting: '{field.name}'")

    return field.default


@dataclass(frozen=True, slots=True, init=False)
class RunsmithSettings(Settings):
    _env_prefix: ClassVar[str] = "RUNSMITH_"

    supervision_interval: float = field(
        default=0.25,
        metadata={
            "doc": "Seconds between supervisor health-check cycles.",
            "group": "Supervision cadence",
        },
    )
    supervisor_restart_quota: int = field(
        default=3,
        metadata={
            "doc": "Maximum restarts for a child supervisor node before escalation.",
            "group": "Restart policy",
        },
    )
    worker_restart_quota: int = field(
        default=3,
        metadata={
            "doc": "Maximum restarts for a leaf worker before escalation.",
            "group": "Restart policy",
        },
    )
    activity_queue_maxsize: int = field(
        default=100,
        metadata={
            "doc": "Maximum buffered WorkerActivity events in the shared queue.",
            "group": "Activity pipeline capacity",
        },
    )
    activity_callback_task_queue_size: int = field(
        default=16,
        metadata={
            "doc": "Maximum pending on_activity tasks in async mode.",
            "group": "Activity pipeline capacity",
        },
    )


settings = RunsmithSettings()
