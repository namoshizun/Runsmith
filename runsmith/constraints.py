import dataclasses
import re

_TRANSITION_PATTERN = re.compile(r"^(\S+)\s*->\s*(\S+)$")


@dataclasses.dataclass
class Timeout:
    timeout: float
    when: str  # state tuple representation of src -> tgt transition

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise ValueError(f"Timeout must be a positive value, got {self.timeout}")


@dataclasses.dataclass
class TransitionTimeout(Timeout):
    """
    when: str: state tuple representation of src -> tgt transition, e.g. "starting -> running"
    """

    def __post_init__(self) -> None:
        super().__post_init__()
        if not _TRANSITION_PATTERN.match(self.when):
            raise ValueError(
                f"Invalid TransitionTimeout 'when' syntax: {self.when!r}. "
                "Expected format: '<src> -> <tgt>'"
            )

    def get_src_and_tgt(self) -> tuple[str, str]:
        m = _TRANSITION_PATTERN.match(self.when)
        assert m, "unreachable: validated in __post_init__"
        src, tgt = m.group(1), m.group(2)
        return src, tgt


@dataclasses.dataclass
class HeartbeatTimeout(Timeout):
    """
    when: str: the state that the periodic heartbeat is expected
    """

    ...


@dataclasses.dataclass
class StateTimeout(Timeout):
    """
    when: str: the state whose total residence time is capped

    Unlike HeartbeatTimeout (which checks that the worker is *actively* doing
    work) this limits how long the worker may remain in a state at all,
    regardless of heartbeat activity.
    """

    ...
