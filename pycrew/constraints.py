import dataclasses


@dataclasses.dataclass
class Timeout:
    timeout: float
    when: str  # state tuple representation of src -> tgt transition


@dataclasses.dataclass
class TransitionTimeout(Timeout):
    """
    when: str: state tuple representation of src -> tgt transition
    """

    ...


@dataclasses.dataclass
class HeartbeatTimeout(Timeout):
    """
    when: str: the state that the periodic heartbeat is expected
    """

    ...
