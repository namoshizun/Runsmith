import dataclasses
import math

from pycrew.worker import WorkerBase


@dataclasses.dataclass(slots=True)
class Expectation:
    state: str
    transition_deadline: float
    next_heartbeat: float = dataclasses.field(default=math.inf)


class Reconciler:
    def __init__(self, worker: WorkerBase):
        self._expectation = None
