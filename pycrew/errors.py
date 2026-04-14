class InvalidStateMachineError(ValueError):
    pass


class InvalidTransitionError(ValueError):
    def __init__(self, state: str, event: str):
        super().__init__(f"Invalid transition: {state} -[{event}]-> ?")
