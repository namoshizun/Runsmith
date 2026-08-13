class IncompatibleExecutorTypeError(ValueError):
    pass


class IncompatibleWorkerTypeError(ValueError):
    pass


class DuplicateWorkerNameError(ValueError):
    def __init__(self, name: str):
        super().__init__(f"Worker name [{name}] is already registered")


class InvalidStateMachineError(ValueError):
    pass


class InvalidTransitionError(ValueError):
    def __init__(self, state: str, event: str):
        super().__init__(f"Invalid transition: {state} -[{event}]-> ?")


class NoWorkersRegisteredError(ValueError):
    pass


class InvalidHookFunctionTypeError(TypeError):
    pass


class RetryExhaustedError(RuntimeError):
    pass
