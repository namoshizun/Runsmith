from typing import TypeVar

TState = TypeVar("TState", bound=str)
TEvent = TypeVar("TEvent", bound=str)

HOOK_ATTR = "_pycrew_hooks"


def _attach_hook(func, hook: tuple):
    hooks = getattr(func, HOOK_ATTR, [])
    hooks.append(hook)
    func._pycrew_hooks = hooks  # noqa: B010
    return func


def pre(tgt: TState, event: TEvent):
    """Called when entering `tgt` state via `event`."""

    def decor(func):
        return _attach_hook(func, ("pre", tgt, event))

    return decor


def post(src: TState, event: TEvent):
    """Called when leaving `src` state via `event`."""

    def decor(func):
        return _attach_hook(func, ("post", src, event))

    return decor


def actor(state: TState):
    """Registers a generator method as the activity handler for `state`."""

    def decor(func):
        return _attach_hook(func, ("actor", state))

    return decor
