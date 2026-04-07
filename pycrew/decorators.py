from collections.abc import Callable
from typing import TypeVar

HOOK_ATTR = "_pycrew_hooks"

_F = TypeVar("_F", bound=Callable)


def _attach_hook(func: _F, hook: tuple) -> _F:
    hooks = getattr(func, HOOK_ATTR, [])
    hooks.append(hook)
    setattr(func, HOOK_ATTR, hooks)
    return func


def pre(tgt: str, event: str):
    """Called when entering `tgt` state via `event`."""

    def decor(func: _F) -> _F:
        return _attach_hook(func, ("pre", tgt, event))

    return decor


def post(src: str, event: str):
    """Called when leaving `src` state via `event`."""

    def decor(func: _F) -> _F:
        return _attach_hook(func, ("post", src, event))

    return decor


def actor(state: str):
    """Registers a generator method as the activity handler for `state`."""

    def decor(func: _F) -> _F:
        return _attach_hook(func, ("actor", state))

    return decor
