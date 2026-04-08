from collections.abc import Callable
from typing import ParamSpec, TypeVar

HOOK_ATTR = "_pycrew_hooks"

_P = ParamSpec("_P")
_R = TypeVar("_R")


def _attach_hook(func: Callable[_P, _R], hook: tuple) -> Callable[_P, _R]:
    hooks = getattr(func, HOOK_ATTR, [])
    hooks.append(hook)
    setattr(func, HOOK_ATTR, hooks)
    return func


def pre(tgt: str, event: str):
    """Called when entering `tgt` state via `event`."""

    def decor(func: Callable[_P, _R]) -> Callable[_P, _R]:
        return _attach_hook(func, ("pre", tgt, event))

    return decor


def post(src: str, event: str):
    """Called when leaving `src` state via `event`."""

    def decor(func: Callable[_P, _R]) -> Callable[_P, _R]:
        return _attach_hook(func, ("post", src, event))

    return decor


def actor(state: str):
    """Registers a generator method as the activity handler for `state`."""

    def decor(func: Callable[_P, _R]) -> Callable[_P, _R]:
        return _attach_hook(func, ("actor", state))

    return decor
