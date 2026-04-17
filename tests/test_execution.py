from __future__ import annotations

import threading

import pytest

from runsmith.execution import drive_async_worker, drive_sync_worker
from runsmith.worker import WorkerActivity


def _activity(kind: str) -> WorkerActivity:
    transition = ("running", "terminate", "terminating") if kind == "transition_begin" else None
    return WorkerActivity(worker_name="worker", kind=kind, transition=transition)


def test_drive_sync_worker_repeats_stop_until_transition_begins() -> None:
    commands: list[str | None] = []

    def execution():
        cmd = yield _activity("heartbeat")
        while True:
            commands.append(cmd)
            if cmd == "stop" and commands.count("stop") < 3:
                cmd = yield _activity("heartbeat")
                continue

            if cmd == "stop":
                cmd = yield _activity("transition_begin")
                continue

            cmd = yield _activity("heartbeat")

    term_event = threading.Event()
    driver = drive_sync_worker(execution(), term_event)

    assert next(driver).kind == "heartbeat"

    term_event.set()
    assert next(driver).kind == "transition_begin"
    assert next(driver).kind == "heartbeat"
    assert commands == ["stop", "stop", "stop", "tick"]


@pytest.mark.asyncio
async def test_drive_async_worker_repeats_stop_until_transition_begins() -> None:
    commands: list[str | None] = []

    async def execution():
        cmd = yield _activity("heartbeat")
        while True:
            commands.append(cmd)
            if cmd == "stop" and commands.count("stop") < 3:
                cmd = yield _activity("heartbeat")
                continue

            if cmd == "stop":
                cmd = yield _activity("transition_begin")
                continue

            cmd = yield _activity("heartbeat")

    term_event = threading.Event()
    driver = drive_async_worker(execution(), term_event)

    assert (await driver.__anext__()).kind == "heartbeat"

    term_event.set()
    assert (await driver.__anext__()).kind == "transition_begin"
    assert (await driver.__anext__()).kind == "heartbeat"
    assert commands == ["stop", "stop", "stop", "tick"]
