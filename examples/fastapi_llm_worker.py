import asyncio
import multiprocessing
import random
import string
import threading
import time
from contextlib import asynccontextmanager
from multiprocessing.queues import Queue as MPQueue
from queue import Empty
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from loguru import logger

from runsmith.decorators import actor
from runsmith.defaults import DefaultWorkerEvent, DefaultWorkerState
from runsmith.supervisor import SyncSupervisor
from runsmith.worker import SyncWorker


class LLMWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    def __init__(self, name: str, request_queue: MPQueue, response_queue: MPQueue, **kwargs: Any):
        super().__init__(name, **kwargs)
        self.request_queue = request_queue
        self.response_queue = response_queue

    @actor("starting")
    def setup(self):
        time.sleep(3)
        logger.info("Mock LLM worker loaded and started")
        return self.emit("run")

    @actor("running")
    def handle_llm_request(self):
        if self.ctx.cmd == "stop":
            return self.emit("terminate")

        try:
            self.request_queue.get(timeout=0.1)
        except Empty:
            return self.emit("keepalive")

        logger.info("Generating gibberish text...")
        time.sleep(1.5)
        gibberish = "".join(random.choices(string.ascii_letters, k=50))
        self.response_queue.put(gibberish)
        return self.emit("keepalive")

    @actor("terminating")
    def terminate(self):
        logger.info("Cleaning up LLM worker...")
        return self.emit("complete")

    def clone(self) -> "LLMWorker":
        return LLMWorker(
            name=self.name,
            request_queue=self.request_queue,
            response_queue=self.response_queue,
        )


class FastAPIWorker(SyncWorker[DefaultWorkerState, DefaultWorkerEvent]):
    def __init__(
        self, name: str, host: str, port: int, request_queue: MPQueue, response_queue: MPQueue
    ):
        super().__init__(name)
        self._thread: threading.Thread | None = None
        self._server: uvicorn.Server | None = None
        self.host = host
        self.port = port
        self.request_queue = request_queue
        self.response_queue = response_queue

    @property
    def heartbeat_evt(self) -> threading.Event:
        if not hasattr(self, "_heartbeat_evt"):
            raise RuntimeError("Heartbeat event not initialized")
        return self._heartbeat_evt  # pyright: ignore

    @heartbeat_evt.setter
    def heartbeat_evt(self, evt: threading.Event):
        self._heartbeat_evt = evt

    def _build_app(self):
        async def keepalive():
            while True:
                await asyncio.sleep(1)
                # Indicating the event loop is still running and smoothly functional
                self.heartbeat_evt.set()

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            beat_task = asyncio.create_task(keepalive())
            yield
            beat_task.cancel()

        return FastAPI(lifespan=lifespan)

    def _setup_app(self, app: FastAPI):
        @app.get("/")
        async def hello():
            return "RUOK 🕺?!"

        @app.get("/blocky")
        async def blocky():
            logger.warning("🙈 Manually blocking the event loop forever")
            time.sleep(60 * 60 * 24)
            return "OMG"

        @app.get("/say-something")
        async def say_some(request: Request):
            self.request_queue.put("say something")
            return await asyncio.to_thread(self.response_queue.get)

    @actor("starting")
    def setup(self):
        self.heartbeat_evt = threading.Event()
        app = self._build_app()
        self._setup_app(app)

        self._server = uvicorn.Server(
            uvicorn.Config(app, host=self.host, port=self.port, reload=False, workers=1)
        )
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()

        logger.info(f"FastAPI worker [{self.name}] started")
        return self.emit("run")

    @actor("running")
    def check_app_health(self):
        if self.ctx.cmd == "stop":
            return self.emit("terminate")

        self.heartbeat_evt.clear()
        if self.heartbeat_evt.wait(timeout=10) and self._thread and self._thread.is_alive():
            return self.emit("keepalive")

        logger.error(f"FastAPI worker [{self.name}] not up and working!")
        return self.emit("error")

    @actor("terminating")
    def terminate(self):
        logger.info("Terminating web server...")
        if self._server:
            self._server.should_exit = True

        if self._thread:
            self._thread.join()

        logger.info("Web server terminated...")
        return self.emit("complete")

    def clone(self) -> "FastAPIWorker":
        return FastAPIWorker(
            name=self.name,
            host=self.host,
            port=self.port,
            request_queue=self.request_queue,
            response_queue=self.response_queue,
        )


if __name__ == "__main__":
    request_queue = multiprocessing.Queue()
    response_queue = multiprocessing.Queue()

    supervisor = SyncSupervisor("supervisor", "process")
    web_worker = FastAPIWorker(
        "web-app", "0.0.0.0", 8050, request_queue=request_queue, response_queue=response_queue
    )
    llm_worker = LLMWorker("llm-worker", request_queue=request_queue, response_queue=response_queue)
    supervisor.register_workers(web_worker, llm_worker)
    supervisor.run()
