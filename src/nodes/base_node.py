"""Common scaffolding for distributed nodes.

Each component (lock manager, queue, cache) runs an aiohttp server. The base
node wires up:

* JSON request/response helpers
* /healthz, /metrics, /status routes
* Optional Raft RPC mount (/raft/request_vote, /raft/append_entries)
* Graceful startup/shutdown
"""

from __future__ import annotations

import asyncio
import signal
from dataclasses import asdict
from typing import Any, Awaitable, Callable

import structlog
from aiohttp import web

from src.consensus.raft import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RaftNode,
    RequestVoteRequest,
    RequestVoteResponse,
)
from src.utils.config import ClusterConfig
from src.utils.metrics import Metrics


log = structlog.get_logger("base_node")


def json_error(status: int, msg: str, **extra: Any) -> web.Response:
    body = {"error": msg, **extra}
    return web.json_response(body, status=status)


def json_ok(payload: dict[str, Any] | None = None) -> web.Response:
    return web.json_response(payload or {"ok": True})


class BaseNode:
    def __init__(self, config: ClusterConfig, metrics: Metrics, *, component: str) -> None:
        self.config = config
        self.metrics = metrics
        self.component = component
        self.app = web.Application()
        self._register_common_routes()
        self.raft: RaftNode | None = None
        self._stop = asyncio.Event()

    # ------------------------------------------------------------------
    # routes
    # ------------------------------------------------------------------

    def _register_common_routes(self) -> None:
        self.app.router.add_get("/healthz", self._healthz)
        self.app.router.add_get("/metrics", self._metrics)
        self.app.router.add_get("/metrics.json", self._metrics_json)
        self.app.router.add_get("/status", self._status)

    def attach_raft(self, raft: RaftNode) -> None:
        self.raft = raft
        self.app.router.add_post("/raft/request_vote", self._raft_request_vote)
        self.app.router.add_post("/raft/append_entries", self._raft_append_entries)

    # ------------------------------------------------------------------
    # handlers
    # ------------------------------------------------------------------

    async def _healthz(self, _req: web.Request) -> web.Response:
        return json_ok({"ok": True, "node": self.config.node_id, "component": self.component})

    async def _metrics(self, _req: web.Request) -> web.Response:
        return web.Response(
            text=self.metrics.render_prometheus(),
            content_type="text/plain",
            charset="utf-8",
        )

    async def _metrics_json(self, _req: web.Request) -> web.Response:
        return web.json_response(self.metrics.snapshot())

    async def _status(self, _req: web.Request) -> web.Response:
        body: dict[str, Any] = {
            "component": self.component,
            "node_id": self.config.node_id,
            "peers": [asdict(p) for p in self.config.peers],
        }
        if self.raft:
            body["raft"] = self.raft.status()
        return web.json_response(body)

    async def _raft_request_vote(self, req: web.Request) -> web.Response:
        if not self.raft:
            return json_error(503, "raft not enabled")
        body = await req.json()
        rv = RequestVoteRequest(
            term=body["term"],
            candidate_id=body["candidate_id"],
            last_log_index=body["last_log_index"],
            last_log_term=body["last_log_term"],
        )
        resp: RequestVoteResponse = await self.raft.handle_request_vote(rv)
        return web.json_response(asdict(resp))

    async def _raft_append_entries(self, req: web.Request) -> web.Response:
        if not self.raft:
            return json_error(503, "raft not enabled")
        body = await req.json()
        ae = AppendEntriesRequest(
            term=body["term"],
            leader_id=body["leader_id"],
            prev_log_index=body["prev_log_index"],
            prev_log_term=body["prev_log_term"],
            entries=body.get("entries", []),
            leader_commit=body["leader_commit"],
        )
        resp: AppendEntriesResponse = await self.raft.handle_append_entries(ae)
        return web.json_response(asdict(resp))

    # ------------------------------------------------------------------
    # server lifecycle
    # ------------------------------------------------------------------

    async def serve(self) -> None:
        host, _, port = self.config.bind.partition(":")
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, host or "0.0.0.0", int(port or "8001"))
        await site.start()
        log.info(
            "node.listening",
            component=self.component,
            node=self.config.node_id,
            bind=self.config.bind,
        )

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except NotImplementedError:
                pass

        try:
            await self._stop.wait()
        finally:
            log.info("node.shutting_down", component=self.component)
            if self.raft:
                await self.raft.stop()
            await runner.cleanup()
