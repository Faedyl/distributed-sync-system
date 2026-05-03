"""Distributed Cache with MESI coherence protocol.

States
------

* M (Modified):  only this cache has the line; value differs from "memory"
* E (Exclusive): only this cache has the line; value matches "memory"
* S (Shared):    one or more caches have the line, all clean
* I (Invalid):   not present (or has been invalidated)

Bus operations (modeled as HTTP RPCs to peers)
----------------------------------------------

* BusRd  (snoop_read):       a remote read miss; peers in M/E downgrade to S and
                             share data; peers in S keep S
* BusRdX (snoop_invalidate): a remote write; all peer copies -> I

Local rules
-----------

* read hit (M/E/S):  serve locally; remain in same state
* read miss:         BusRd to peers. Result:
    - some peer had M/E/S => load and adopt S (any sharers exist)
    - no peer had it      => adopt E (exclusive)
* write hit M:       no bus traffic, stay in M
* write hit E:       no bus traffic, transition to M
* write hit S:       BusRdX to peers, transition to M
* write miss:        BusRdX to peers, transition to M

Replacement: LRU (default) or LFU. Modified lines are ``flushed`` to a local
file before eviction so reads after eviction can repopulate (modeling
write-back to memory).

Wire format
-----------

    POST /cache/get   {key}                 -> {ok, value?, state, source}
    POST /cache/set   {key, value}          -> {ok, state}
    POST /cache/snoop_read         {key}    -> {has, value?, downgraded_from?}
    POST /cache/snoop_invalidate   {key}    -> {ok, was}
    GET  /cache/state                       -> entries + metrics
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

import structlog
from aiohttp import web

from src.communication.message_passing import HttpClient
from src.nodes.base_node import BaseNode, json_error
from src.utils.config import ClusterConfig, Peer
from src.utils.logging_setup import configure as configure_logging
from src.utils.metrics import Metrics


log = structlog.get_logger("cache")


M, E, S, I = "M", "E", "S", "I"


@dataclass
class Line:
    value: Any
    state: str
    last_access: float
    access_count: int = 0


class CacheStore:
    """LRU/LFU-bounded cache with MESI-aware eviction."""

    def __init__(self, capacity: int, policy: str, memory_path: str) -> None:
        self.capacity = capacity
        self.policy = policy
        self._lines: OrderedDict[str, Line] = OrderedDict()
        self._memory_path = memory_path
        self._memory: dict[str, Any] = {}
        self._load_memory()

    def _load_memory(self) -> None:
        if os.path.exists(self._memory_path):
            try:
                with open(self._memory_path, "r", encoding="utf-8") as fh:
                    self._memory = json.load(fh)
            except Exception:
                self._memory = {}

    def _save_memory(self) -> None:
        tmp = self._memory_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self._memory, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._memory_path)

    def get(self, key: str) -> Line | None:
        line = self._lines.get(key)
        if line:
            self._lines.move_to_end(key)
            line.last_access = time.monotonic()
            line.access_count += 1
        return line

    def memory_get(self, key: str) -> Any | None:
        return self._memory.get(key)

    def memory_put(self, key: str, value: Any) -> None:
        self._memory[key] = value
        self._save_memory()

    def put(self, key: str, value: Any, state: str) -> str | None:
        evicted: str | None = None
        if key in self._lines:
            line = self._lines[key]
            line.value = value
            line.state = state
            line.last_access = time.monotonic()
            line.access_count += 1
            self._lines.move_to_end(key)
        else:
            if len(self._lines) >= self.capacity:
                evicted = self._evict()
            self._lines[key] = Line(
                value=value, state=state, last_access=time.monotonic(), access_count=1
            )
        return evicted

    def update_state(self, key: str, state: str) -> None:
        if key in self._lines:
            self._lines[key].state = state

    def remove(self, key: str) -> None:
        self._lines.pop(key, None)

    def _evict(self) -> str | None:
        if not self._lines:
            return None
        if self.policy == "LFU":
            victim_key = min(self._lines, key=lambda k: self._lines[k].access_count)
        else:  # LRU default
            victim_key = next(iter(self._lines))
        line = self._lines.pop(victim_key)
        if line.state == M:
            # write-back to "memory"
            self.memory_put(victim_key, line.value)
        return victim_key

    def snapshot(self) -> dict[str, Any]:
        return {
            "capacity": self.capacity,
            "policy": self.policy,
            "size": len(self._lines),
            "lines": {
                k: {"state": v.state, "value": v.value, "access_count": v.access_count}
                for k, v in self._lines.items()
            },
        }


class CacheNode(BaseNode):
    def __init__(self, config: ClusterConfig) -> None:
        metrics = Metrics()
        super().__init__(config, metrics, component="cache")
        cache_dir = os.path.join(config.data_dir, "cache", config.node_id)
        os.makedirs(cache_dir, exist_ok=True)
        self.store = CacheStore(
            capacity=config.cache_capacity,
            policy=config.cache_policy,
            memory_path=os.path.join(cache_dir, "memory.json"),
        )
        self.client = HttpClient(timeout_ms=600)
        self._lock = asyncio.Lock()
        self._register_routes()

    # ------------- routes -------------

    def _register_routes(self) -> None:
        self.app.router.add_post("/cache/get", self._handle_get)
        self.app.router.add_post("/cache/set", self._handle_set)
        self.app.router.add_post("/cache/snoop_read", self._handle_snoop_read)
        self.app.router.add_post("/cache/snoop_invalidate", self._handle_snoop_invalidate)
        self.app.router.add_get("/cache/state", self._handle_state)

    # ------------- helpers -------------

    async def _broadcast(self, path: str, body: dict[str, Any]) -> list[dict[str, Any]]:
        peers: list[Peer] = self.config.other_peers
        results = await asyncio.gather(
            *(self.client.post_json(f"{p.url}{path}", body) for p in peers),
            return_exceptions=True,
        )
        out: list[dict[str, Any]] = []
        for p, r in zip(peers, results):
            if isinstance(r, Exception):
                self.metrics.incr("cache_peer_unreachable")
                continue
            out.append({"peer": p.id, **r})
        return out

    # ------------- public ops -------------

    async def _handle_get(self, req: web.Request) -> web.Response:
        body = await req.json()
        key = body.get("key")
        if not key:
            return json_error(400, "missing key")
        async with self._lock:
            line = self.store.get(key)
            if line and line.state in (M, E, S):
                self.metrics.incr("cache_hits")
                return web.json_response(
                    {"ok": True, "value": line.value, "state": line.state, "source": "local"}
                )
            self.metrics.incr("cache_misses")

        # read miss -> BusRd peers
        peer_results = await self._broadcast("/cache/snoop_read", {"key": key})
        sharing_peers = [r for r in peer_results if r.get("has")]
        async with self._lock:
            if sharing_peers:
                value = sharing_peers[0]["value"]
                state = S  # at least one sharer exists
                self.metrics.incr("cache_remote_fill")
                source = f"peer:{sharing_peers[0]['peer']}"
            else:
                # no sharers; load from local "memory" backing store
                value = self.store.memory_get(key)
                if value is None:
                    return web.json_response({"ok": False, "reason": "not found"}, status=404)
                state = E
                self.metrics.incr("cache_memory_fill")
                source = "memory"
            evicted = self.store.put(key, value, state)
            if evicted:
                self.metrics.incr("cache_evictions")
        return web.json_response({"ok": True, "value": value, "state": state, "source": source})

    async def _handle_set(self, req: web.Request) -> web.Response:
        body = await req.json()
        key = body.get("key")
        value = body.get("value")
        if not key or value is None:
            return json_error(400, "missing key/value")

        async with self._lock:
            line = self.store.get(key)
            current_state = line.state if line else I

        if current_state == M:
            async with self._lock:
                self.store.put(key, value, M)
            self.metrics.incr("cache_write_hit_m")
            return web.json_response({"ok": True, "state": M})

        if current_state == E:
            async with self._lock:
                self.store.put(key, value, M)
            self.metrics.incr("cache_write_hit_e_to_m")
            return web.json_response({"ok": True, "state": M})

        # S, I, or miss => BusRdX to invalidate peers
        await self._broadcast("/cache/snoop_invalidate", {"key": key})
        async with self._lock:
            evicted = self.store.put(key, value, M)
            self.store.memory_put(key, value)
            if evicted:
                self.metrics.incr("cache_evictions")
        self.metrics.incr("cache_write_with_invalidate")
        return web.json_response({"ok": True, "state": M})

    # ------------- snoop handlers -------------

    async def _handle_snoop_read(self, req: web.Request) -> web.Response:
        body = await req.json()
        key = body.get("key")
        async with self._lock:
            line = self.store.get(key)
            if not line or line.state == I:
                return web.json_response({"has": False})
            prior = line.state
            value = line.value
            if line.state == M:
                # write-back to memory before sharing
                self.store.memory_put(key, line.value)
                self.store.update_state(key, S)
                self.metrics.incr("cache_snoop_m_downgrade")
            elif line.state == E:
                self.store.update_state(key, S)
                self.metrics.incr("cache_snoop_e_downgrade")
            elif line.state == S:
                self.metrics.incr("cache_snoop_s_serve")
        return web.json_response({"has": True, "value": value, "downgraded_from": prior})

    async def _handle_snoop_invalidate(self, req: web.Request) -> web.Response:
        body = await req.json()
        key = body.get("key")
        async with self._lock:
            line = self.store.get(key)
            was = line.state if line else I
            if line:
                if line.state == M:
                    # write-back before invalidate
                    self.store.memory_put(key, line.value)
                self.store.remove(key)
            self.metrics.incr("cache_invalidations")
        return web.json_response({"ok": True, "was": was})

    async def _handle_state(self, _req: web.Request) -> web.Response:
        return web.json_response(self.store.snapshot())


async def _amain() -> None:
    cfg = ClusterConfig.from_env()
    os.makedirs(cfg.data_dir, exist_ok=True)
    configure_logging("cache")
    log.info("cache.boot", node=cfg.node_id, peers=[p.id for p in cfg.peers])
    node = CacheNode(cfg)
    await node.serve()


def main() -> None:
    try:
        import uvloop  # type: ignore

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except Exception:
        pass
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
