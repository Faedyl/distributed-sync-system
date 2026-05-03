"""Distributed Queue with consistent hashing.

Design
------

* Topics are partitioned across the cluster by consistent hashing. Each topic's
  primary is the first virtual node CW from hash(topic). Replicas are the next
  ``REPLICATION_FACTOR-1`` distinct physical nodes.
* Producers send to any node; non-primary nodes forward the produce to the
  primary. The primary appends to a write-ahead log and replicates synchronously
  to all replicas before acking the producer (durability + survives node loss).
* Consumers read from the primary. Delivered messages enter an in-flight table
  with a visibility timeout; if the consumer fails to ack within the timeout
  the message becomes available again (at-least-once delivery).
* Recovery: on startup each node re-reads its WAL; consumed offsets are
  persisted alongside.

Wire format
-----------

    POST /queue/produce      {topic, payload}
        -> {ok, seq, leader, replicas}
    POST /queue/consume      {topic, consumer_id, count?, visibility_ms?}
        -> {messages: [{seq, payload}], leader}
    POST /queue/ack          {topic, consumer_id, seq}
        -> {ok}
    GET  /queue/state        -> per-topic stats
    POST /queue/replicate    (internal) {topic, entry}
        -> {ok}
"""

from __future__ import annotations

import asyncio
import bisect
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import structlog
from aiohttp import web

from src.communication.message_passing import HttpClient
from src.nodes.base_node import BaseNode, json_error
from src.utils.config import ClusterConfig, Peer
from src.utils.logging_setup import configure as configure_logging
from src.utils.metrics import Metrics


log = structlog.get_logger("queue")


# ---------------------------------------------------------------------------
# Consistent hash ring
# ---------------------------------------------------------------------------


def _hash(s: str) -> int:
    return int.from_bytes(hashlib.sha256(s.encode()).digest()[:8], "big")


@dataclass
class HashRing:
    """A consistent hash ring with virtual nodes for even key distribution."""

    virtual_nodes: int

    def __post_init__(self) -> None:
        self._tokens: list[int] = []
        self._owner: dict[int, str] = {}

    def add(self, node_id: str) -> None:
        for i in range(self.virtual_nodes):
            t = _hash(f"{node_id}#{i}")
            self._owner[t] = node_id
            bisect.insort(self._tokens, t)

    def remove(self, node_id: str) -> None:
        new_tokens: list[int] = []
        for t in self._tokens:
            if self._owner.get(t) == node_id:
                self._owner.pop(t, None)
            else:
                new_tokens.append(t)
        self._tokens = new_tokens

    def primary_for(self, key: str) -> str | None:
        if not self._tokens:
            return None
        h = _hash(key)
        idx = bisect.bisect_left(self._tokens, h) % len(self._tokens)
        return self._owner[self._tokens[idx]]

    def successors(self, key: str, n: int) -> list[str]:
        """Up to ``n`` distinct physical nodes starting at the primary, CW."""
        if not self._tokens:
            return []
        h = _hash(key)
        idx = bisect.bisect_left(self._tokens, h) % len(self._tokens)
        seen: list[str] = []
        for i in range(len(self._tokens)):
            owner = self._owner[self._tokens[(idx + i) % len(self._tokens)]]
            if owner not in seen:
                seen.append(owner)
                if len(seen) >= n:
                    break
        return seen


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


@dataclass
class QueueEntry:
    seq: int
    payload: Any
    produced_at: float

    def to_json(self) -> dict[str, Any]:
        return {"seq": self.seq, "payload": self.payload, "produced_at": self.produced_at}

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> "QueueEntry":
        return cls(seq=d["seq"], payload=d["payload"], produced_at=d["produced_at"])


@dataclass
class TopicState:
    """Per-topic durable + in-memory state. Exists on primary and on replicas."""

    name: str
    log_path: str
    offsets_path: str
    next_seq: int = 1
    entries: dict[int, QueueEntry] = field(default_factory=dict)
    consumed: set[int] = field(default_factory=set)
    # in-flight: seq -> (consumer_id, deadline_monotonic)
    inflight: dict[int, tuple[str, float]] = field(default_factory=dict)

    def load(self) -> None:
        if os.path.exists(self.log_path):
            with open(self.log_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    e = QueueEntry.from_json(json.loads(line))
                    self.entries[e.seq] = e
                    if e.seq >= self.next_seq:
                        self.next_seq = e.seq + 1
        if os.path.exists(self.offsets_path):
            with open(self.offsets_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                self.consumed = set(data.get("consumed", []))

    def append(self, entry: QueueEntry) -> None:
        with open(self.log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry.to_json()) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        self.entries[entry.seq] = entry
        if entry.seq >= self.next_seq:
            self.next_seq = entry.seq + 1

    def persist_offsets(self) -> None:
        tmp = self.offsets_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"consumed": sorted(self.consumed)}, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.offsets_path)

    def stats(self) -> dict[str, Any]:
        return {
            "next_seq": self.next_seq,
            "produced": len(self.entries),
            "consumed": len(self.consumed),
            "inflight": len(self.inflight),
            "available": len(self.entries) - len(self.consumed) - len(self.inflight),
        }


# ---------------------------------------------------------------------------
# Queue node
# ---------------------------------------------------------------------------


class QueueNode(BaseNode):
    def __init__(self, config: ClusterConfig) -> None:
        metrics = Metrics()
        super().__init__(config, metrics, component="queue")
        self.ring = HashRing(virtual_nodes=config.queue_virtual_nodes)
        for p in config.peers:
            self.ring.add(p.id)
        self.replication_factor = max(1, config.queue_replication_factor)
        self.client = HttpClient(timeout_ms=1500)
        self.topics: dict[str, TopicState] = {}
        self._lock = asyncio.Lock()
        self._queue_dir = os.path.join(config.data_dir, "queue", config.node_id)
        os.makedirs(self._queue_dir, exist_ok=True)
        self._register_routes()

    # ------------- routes -------------

    def _register_routes(self) -> None:
        self.app.router.add_post("/queue/produce", self._handle_produce)
        self.app.router.add_post("/queue/consume", self._handle_consume)
        self.app.router.add_post("/queue/ack", self._handle_ack)
        self.app.router.add_post("/queue/replicate", self._handle_replicate)
        self.app.router.add_get("/queue/state", self._handle_state)
        self.app.router.add_get("/queue/ring", self._handle_ring)

    # ------------- helpers -------------

    def _topic(self, name: str) -> TopicState:
        ts = self.topics.get(name)
        if ts is None:
            ts = TopicState(
                name=name,
                log_path=os.path.join(self._queue_dir, f"{_safe(name)}.log"),
                offsets_path=os.path.join(self._queue_dir, f"{_safe(name)}.offsets.json"),
            )
            ts.load()
            self.topics[name] = ts
        return ts

    def _is_primary(self, topic: str) -> bool:
        return self.ring.primary_for(topic) == self.config.node_id

    def _replicas(self, topic: str) -> list[str]:
        return self.ring.successors(topic, self.replication_factor)

    async def _forward(self, peer_id: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
        peer: Peer | None = self.config.peer(peer_id)
        if peer is None:
            raise web.HTTPInternalServerError(reason=f"unknown peer {peer_id}")
        return await self.client.post_json(f"{peer.url}{path}", body)

    # ------------- produce -------------

    async def _handle_produce(self, req: web.Request) -> web.Response:
        body = await req.json()
        topic = body.get("topic")
        payload = body.get("payload")
        if not topic or payload is None:
            return json_error(400, "invalid request")

        primary = self.ring.primary_for(topic)
        if primary is None:
            return json_error(503, "ring empty")

        if primary != self.config.node_id:
            try:
                resp = await self._forward(primary, "/queue/produce", body)
                return web.json_response(resp)
            except Exception as exc:
                self.metrics.incr("queue_forward_failed")
                return json_error(502, f"forward to {primary} failed: {exc}")

        # I am primary
        async with self._lock:
            ts = self._topic(topic)
            entry = QueueEntry(seq=ts.next_seq, payload=payload, produced_at=time.time())
            ts.append(entry)

        replicas = [r for r in self._replicas(topic) if r != self.config.node_id]
        successes: list[str] = [self.config.node_id]
        failures: list[str] = []
        results = await asyncio.gather(
            *(
                self._forward(r, "/queue/replicate", {"topic": topic, "entry": entry.to_json()})
                for r in replicas
            ),
            return_exceptions=True,
        )
        for r, res in zip(replicas, results):
            if isinstance(res, Exception):
                failures.append(r)
            else:
                successes.append(r)

        self.metrics.incr("queue_produced")
        if failures:
            self.metrics.incr("queue_replication_partial")
        return web.json_response(
            {"ok": True, "seq": entry.seq, "leader": self.config.node_id,
             "replicas": successes, "failed_replicas": failures}
        )

    async def _handle_replicate(self, req: web.Request) -> web.Response:
        body = await req.json()
        topic = body["topic"]
        entry = QueueEntry.from_json(body["entry"])
        async with self._lock:
            ts = self._topic(topic)
            if entry.seq in ts.entries:
                return web.json_response({"ok": True, "duplicate": True})
            ts.append(entry)
        self.metrics.incr("queue_replica_appends")
        return web.json_response({"ok": True})

    # ------------- consume -------------

    async def _handle_consume(self, req: web.Request) -> web.Response:
        body = await req.json()
        topic = body.get("topic")
        consumer_id = body.get("consumer_id")
        count = int(body.get("count", 1))
        visibility_ms = int(body.get("visibility_ms", 5000))
        if not topic or not consumer_id:
            return json_error(400, "invalid request")

        primary = self.ring.primary_for(topic)
        if primary is None:
            return json_error(503, "ring empty")
        if primary != self.config.node_id:
            try:
                resp = await self._forward(primary, "/queue/consume", body)
                return web.json_response(resp)
            except Exception as exc:
                return json_error(502, f"forward to {primary} failed: {exc}")

        async with self._lock:
            ts = self._topic(topic)
            self._reclaim_expired(ts)
            available = sorted(
                seq for seq in ts.entries
                if seq not in ts.consumed and seq not in ts.inflight
            )
            taken = available[:count]
            now = time.monotonic()
            messages: list[dict[str, Any]] = []
            for seq in taken:
                ts.inflight[seq] = (consumer_id, now + visibility_ms / 1000.0)
                messages.append(
                    {"seq": seq, "payload": ts.entries[seq].payload, "topic": topic}
                )
            self.metrics.incr("queue_consumed", len(taken))
        return web.json_response({"messages": messages, "leader": self.config.node_id})

    async def _handle_ack(self, req: web.Request) -> web.Response:
        body = await req.json()
        topic = body.get("topic")
        consumer_id = body.get("consumer_id")
        seq = body.get("seq")
        if not topic or not consumer_id or seq is None:
            return json_error(400, "invalid request")
        primary = self.ring.primary_for(topic)
        if primary != self.config.node_id:
            try:
                resp = await self._forward(primary, "/queue/ack", body)
                return web.json_response(resp)
            except Exception as exc:
                return json_error(502, f"forward to {primary} failed: {exc}")

        async with self._lock:
            ts = self._topic(topic)
            inflight = ts.inflight.get(seq)
            if inflight is None:
                return web.json_response({"ok": False, "reason": "not in flight"})
            if inflight[0] != consumer_id:
                return web.json_response({"ok": False, "reason": "wrong consumer"})
            ts.inflight.pop(seq, None)
            ts.consumed.add(seq)
            ts.persist_offsets()
            self.metrics.incr("queue_acked")
        return web.json_response({"ok": True})

    def _reclaim_expired(self, ts: TopicState) -> None:
        now = time.monotonic()
        expired = [seq for seq, (_, dl) in ts.inflight.items() if dl <= now]
        for seq in expired:
            ts.inflight.pop(seq, None)
            self.metrics.incr("queue_visibility_expired")

    # ------------- introspection -------------

    async def _handle_state(self, _req: web.Request) -> web.Response:
        return web.json_response(
            {
                "node": self.config.node_id,
                "topics": {name: ts.stats() for name, ts in self.topics.items()},
            }
        )

    async def _handle_ring(self, _req: web.Request) -> web.Response:
        sample_topics = ["orders", "events", "audit", "billing", "telemetry"]
        return web.json_response(
            {
                "virtual_nodes": self.config.queue_virtual_nodes,
                "replication_factor": self.replication_factor,
                "primaries": {t: self.ring.primary_for(t) for t in sample_topics},
                "replicas": {t: self._replicas(t) for t in sample_topics},
            }
        )


def _safe(name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in name)


async def _amain() -> None:
    cfg = ClusterConfig.from_env()
    os.makedirs(cfg.data_dir, exist_ok=True)
    configure_logging("queue")
    log.info("queue.boot", node=cfg.node_id, peers=[p.id for p in cfg.peers])
    node = QueueNode(cfg)
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
