"""Distributed Lock Manager.

Built on Raft consensus. The replicated state machine maintains the lock table:

    {resource_id: LockEntry(holders=[...], waiters=[...])}

Supports shared (S) and exclusive (X) modes with standard compatibility:

    holder mode | requested S | requested X
    ----------- | ----------- | -----------
    none        | grant       | grant
    S only      | grant       | wait
    X           | wait        | wait

The leader handles client requests; followers redirect (HTTP 421-style with
``leader_id``). Network partitions are handled by Raft: a minority partition
cannot commit new entries, so it can neither grant nor release. When the
partition heals, the minority's local view reconciles via log replication.

Deadlock detection runs over the wait-for graph (waiter -> holders). On cycle
detection, the youngest waiter is selected as victim and aborted (its lock
requests/holdings are released).

Wire format:

    POST /lock/acquire   {resource, mode, owner, [timeout_ms]}
        -> {status: granted|waiting|deadlock|aborted, leader_id?}
    POST /lock/release   {resource, owner}
        -> {ok: true, granted: [{owner, mode, resource}]}
    POST /lock/release_all  {owner}
        -> {ok: true, released: [...]}
    GET  /lock/state             -> snapshot of lock table
    GET  /lock/wait_for_graph    -> snapshot of wait-for graph
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import structlog
from aiohttp import web

from src.communication.message_passing import HttpClient
from src.consensus.raft import (
    CommitTimeoutError,
    LogEntry,
    NotLeaderError,
    RaftNode,
    StateMachine,
)
from src.nodes.base_node import BaseNode, json_error
from src.utils.config import ClusterConfig
from src.utils.logging_setup import configure as configure_logging
from src.utils.metrics import Metrics


log = structlog.get_logger("lock_manager")


SHARED = "shared"
EXCLUSIVE = "exclusive"


@dataclass
class Holder:
    owner: str
    mode: str


@dataclass
class Waiter:
    owner: str
    mode: str
    request_id: str
    enqueued_at: float


@dataclass
class LockEntry:
    resource: str
    holders: list[Holder] = field(default_factory=list)
    waiters: list[Waiter] = field(default_factory=list)


class LockStateMachine(StateMachine):
    """Replicated state machine for the lock table.

    All mutations flow through Raft submit() -> apply(). Reads against
    `state()` are local (linearizable reads would require submit-then-read; not
    required for this assignment).
    """

    def __init__(self, metrics: Metrics) -> None:
        self.metrics = metrics
        self.locks: dict[str, LockEntry] = {}
        # request_id -> outcome string ("granted", "waiting", "deadlock", "aborted")
        # used by leader to resolve the awaiting client when grant happens.
        self.outcomes: dict[str, str] = {}
        # request_id -> future for the leader's pending acquire
        self._pending_grants: dict[str, asyncio.Future[str]] = {}

    # ------------- public reads -------------

    def snapshot(self) -> dict[str, Any]:
        return {
            res: {
                "holders": [{"owner": h.owner, "mode": h.mode} for h in e.holders],
                "waiters": [
                    {
                        "owner": w.owner,
                        "mode": w.mode,
                        "request_id": w.request_id,
                        "wait_ms": int((time.time() - w.enqueued_at) * 1000),
                    }
                    for w in e.waiters
                ],
            }
            for res, e in self.locks.items()
        }

    def wait_for_graph(self) -> dict[str, list[str]]:
        """waiter_owner -> [holder_owner...] across all resources."""
        wfg: dict[str, set[str]] = {}
        for entry in self.locks.values():
            holders = {h.owner for h in entry.holders}
            for w in entry.waiters:
                if w.owner in holders:
                    continue
                wfg.setdefault(w.owner, set()).update(holders - {w.owner})
        return {k: sorted(v) for k, v in wfg.items()}

    def find_deadlock_victim(self) -> str | None:
        """Cycle detection in wait-for graph. Returns the youngest waiter on cycle."""
        wfg = self.wait_for_graph()
        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[str, int] = {n: WHITE for n in wfg}
        cycle_nodes: set[str] = set()
        path: list[str] = []

        def dfs(u: str) -> None:
            color[u] = GRAY
            path.append(u)
            for v in wfg.get(u, []):
                if color.get(v, WHITE) == GRAY:
                    # back edge: cycle from path[path.index(v):]
                    if v in path:
                        cycle_nodes.update(path[path.index(v):])
                elif color.get(v, WHITE) == WHITE:
                    dfs(v)
            color[u] = BLACK
            path.pop()

        for n in list(wfg.keys()):
            if color.get(n, WHITE) == WHITE:
                dfs(n)

        if not cycle_nodes:
            return None

        # pick the youngest waiter (latest enqueued_at) as victim
        victim, victim_t = None, -1.0
        for entry in self.locks.values():
            for w in entry.waiters:
                if w.owner in cycle_nodes and w.enqueued_at > victim_t:
                    victim, victim_t = w.owner, w.enqueued_at
        return victim

    # ------------- pending future plumbing -------------

    def register_pending(self, request_id: str) -> asyncio.Future[str]:
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        self._pending_grants[request_id] = fut
        return fut

    def discard_pending(self, request_id: str) -> None:
        self._pending_grants.pop(request_id, None)

    def _signal(self, request_id: str, outcome: str) -> None:
        fut = self._pending_grants.pop(request_id, None)
        if fut and not fut.done():
            fut.set_result(outcome)

    # ------------- state machine apply -------------

    async def apply(self, entry: LogEntry) -> Any:
        cmd = entry.command
        op = cmd.get("op")
        if op == "acquire":
            return self._apply_acquire(cmd)
        if op == "release":
            return self._apply_release(cmd)
        if op == "release_all":
            return self._apply_release_all(cmd)
        if op == "abort_waiter":
            return self._apply_abort_waiter(cmd)
        raise ValueError(f"unknown op {op}")

    # ------------- ops -------------

    def _entry(self, resource: str) -> LockEntry:
        e = self.locks.get(resource)
        if e is None:
            e = LockEntry(resource=resource)
            self.locks[resource] = e
        return e

    def _can_grant(self, entry: LockEntry, mode: str) -> bool:
        if not entry.holders:
            return True
        if mode == SHARED and all(h.mode == SHARED for h in entry.holders):
            return True
        return False

    def _apply_acquire(self, cmd: dict[str, Any]) -> dict[str, Any]:
        resource = cmd["resource"]
        owner = cmd["owner"]
        mode = cmd["mode"]
        request_id = cmd["request_id"]
        entry = self._entry(resource)

        # idempotent re-acquire by same owner
        if any(h.owner == owner and h.mode == mode for h in entry.holders):
            self.outcomes[request_id] = "granted"
            self._signal(request_id, "granted")
            return {"status": "granted"}

        if self._can_grant(entry, mode) and not any(
            w.mode == EXCLUSIVE for w in entry.waiters
        ):
            entry.holders.append(Holder(owner=owner, mode=mode))
            self.outcomes[request_id] = "granted"
            self.metrics.incr("lock_granted_immediate")
            self._signal(request_id, "granted")
            return {"status": "granted"}

        if not any(w.owner == owner for w in entry.waiters):
            entry.waiters.append(
                Waiter(
                    owner=owner,
                    mode=mode,
                    request_id=request_id,
                    enqueued_at=time.time(),
                )
            )
        self.outcomes[request_id] = "waiting"
        self.metrics.incr("lock_waiting")

        victim = self.find_deadlock_victim()
        if victim and victim == owner:
            self._abort_owner_locally(owner)
            self.outcomes[request_id] = "deadlock"
            self.metrics.incr("lock_deadlock_detected")
            self._signal(request_id, "deadlock")
            return {"status": "deadlock", "victim": victim}
        if victim:
            # abort someone else's request; their leader-side future will resolve
            self._abort_owner_locally(victim)
            self.metrics.incr("lock_deadlock_detected")
        return {"status": "waiting"}

    def _abort_owner_locally(self, owner: str) -> None:
        # remove waiters and holders for this owner; signal aborted
        aborted_request_ids: list[str] = []
        for entry in self.locks.values():
            for w in entry.waiters:
                if w.owner == owner:
                    aborted_request_ids.append(w.request_id)
            entry.waiters = [w for w in entry.waiters if w.owner != owner]
            entry.holders = [h for h in entry.holders if h.owner != owner]
        for rid in aborted_request_ids:
            self.outcomes[rid] = "aborted"
            self._signal(rid, "aborted")
        self._maybe_grant_all()

    def _maybe_grant_all(self) -> None:
        # try to grant waiters at the head of each queue while compatible
        for entry in self.locks.values():
            self._maybe_grant(entry)

    def _maybe_grant(self, entry: LockEntry) -> None:
        while entry.waiters:
            w = entry.waiters[0]
            if not self._can_grant(entry, w.mode):
                break
            entry.holders.append(Holder(owner=w.owner, mode=w.mode))
            entry.waiters.pop(0)
            self.outcomes[w.request_id] = "granted"
            self.metrics.incr("lock_granted_after_wait")
            self._signal(w.request_id, "granted")
            if w.mode == EXCLUSIVE:
                break

    def _apply_release(self, cmd: dict[str, Any]) -> dict[str, Any]:
        resource = cmd["resource"]
        owner = cmd["owner"]
        entry = self._entry(resource)
        before = len(entry.holders)
        entry.holders = [h for h in entry.holders if h.owner != owner]
        if len(entry.holders) == before:
            return {"ok": True, "released": False}
        self.metrics.incr("lock_released")
        self._maybe_grant(entry)
        return {"ok": True, "released": True}

    def _apply_release_all(self, cmd: dict[str, Any]) -> dict[str, Any]:
        owner = cmd["owner"]
        released = 0
        for entry in self.locks.values():
            n = len(entry.holders)
            entry.holders = [h for h in entry.holders if h.owner != owner]
            entry.waiters = [w for w in entry.waiters if w.owner != owner]
            released += n - len(entry.holders)
            self._maybe_grant(entry)
        return {"ok": True, "released": released}

    def _apply_abort_waiter(self, cmd: dict[str, Any]) -> dict[str, Any]:
        request_id = cmd["request_id"]
        for entry in self.locks.values():
            entry.waiters = [w for w in entry.waiters if w.request_id != request_id]
        self.outcomes[request_id] = "aborted"
        self._signal(request_id, "aborted")
        return {"ok": True}


# ---------------------------------------------------------------------------
# HTTP node
# ---------------------------------------------------------------------------


class LockManagerNode(BaseNode):
    def __init__(self, config: ClusterConfig) -> None:
        metrics = Metrics()
        super().__init__(config, metrics, component="lock_manager")
        self.sm = LockStateMachine(metrics)
        self.raft_node = RaftNode(config, self.sm, metrics)
        self.attach_raft(self.raft_node)
        self._register_lock_routes()

    def _register_lock_routes(self) -> None:
        self.app.router.add_post("/lock/acquire", self._handle_acquire)
        self.app.router.add_post("/lock/release", self._handle_release)
        self.app.router.add_post("/lock/release_all", self._handle_release_all)
        self.app.router.add_get("/lock/state", self._handle_state)
        self.app.router.add_get("/lock/wait_for_graph", self._handle_wfg)

    # ----------------- handlers -----------------

    async def _handle_acquire(self, req: web.Request) -> web.Response:
        body = await req.json()
        resource = body.get("resource")
        owner = body.get("owner")
        mode = body.get("mode", EXCLUSIVE)
        timeout_ms = int(body.get("timeout_ms", 3000))
        if not resource or not owner or mode not in (SHARED, EXCLUSIVE):
            return json_error(400, "invalid request")
        if not self.raft_node.is_leader:
            return json_error(421, "not leader", leader_id=self.raft_node.leader_id)

        request_id = str(uuid.uuid4())
        fut = self.sm.register_pending(request_id)
        cmd = {
            "op": "acquire",
            "resource": resource,
            "owner": owner,
            "mode": mode,
            "request_id": request_id,
        }
        try:
            with self.metrics.timer("lock_acquire_submit_ms"):
                initial = await self.raft_node.submit(cmd)
        except NotLeaderError as e:
            self.sm.discard_pending(request_id)
            return json_error(421, "not leader", leader_id=e.leader_id)
        except CommitTimeoutError:
            self.sm.discard_pending(request_id)
            return json_error(504, "commit timeout")

        if initial.get("status") in ("granted", "deadlock"):
            return web.json_response(initial)

        # waiting: wait for grant or timeout
        try:
            outcome = await asyncio.wait_for(fut, timeout=timeout_ms / 1000.0)
            return web.json_response({"status": outcome})
        except asyncio.TimeoutError:
            self.sm.discard_pending(request_id)
            try:
                await self.raft_node.submit({"op": "abort_waiter", "request_id": request_id})
            except Exception:
                pass
            return web.json_response({"status": "timeout"}, status=408)

    async def _handle_release(self, req: web.Request) -> web.Response:
        body = await req.json()
        resource = body.get("resource")
        owner = body.get("owner")
        if not resource or not owner:
            return json_error(400, "invalid request")
        if not self.raft_node.is_leader:
            return json_error(421, "not leader", leader_id=self.raft_node.leader_id)
        try:
            res = await self.raft_node.submit(
                {"op": "release", "resource": resource, "owner": owner}
            )
        except NotLeaderError as e:
            return json_error(421, "not leader", leader_id=e.leader_id)
        return web.json_response(res)

    async def _handle_release_all(self, req: web.Request) -> web.Response:
        body = await req.json()
        owner = body.get("owner")
        if not owner:
            return json_error(400, "invalid request")
        if not self.raft_node.is_leader:
            return json_error(421, "not leader", leader_id=self.raft_node.leader_id)
        try:
            res = await self.raft_node.submit({"op": "release_all", "owner": owner})
        except NotLeaderError as e:
            return json_error(421, "not leader", leader_id=e.leader_id)
        return web.json_response(res)

    async def _handle_state(self, _req: web.Request) -> web.Response:
        return web.json_response(self.sm.snapshot())

    async def _handle_wfg(self, _req: web.Request) -> web.Response:
        return web.json_response(self.sm.wait_for_graph())


async def _amain() -> None:
    cfg = ClusterConfig.from_env()
    os.makedirs(cfg.data_dir, exist_ok=True)
    configure_logging("lock_manager")
    log.info("lock_manager.boot", node=cfg.node_id, peers=[p.id for p in cfg.peers])
    node = LockManagerNode(cfg)
    await node.raft_node.start()
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
