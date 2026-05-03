"""Raft consensus implementation.

A pragmatic implementation of the Raft consensus algorithm
(Ongaro & Ousterhout, 2014):

* Leader election with randomized timeouts
* Log replication with prevLogIndex/prevLogTerm matching
* Safety check: a candidate's log must be at least as up-to-date to win a vote
* Persistent state (currentTerm, votedFor, log) on disk; volatile state in memory
* Pluggable state machine via the ``StateMachine`` protocol

The state machine is a function that consumes committed log entries in order and
returns a result back to whoever submitted the command. The lock manager
(``nodes.lock_manager``) plugs its lock-table state machine in here.

Membership is static (set at startup via PEERS env). Snapshotting and dynamic
membership changes are out of scope for this assignment.

RPCs are exposed by the host as HTTP POST endpoints:

    POST /raft/request_vote       -> RequestVoteResponse
    POST /raft/append_entries     -> AppendEntriesResponse

Submission flow (client request to a leader):

    submit(command) -> await commit_index >= entry.index -> result from SM
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Protocol

import structlog

from src.communication.message_passing import HttpClient
from src.utils.config import ClusterConfig
from src.utils.metrics import Metrics


log = structlog.get_logger("raft")


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: dict[str, Any]

    def to_json(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> "LogEntry":
        return cls(term=d["term"], index=d["index"], command=d["command"])


class StateMachine(Protocol):
    """Anything that can apply Raft-committed commands and return a result."""

    async def apply(self, entry: LogEntry) -> Any: ...


@dataclass
class PersistentState:
    current_term: int = 0
    voted_for: str | None = None
    log: list[LogEntry] = field(default_factory=list)

    def last_index(self) -> int:
        return self.log[-1].index if self.log else 0

    def last_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def entry_at(self, idx: int) -> LogEntry | None:
        # log is 1-indexed by entry.index; list is 0-indexed
        if idx < 1 or idx > len(self.log):
            return None
        return self.log[idx - 1]

    def truncate_from(self, idx: int) -> None:
        # remove entries with index >= idx
        if idx < 1:
            self.log.clear()
        else:
            self.log = self.log[: idx - 1]

    def append(self, entry: LogEntry) -> None:
        self.log.append(entry)


class _Persister:
    """Write-ahead persistence for Raft state.

    State is serialized as a single JSON file; for the assignment workload this
    is simple, durable, and easy to reason about.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    def load(self) -> PersistentState:
        if not os.path.exists(self.path):
            return PersistentState()
        with open(self.path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return PersistentState(
            current_term=data.get("current_term", 0),
            voted_for=data.get("voted_for"),
            log=[LogEntry.from_json(e) for e in data.get("log", [])],
        )

    def save(self, state: PersistentState) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "current_term": state.current_term,
                    "voted_for": state.voted_for,
                    "log": [e.to_json() for e in state.log],
                },
                fh,
            )
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)


@dataclass
class RequestVoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[dict[str, Any]]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int  # last index that matches the leader's log on this follower


PendingFuture = asyncio.Future


class RaftNode:
    def __init__(
        self,
        config: ClusterConfig,
        state_machine: StateMachine,
        metrics: Metrics | None = None,
    ) -> None:
        self.config = config
        self.state_machine = state_machine
        self.metrics = metrics or Metrics()

        self._client = HttpClient(timeout_ms=400)
        self._persister = _Persister(os.path.join(config.data_dir, f"raft-{config.node_id}.json"))
        self.state: PersistentState = self._persister.load()

        self.role: Role = Role.FOLLOWER
        self.leader_id: str | None = None
        self.commit_index: int = 0
        self.last_applied: int = 0

        # leader-only volatile state
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self._election_deadline: float = 0.0
        self._reset_election_timer()
        self._stop_event = asyncio.Event()
        self._main_task: asyncio.Task[None] | None = None

        # commit watchers: index -> [futures] resolved when committed and applied
        self._commit_waiters: dict[int, list[PendingFuture]] = {}

        self._step_lock = asyncio.Lock()

    # ----------------------- lifecycle --------------------------

    async def start(self) -> None:
        log.info("raft.start", node=self.config.node_id, term=self.state.current_term)
        self._main_task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._main_task:
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass
        await self._client.close()

    # ----------------------- public API --------------------------

    @property
    def is_leader(self) -> bool:
        return self.role is Role.LEADER

    def status(self) -> dict[str, Any]:
        return {
            "node_id": self.config.node_id,
            "role": self.role.value,
            "term": self.state.current_term,
            "leader_id": self.leader_id,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log_len": len(self.state.log),
            "voted_for": self.state.voted_for,
        }

    async def submit(self, command: dict[str, Any], timeout_s: float = 3.0) -> Any:
        """Append a command to the log and await its commit. Leader only."""
        async with self._step_lock:
            if self.role is not Role.LEADER:
                raise NotLeaderError(self.leader_id)
            entry = LogEntry(
                term=self.state.current_term,
                index=self.state.last_index() + 1,
                command=command,
            )
            self.state.append(entry)
            self._persister.save(self.state)
            self.match_index[self.config.node_id] = entry.index
            fut: PendingFuture = asyncio.get_event_loop().create_future()
            self._commit_waiters.setdefault(entry.index, []).append(fut)
            self.metrics.incr("raft_log_appended")

        # kick replication immediately
        asyncio.create_task(self._broadcast_append_entries())

        try:
            result = await asyncio.wait_for(fut, timeout=timeout_s)
            return result
        except asyncio.TimeoutError:
            raise CommitTimeoutError(entry.index) from None

    # --------------------- RPC handlers --------------------------

    async def handle_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        async with self._step_lock:
            if req.term > self.state.current_term:
                self._become_follower(req.term)

            if req.term < self.state.current_term:
                return RequestVoteResponse(self.state.current_term, False)

            up_to_date = (
                req.last_log_term > self.state.last_term()
                or (
                    req.last_log_term == self.state.last_term()
                    and req.last_log_index >= self.state.last_index()
                )
            )
            can_vote = self.state.voted_for in (None, req.candidate_id)

            if can_vote and up_to_date:
                self.state.voted_for = req.candidate_id
                self._persister.save(self.state)
                self._reset_election_timer()
                log.info(
                    "raft.vote_granted",
                    to=req.candidate_id,
                    term=req.term,
                )
                return RequestVoteResponse(self.state.current_term, True)
            return RequestVoteResponse(self.state.current_term, False)

    async def handle_append_entries(
        self, req: AppendEntriesRequest
    ) -> AppendEntriesResponse:
        async with self._step_lock:
            if req.term > self.state.current_term:
                self._become_follower(req.term)

            if req.term < self.state.current_term:
                return AppendEntriesResponse(self.state.current_term, False, 0)

            # valid leader for this term
            self._become_follower(req.term, leader_id=req.leader_id)

            # log consistency check
            if req.prev_log_index > 0:
                prev = self.state.entry_at(req.prev_log_index)
                if prev is None or prev.term != req.prev_log_term:
                    return AppendEntriesResponse(self.state.current_term, False, 0)

            # append / overwrite entries
            for raw in req.entries:
                e = LogEntry.from_json(raw)
                existing = self.state.entry_at(e.index)
                if existing and existing.term != e.term:
                    self.state.truncate_from(e.index)
                if not self.state.entry_at(e.index):
                    self.state.append(e)
            if req.entries:
                self._persister.save(self.state)

            # commit index advance
            if req.leader_commit > self.commit_index:
                last_new = req.entries[-1]["index"] if req.entries else self.state.last_index()
                self.commit_index = min(req.leader_commit, last_new)

        await self._apply_committed()
        return AppendEntriesResponse(
            term=self.state.current_term,
            success=True,
            match_index=self.state.last_index(),
        )

    # --------------------- main loop --------------------------

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                if self.role is Role.LEADER:
                    await self._broadcast_append_entries()
                    await asyncio.sleep(self.config.raft_heartbeat_ms / 1000.0)
                else:
                    now = time.monotonic()
                    if now >= self._election_deadline:
                        await self._start_election()
                    else:
                        await asyncio.sleep(min(0.05, self._election_deadline - now))
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # pragma: no cover
            log.error("raft.loop_crashed", err=str(exc))

    # ---------------------- elections --------------------------

    async def _start_election(self) -> None:
        async with self._step_lock:
            self.role = Role.CANDIDATE
            self.state.current_term += 1
            self.state.voted_for = self.config.node_id
            self._persister.save(self.state)
            self.leader_id = None
            term = self.state.current_term
            last_idx = self.state.last_index()
            last_term = self.state.last_term()
            self._reset_election_timer()
            self.metrics.incr("raft_elections_started")
            log.info("raft.election_start", term=term)

        votes = 1  # self
        majority = (len(self.config.peers) // 2) + 1

        async def request(peer_id: str, url: str) -> None:
            nonlocal votes
            try:
                with self.metrics.timer("raft_rpc_request_vote_ms"):
                    resp = await self._client.post_json(
                        url,
                        {
                            "term": term,
                            "candidate_id": self.config.node_id,
                            "last_log_index": last_idx,
                            "last_log_term": last_term,
                        },
                    )
            except Exception:
                return
            async with self._step_lock:
                if resp.get("term", 0) > self.state.current_term:
                    self._become_follower(resp["term"])
                    return
                if (
                    self.role is Role.CANDIDATE
                    and self.state.current_term == term
                    and resp.get("vote_granted")
                ):
                    votes += 1
                    if votes >= majority:
                        self._become_leader()

        peers = self.config.other_peers
        await asyncio.gather(
            *(request(p.id, f"{p.url}/raft/request_vote") for p in peers),
            return_exceptions=True,
        )

    def _become_leader(self) -> None:
        if self.role is Role.LEADER:
            return
        self.role = Role.LEADER
        self.leader_id = self.config.node_id
        last = self.state.last_index()
        self.next_index = {p.id: last + 1 for p in self.config.other_peers}
        self.match_index = {p.id: 0 for p in self.config.other_peers}
        self.match_index[self.config.node_id] = last
        self.metrics.incr("raft_leader_transitions")
        log.info("raft.became_leader", term=self.state.current_term, last_idx=last)

    def _become_follower(self, term: int, leader_id: str | None = None) -> None:
        if term > self.state.current_term:
            self.state.current_term = term
            self.state.voted_for = None
            self._persister.save(self.state)
        self.role = Role.FOLLOWER
        if leader_id:
            self.leader_id = leader_id
        self._reset_election_timer()

    def _reset_election_timer(self) -> None:
        ms = random.randint(
            self.config.raft_election_min_ms, self.config.raft_election_max_ms
        )
        self._election_deadline = time.monotonic() + (ms / 1000.0)

    # --------------------- replication --------------------------

    async def _broadcast_append_entries(self) -> None:
        if self.role is not Role.LEADER:
            return
        peers = self.config.other_peers
        await asyncio.gather(
            *(self._send_append_entries(p.id, p.url) for p in peers),
            return_exceptions=True,
        )
        await self._advance_commit_index()
        await self._apply_committed()

    async def _send_append_entries(self, peer_id: str, peer_url: str) -> None:
        if self.role is not Role.LEADER:
            return
        next_idx = self.next_index.get(peer_id, 1)
        prev_idx = next_idx - 1
        prev = self.state.entry_at(prev_idx) if prev_idx > 0 else None
        prev_term = prev.term if prev else 0
        entries = [e.to_json() for e in self.state.log[prev_idx:]]
        payload = {
            "term": self.state.current_term,
            "leader_id": self.config.node_id,
            "prev_log_index": prev_idx,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": self.commit_index,
        }
        try:
            with self.metrics.timer("raft_rpc_append_entries_ms"):
                resp = await self._client.post_json(
                    f"{peer_url}/raft/append_entries", payload
                )
        except Exception:
            return

        async with self._step_lock:
            if resp.get("term", 0) > self.state.current_term:
                self._become_follower(resp["term"])
                return
            if not resp.get("success", False):
                # consistency check failed; retreat
                self.next_index[peer_id] = max(1, next_idx - 1)
                return
            self.match_index[peer_id] = resp.get("match_index", prev_idx + len(entries))
            self.next_index[peer_id] = self.match_index[peer_id] + 1

    async def _advance_commit_index(self) -> None:
        if self.role is not Role.LEADER:
            return
        # find largest N s.t. a majority has match_index >= N AND log[N].term == currentTerm
        match_vals = sorted(self.match_index.values())
        if not match_vals:
            return
        majority_idx = len(match_vals) // 2  # because peers + self
        candidate_n = match_vals[majority_idx]
        if candidate_n <= self.commit_index:
            return
        entry = self.state.entry_at(candidate_n)
        if entry and entry.term == self.state.current_term:
            self.commit_index = candidate_n

    async def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.state.entry_at(self.last_applied)
            if entry is None:
                break
            try:
                with self.metrics.timer("raft_apply_ms"):
                    result = await self.state_machine.apply(entry)
                self.metrics.incr("raft_log_applied")
            except Exception as exc:  # state machine error shouldn't kill the node
                log.error("raft.apply_failed", index=entry.index, err=str(exc))
                result = {"error": str(exc)}
            waiters = self._commit_waiters.pop(entry.index, [])
            for fut in waiters:
                if not fut.done():
                    fut.set_result(result)


class NotLeaderError(Exception):
    def __init__(self, leader_id: str | None) -> None:
        super().__init__(f"not leader; current leader: {leader_id}")
        self.leader_id = leader_id


class CommitTimeoutError(Exception):
    def __init__(self, index: int) -> None:
        super().__init__(f"commit timeout at index {index}")
        self.index = index
