"""Cluster configuration loaded from environment variables.

Each component (lock manager, queue, cache) shares the same membership format:

    PEERS=node1@host:port,node2@host:port,...

The local node identifies itself by NODE_ID and binds to NODE_BIND.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Peer:
    id: str
    address: str  # host:port

    @classmethod
    def parse(cls, spec: str) -> "Peer":
        if "@" not in spec:
            raise ValueError(f"expected 'id@host:port', got {spec!r}")
        node_id, address = spec.split("@", 1)
        return cls(id=node_id.strip(), address=address.strip())

    @property
    def url(self) -> str:
        return f"http://{self.address}"


@dataclass
class ClusterConfig:
    node_id: str
    bind: str
    peers: list[Peer] = field(default_factory=list)
    data_dir: str = "/var/lib/dsync"
    redis_url: str | None = None

    raft_heartbeat_ms: int = 120
    raft_election_min_ms: int = 400
    raft_election_max_ms: int = 800

    cache_capacity: int = 1024
    cache_policy: str = "LRU"

    queue_replication_factor: int = 2
    queue_virtual_nodes: int = 128

    @property
    def peer_ids(self) -> list[str]:
        return [p.id for p in self.peers]

    @property
    def other_peers(self) -> list[Peer]:
        return [p for p in self.peers if p.id != self.node_id]

    def peer(self, node_id: str) -> Peer | None:
        return next((p for p in self.peers if p.id == node_id), None)

    @classmethod
    def from_env(cls) -> "ClusterConfig":
        node_id = os.environ.get("NODE_ID") or _required("NODE_ID")
        bind = os.environ.get("NODE_BIND", "0.0.0.0:8001")
        peers_raw = os.environ.get("PEERS", "")
        peers = [Peer.parse(p) for p in peers_raw.split(",") if p.strip()]
        if not any(p.id == node_id for p in peers):
            # implicit self when not listed
            host, _, port = bind.partition(":")
            peers.append(Peer(id=node_id, address=f"{host or 'localhost'}:{port or '8001'}"))
        return cls(
            node_id=node_id,
            bind=bind,
            peers=peers,
            data_dir=os.environ.get("DATA_DIR", "/var/lib/dsync"),
            redis_url=os.environ.get("REDIS_URL") or None,
            raft_heartbeat_ms=int(os.environ.get("RAFT_HEARTBEAT_MS", "120")),
            raft_election_min_ms=int(os.environ.get("RAFT_ELECTION_MIN_MS", "400")),
            raft_election_max_ms=int(os.environ.get("RAFT_ELECTION_MAX_MS", "800")),
            cache_capacity=int(os.environ.get("CACHE_CAPACITY", "1024")),
            cache_policy=os.environ.get("CACHE_POLICY", "LRU").upper(),
            queue_replication_factor=int(os.environ.get("QUEUE_REPLICATION_FACTOR", "2")),
            queue_virtual_nodes=int(os.environ.get("QUEUE_VIRTUAL_NODES", "128")),
        )


def _required(key: str) -> str:
    raise RuntimeError(f"missing required env var: {key}")
