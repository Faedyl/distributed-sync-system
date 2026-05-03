# Distributed Synchronization System

Python implementation of three core building blocks of a distributed system:

| Component | Algorithm | Purpose |
| --- | --- | --- |
| **Distributed Lock Manager** | Raft consensus | Replicated shared/exclusive locks with deadlock detection |
| **Distributed Queue** | Consistent hashing + replication | At-least-once delivery, persistent, survives node failure |
| **Distributed Cache** | MESI coherence protocol | Multi-node cache with snoop-based invalidation, LRU/LFU eviction |

Each component runs as an `aiohttp` HTTP service. Inter-node RPC is JSON over HTTP. State is persisted to disk via append-only logs and atomic JSON snapshots.

> Tugas 3 — Sistem Parallel dan Terdistribusi

## Quickstart (Docker Compose)

```bash
cp .env.example .env
docker compose -f docker/docker-compose.yml --profile all up -d --build
```

To scale a cluster (e.g. 5-node Raft for higher fault tolerance), regenerate the compose file:

```bash
python tools/gen_compose.py --lock 5 --queue 4 --cache 3 -o docker/docker-compose.yml
docker compose -f docker/docker-compose.yml --profile all up -d --build
```

Endpoints (host -> container):

| Service | Host port |
| --- | --- |
| `lock1`, `lock2`, `lock3` | `18011`, `18012`, `18013` |
| `queue1`, `queue2`, `queue3` | `18021`, `18022`, `18023` |
| `cache1`, `cache2`, `cache3` | `18031`, `18032`, `18033` |

```bash
# Find the Raft leader for the lock cluster
for p in 18011 18012 18013; do
  curl -s "http://127.0.0.1:$p/status" | python -m json.tool | grep -E 'role|leader_id'
done

# Acquire an exclusive lock
curl -s -X POST http://127.0.0.1:18011/lock/acquire \
  -H 'content-type: application/json' \
  -d '{"resource":"orders","owner":"tx-1","mode":"exclusive"}'
```

## Local development

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=. NODE_ID=node1 NODE_BIND=127.0.0.1:8001 \
  PEERS='node1@127.0.0.1:8001,node2@127.0.0.1:8002,node3@127.0.0.1:8003' \
  DATA_DIR=./data/node1 python -m src.nodes.lock_manager
# in another shell, NODE_ID=node2, port 8002, etc.
```

## Documentation

- [Architecture](docs/architecture.md) — system design, algorithms, diagrams
- [Deployment guide](docs/deployment_guide.md) — install, scale, troubleshoot
- [API spec](docs/api_spec.yaml) — OpenAPI 3 for all endpoints
- [Performance report](docs/performance.md) — benchmarks, scalability analysis

## Repository layout

```
distributed-sync-system/
├── src/
│   ├── nodes/         base_node, lock_manager, queue_node, cache_node
│   ├── consensus/     raft (and pbft, optional)
│   ├── communication/ message_passing, failure_detector
│   └── utils/         config, logging, metrics
├── tests/             unit, integration, performance
├── docker/            Dockerfile.node, docker-compose.yml
├── docs/              architecture, deployment, api_spec, performance
├── benchmarks/        load_test_scenarios.py (locust)
└── requirements.txt
```

## Video demo

> *Replace with the YouTube link before submission.*

`https://www.youtube.com/watch?v=YOUR_VIDEO_ID`
