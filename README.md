# Distributed Synchronization System

Implementasi Python dari tiga komponen utama sistem terdistribusi:

| Komponen | Algoritma | Tujuan |
| --- | --- | --- |
| **Distributed Lock Manager** | Raft consensus | Lock shared/exclusive terreplikasi dengan deteksi deadlock |
| **Distributed Queue** | Consistent hashing + replikasi | Pengiriman at-least-once, persisten, tahan terhadap kegagalan node |
| **Distributed Cache** | Protokol koherensi MESI | Cache multi-node dengan invalidasi berbasis snoop, eviction LRU/LFU |

Setiap komponen berjalan sebagai layanan HTTP `aiohttp`. RPC antar-node menggunakan JSON over HTTP. State disimpan ke disk melalui append-only log dan snapshot JSON atomik.

> Tugas 3 — Sistem Parallel dan Terdistribusi

## Quickstart (Docker Compose)

```bash
cp .env.example .env
docker compose -f docker/docker-compose.yml --profile all up -d --build
```

Endpoint (host -> container):

| Layanan | Port host |
| --- | --- |
| `lock1`, `lock2`, `lock3`, `lock4`, `lock5` | `18011`, `18012`, `18013`, `18014`, `18015` |
| `queue1`, `queue2`, `queue3`, `queue4` | `18021`, `18022`, `18023`, `18024` |
| `cache1`, `cache2`, `cache3` | `18031`, `18032`, `18033` |
| `redis` | `6379` (internal only) |

```bash
# Cari Raft leader pada cluster lock
for p in 18011 18012 18013 18014 18015; do
  curl -s "http://127.0.0.1:$p/status" | python -m json.tool | grep -E 'role|leader_id'
done

# Ambil exclusive lock
curl -s -X POST http://127.0.0.1:18011/lock/acquire \
  -H 'content-type: application/json' \
  -d '{"resource":"orders","owner":"tx-1","mode":"exclusive"}'
```

## Pengembangan Lokal

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=. NODE_ID=node1 NODE_BIND=127.0.0.1:8001 \
  PEERS='node1@127.0.0.1:8001,node2@127.0.0.1:8002,node3@127.0.0.1:8003' \
  DATA_DIR=./data/node1 python -m src.nodes.lock_manager
# di shell lain, gunakan NODE_ID=node2, port 8002, dst.
```

## Dokumentasi

- [Arsitektur](docs/architecture.md) — desain sistem, algoritma, diagram
- [Panduan deployment](docs/deployment_guide.md) — instalasi, scaling, troubleshooting
- [Spesifikasi API](docs/api_spec.yaml) — OpenAPI 3 untuk semua endpoint
- [Laporan performa](docs/performance.md) — benchmark, analisis skalabilitas

## Struktur Repositori

```
distributed-sync-system/
├── src/
│   ├── nodes/         base_node, lock_manager, queue_node, cache_node
│   ├── consensus/     raft
│   ├── communication/ message_passing, failure_detector
│   └── utils/         config, logging_setup, metrics
├── tests/             unit, integration, performance
├── docker/            Dockerfile.node, docker-compose.yml
├── docs/              architecture, deployment, api_spec, performance
├── benchmarks/        load_test_scenarios.py (locust)
└── requirements.txt
```

## Video Demo

> *Ganti dengan link YouTube sebelum pengumpulan.*

`https://www.youtube.com/watch?v=YOUR_VIDEO_ID`
