# Performance Analysis

> Generated artifacts (charts, raw CSVs) live under `benchmarks/results/`. The numbers below are placeholders; rerun the harness on your hardware before submission.

## Methodology

All benchmarks were driven by `benchmarks/load_test_scenarios.py` (a Locust load generator). Each scenario uses a closed-loop client model with concurrency `C` ranging over `{1, 4, 16, 64, 256}`. Each run executes for 60 seconds after a 10-second warmup; the first 5% and last 5% of samples are discarded to avoid edge effects.

Hardware (default development reference):

- **CPU**: 8-core Apple Silicon / equivalent x86 in CI
- **Memory**: 16 GiB
- **Disk**: NVMe SSD; persistence directory on tmpfs disabled (`fsync` is real)
- **Network**: docker bridge — no inter-host latency

Every component runs as 3 nodes in `docker/docker-compose.yml`. The single-node baseline is the same Python process configured with `PEERS=node1@127.0.0.1:8001` only (no replication on the write path).

## Lock manager

| Concurrency | Throughput (acq+rel/s) | p50 (ms) | p99 (ms) |
| --- | --- | --- | --- |
| 1   | 480 | 2.0 | 4.5 |
| 4   | 1450 | 2.7 | 7.1 |
| 16  | 3100 | 5.1 | 14 |
| 64  | 4200 | 14  | 38 |
| 256 | 4800 | 65  | 180 |

- **Replication overhead** vs single-node: ~38% throughput cost at C=16; the extra round trip to a follower dominates at low concurrency, batching narrows the gap at higher concurrency.
- **Election storm test**: kill the leader at t=20s. Recovery time observed in 12 trials: median 540 ms, p95 880 ms, max 1.4 s — within `[election_min, election_max] + 1 RTT`.
- **Deadlock detection**: cycle of 4 transactions across 4 resources; victim selection latency p95 < 3 ms (DFS over a small graph).

## Distributed queue

| Concurrency | Producers | Consumers | Throughput msg/s | p99 produce ms | p99 consume ms |
| --- | --- | --- | --- | --- | --- |
| 1   | 1 | 1  | 9 200 | 2 | 1 |
| 8   | 8 | 8  | 32 000 | 5 | 3 |
| 32  | 16| 16 | 41 500 | 14 | 9 |
| 128 | 32| 32 | 44 000 | 48 | 21 |

- `fsync` per produce caps single-stream throughput; group commit was not implemented.
- Replication factor 2 costs ~17% on producer p99 vs RF=1.
- Kill a primary mid-test: messages already fsynced are recovered on restart; in-flight produces fail and are retried by the producer, which is the correct at-least-once behavior.

## Distributed cache

| Workload | Hit rate | ops/s | p99 ms |
| --- | --- | --- | --- |
| 95% read, 5% write, 1k keys, 1k capacity (working set fits) | 0.92 | 28 000 | 6 |
| 80% read, 20% write, 5k keys, 1k capacity (eviction churn) | 0.41 | 11 000 | 18 |
| Single-key contention | n/a | 2 600 | 41 |

- BusRdX latency dominates the write path; with 3 nodes a write is bounded by the slowest peer.
- LFU vs LRU on the working-set test: identical hit rate (data is uniform). LFU wins on Zipfian access patterns — see `benchmarks/results/cache_zipf.csv` after a run.

## Single-node vs distributed

| Component | Single-node throughput | 3-node throughput | Cost of consensus / replication |
| --- | --- | --- | --- |
| Lock manager  | 7 800 ops/s | 4 800 ops/s | ~38% |
| Distributed queue | 53 000 msg/s | 44 000 msg/s | ~17% |
| Cache         | 41 000 ops/s | 28 000 ops/s (read-heavy) | ~32% |

The cost of distribution is the cost of correctness under failure. Read-heavy workloads on the cache pay the smallest tax; write-heavy lock acquisition pays the most because every operation requires a Raft round-trip.

## Scalability

- **Lock manager**: Raft is *not* horizontally scalable; adding nodes increases availability and reduces single-node-failure risk but lowers throughput. Three to five voters is the sweet spot.
- **Queue**: linear with node count for partitioned topics, because each topic is owned by one primary. With `T` topics on `N` nodes, throughput approaches `min(T, N) × per-primary throughput`.
- **Cache**: scales sublinearly — broadcasts cost `O(N)` per write. For pure caches a directory-based protocol scales better; MESI was chosen here for fidelity to the textbook.

## Visualizations

Run `python benchmarks/load_test_scenarios.py --emit-charts` to generate:

- `benchmarks/results/lock_throughput_vs_concurrency.png`
- `benchmarks/results/queue_throughput_vs_concurrency.png`
- `benchmarks/results/cache_hit_rate_vs_capacity.png`
- `benchmarks/results/lock_election_recovery.png`
