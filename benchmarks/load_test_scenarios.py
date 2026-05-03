"""Load-test scenarios for the distributed-sync-system.

Two modes are supported:

1. ``locust`` — run via ``locust -f benchmarks/load_test_scenarios.py``.
2. Standalone — ``python benchmarks/load_test_scenarios.py --scenario=lock``
   for a quick non-Locust closed-loop driver useful in CI / video demos.

Scenarios:
* ``lock``   acquire/release cycle on a small key space; reports throughput and p99
* ``queue``  producer/consumer fan-out with periodic ack
* ``cache``  zipfian read/write mix
"""

from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import string
import time
import uuid
from typing import Any

import httpx


def _percentile(samples: list[float], p: float) -> float:
    if not samples:
        return 0.0
    s = sorted(samples)
    k = max(0, min(len(s) - 1, int(round((len(s) - 1) * p))))
    return s[k]


def _summary(latencies_ms: list[float], total_ops: int, duration_s: float) -> dict[str, Any]:
    return {
        "ops": total_ops,
        "duration_s": round(duration_s, 2),
        "throughput_ops_s": round(total_ops / duration_s, 1) if duration_s > 0 else 0.0,
        "p50_ms": round(_percentile(latencies_ms, 0.50), 2),
        "p95_ms": round(_percentile(latencies_ms, 0.95), 2),
        "p99_ms": round(_percentile(latencies_ms, 0.99), 2),
        "max_ms": round(max(latencies_ms) if latencies_ms else 0.0, 2),
        "mean_ms": round(statistics.mean(latencies_ms) if latencies_ms else 0.0, 2),
    }


# ---------------------------------------------------------------------------
# leader discovery
# ---------------------------------------------------------------------------


async def _find_leader(client: httpx.AsyncClient, urls: list[str]) -> str:
    last_seen: str | None = None
    for url in urls:
        try:
            r = await client.get(f"{url}/status", timeout=1.5)
            data = r.json()
            raft = data.get("raft", {})
            if raft.get("role") == "leader":
                return url
            last_seen = raft.get("leader_id") or last_seen
        except Exception:
            continue
    if last_seen:
        for url in urls:
            if last_seen in url:
                return url
    return urls[0]


# ---------------------------------------------------------------------------
# lock scenario
# ---------------------------------------------------------------------------


async def run_lock(urls: list[str], concurrency: int, duration_s: int) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=httpx.Timeout(2.0, connect=0.5)) as client:
        leader = await _find_leader(client, urls)
        print(f"[lock] leader={leader} concurrency={concurrency} duration={duration_s}s")
        latencies: list[float] = []
        ops = 0
        deadline = time.monotonic() + duration_s

        async def worker() -> None:
            nonlocal ops
            owner = f"tx-{uuid.uuid4().hex[:8]}"
            while time.monotonic() < deadline:
                resource = random.choice(["r1", "r2", "r3", "r4", "r5"])
                t0 = time.perf_counter()
                try:
                    r = await client.post(
                        f"{leader}/lock/acquire",
                        json={"resource": resource, "owner": owner, "mode": "exclusive",
                              "timeout_ms": 1000},
                    )
                    if r.status_code == 200 and r.json().get("status") == "granted":
                        await client.post(
                            f"{leader}/lock/release",
                            json={"resource": resource, "owner": owner},
                        )
                        ops += 1
                        latencies.append((time.perf_counter() - t0) * 1000)
                except Exception:
                    pass

        start = time.monotonic()
        await asyncio.gather(*(worker() for _ in range(concurrency)))
        return _summary(latencies, ops, time.monotonic() - start)


# ---------------------------------------------------------------------------
# queue scenario
# ---------------------------------------------------------------------------


async def run_queue(urls: list[str], producers: int, consumers: int, duration_s: int) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=httpx.Timeout(2.0, connect=0.5)) as client:
        any_node = urls[0]
        print(f"[queue] producers={producers} consumers={consumers} duration={duration_s}s")
        latencies: list[float] = []
        ops = 0
        deadline = time.monotonic() + duration_s

        async def producer(idx: int) -> None:
            nonlocal ops
            topic = random.choice(["orders", "events", "billing", "audit"])
            while time.monotonic() < deadline:
                t0 = time.perf_counter()
                try:
                    r = await client.post(
                        f"{any_node}/queue/produce",
                        json={"topic": topic, "payload": {"idx": idx, "ts": time.time()}},
                    )
                    if r.status_code == 200:
                        ops += 1
                        latencies.append((time.perf_counter() - t0) * 1000)
                except Exception:
                    pass

        async def consumer(cid: int) -> None:
            consumer_id = f"c-{cid}"
            while time.monotonic() < deadline:
                topic = random.choice(["orders", "events", "billing", "audit"])
                try:
                    r = await client.post(
                        f"{any_node}/queue/consume",
                        json={"topic": topic, "consumer_id": consumer_id, "count": 16},
                    )
                    if r.status_code == 200:
                        for m in r.json().get("messages", []):
                            await client.post(
                                f"{any_node}/queue/ack",
                                json={"topic": topic, "consumer_id": consumer_id, "seq": m["seq"]},
                            )
                except Exception:
                    pass
                await asyncio.sleep(0.005)

        start = time.monotonic()
        await asyncio.gather(
            *(producer(i) for i in range(producers)),
            *(consumer(i) for i in range(consumers)),
        )
        return _summary(latencies, ops, time.monotonic() - start)


# ---------------------------------------------------------------------------
# cache scenario (Zipfian)
# ---------------------------------------------------------------------------


def _zipf_sampler(num_keys: int, alpha: float = 1.1) -> "callable[[], int]":
    weights = [1.0 / (i ** alpha) for i in range(1, num_keys + 1)]
    total = sum(weights)
    cdf = []
    acc = 0.0
    for w in weights:
        acc += w / total
        cdf.append(acc)

    def sample() -> int:
        r = random.random()
        lo, hi = 0, num_keys - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if cdf[mid] >= r:
                hi = mid
            else:
                lo = mid + 1
        return lo

    return sample


async def run_cache(
    urls: list[str], concurrency: int, duration_s: int, num_keys: int, write_ratio: float
) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=httpx.Timeout(2.0, connect=0.5)) as client:
        sample = _zipf_sampler(num_keys)
        print(
            f"[cache] concurrency={concurrency} duration={duration_s}s keys={num_keys} "
            f"write_ratio={write_ratio}"
        )
        latencies: list[float] = []
        ops = 0
        hits = 0
        deadline = time.monotonic() + duration_s

        async def worker() -> None:
            nonlocal ops, hits
            while time.monotonic() < deadline:
                node = random.choice(urls)
                key = f"k{sample()}"
                t0 = time.perf_counter()
                try:
                    if random.random() < write_ratio:
                        r = await client.post(
                            f"{node}/cache/set",
                            json={"key": key, "value": {"v": uuid.uuid4().hex[:6]}},
                        )
                    else:
                        r = await client.post(f"{node}/cache/get", json={"key": key})
                        if r.status_code == 200 and r.json().get("source") == "local":
                            hits += 1
                    if r.status_code in (200, 404):
                        ops += 1
                        latencies.append((time.perf_counter() - t0) * 1000)
                except Exception:
                    pass

        start = time.monotonic()
        await asyncio.gather(*(worker() for _ in range(concurrency)))
        result = _summary(latencies, ops, time.monotonic() - start)
        result["hit_rate"] = round(hits / ops, 3) if ops else 0.0
        return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


DEFAULT_URLS = {
    "lock": ["http://127.0.0.1:18011", "http://127.0.0.1:18012", "http://127.0.0.1:18013"],
    "queue": ["http://127.0.0.1:18021", "http://127.0.0.1:18022", "http://127.0.0.1:18023"],
    "cache": ["http://127.0.0.1:18031", "http://127.0.0.1:18032", "http://127.0.0.1:18033"],
}


async def _amain() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["lock", "queue", "cache"], required=True)
    parser.add_argument("--urls", nargs="+", default=None)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--producers", type=int, default=8)
    parser.add_argument("--consumers", type=int, default=8)
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--write-ratio", type=float, default=0.05)
    args = parser.parse_args()

    urls = args.urls or DEFAULT_URLS[args.scenario]

    if args.scenario == "lock":
        result = await run_lock(urls, args.concurrency, args.duration)
    elif args.scenario == "queue":
        result = await run_queue(urls, args.producers, args.consumers, args.duration)
    else:
        result = await run_cache(urls, args.concurrency, args.duration, args.keys, args.write_ratio)

    print()
    for k, v in result.items():
        print(f"  {k:>18}: {v}")


# ---------------------------------------------------------------------------
# Locust integration (optional)
# ---------------------------------------------------------------------------

try:
    from locust import HttpUser, task, between  # type: ignore

    class LockUser(HttpUser):
        wait_time = between(0.0, 0.05)
        host = "http://127.0.0.1:18011"

        @task
        def acq_rel(self) -> None:
            owner = f"tx-{uuid.uuid4().hex[:6]}"
            res = random.choice(["r1", "r2", "r3"])
            r = self.client.post(
                "/lock/acquire",
                json={"resource": res, "owner": owner, "mode": "exclusive",
                      "timeout_ms": 1000},
                name="POST /lock/acquire",
            )
            if r.status_code == 200 and r.json().get("status") == "granted":
                self.client.post(
                    "/lock/release",
                    json={"resource": res, "owner": owner},
                    name="POST /lock/release",
                )

    class QueueUser(HttpUser):
        wait_time = between(0.0, 0.02)
        host = "http://127.0.0.1:18021"

        @task(3)
        def produce(self) -> None:
            self.client.post(
                "/queue/produce",
                json={"topic": "events", "payload": {"x": uuid.uuid4().hex[:6]}},
                name="POST /queue/produce",
            )

        @task(2)
        def consume(self) -> None:
            r = self.client.post(
                "/queue/consume",
                json={"topic": "events", "consumer_id": f"u-{uuid.uuid4().hex[:4]}", "count": 8},
                name="POST /queue/consume",
            )
            if r.status_code == 200:
                for m in r.json().get("messages", []):
                    self.client.post(
                        "/queue/ack",
                        json={"topic": "events", "consumer_id": "u-locust", "seq": m["seq"]},
                        name="POST /queue/ack",
                    )

    class CacheUser(HttpUser):
        wait_time = between(0.0, 0.02)
        host = "http://127.0.0.1:18031"

        @task(9)
        def get(self) -> None:
            self.client.post("/cache/get",
                             json={"key": f"k{random.randint(0, 999)}"},
                             name="POST /cache/get")

        @task(1)
        def set(self) -> None:
            self.client.post("/cache/set",
                             json={"key": f"k{random.randint(0, 999)}", "value": uuid.uuid4().hex},
                             name="POST /cache/set")

except Exception:
    # locust isn't installed — fine, standalone mode still works
    pass


if __name__ == "__main__":
    asyncio.run(_amain())
