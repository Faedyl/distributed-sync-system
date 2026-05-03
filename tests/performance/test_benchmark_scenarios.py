"""Benchmark tests that spin up real node clusters and drive each scenario.

Each test starts a 3-node cluster (same approach as test_smoke.py), runs a
short benchmark scenario, then asserts that at least one successful operation
was recorded and that latency metrics are populated.

Run with:
    pytest -q tests/performance/
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

import httpx
import pytest

from benchmarks.load_test_scenarios import run_cache, run_lock, run_queue

REPO_ROOT = Path(__file__).resolve().parents[2]

BENCHMARK_DURATION = 5  # seconds — short enough for CI


def _port_free(port: int) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _start(
    component: str,
    node_id: str,
    port: int,
    peers: str,
    data_dir: Path,
    extra_env: dict[str, str] | None = None,
) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(REPO_ROOT),
            "NODE_ID": node_id,
            "NODE_BIND": f"127.0.0.1:{port}",
            "PEERS": peers,
            "DATA_DIR": str(data_dir),
            "LOG_LEVEL": "WARNING",
        }
    )
    if extra_env:
        env.update(extra_env)
    return subprocess.Popen(
        [sys.executable, "-m", f"src.nodes.{component}"],
        env=env,
        cwd=str(REPO_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _kill_all(procs: list[subprocess.Popen[bytes]]) -> None:
    for p in procs:
        try:
            p.kill()
        except Exception:
            pass
    for p in procs:
        try:
            p.wait(timeout=3)
        except Exception:
            pass


async def _wait_healthy(ports: list[int], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient(timeout=1.0) as client:
        for p in ports:
            while time.monotonic() < deadline:
                try:
                    r = await client.get(f"http://127.0.0.1:{p}/healthz")
                    if r.status_code == 200:
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.1)
            else:
                raise TimeoutError(f"port {p} never became healthy")


async def _wait_leader(ports: list[int], timeout: float = 10.0) -> str:
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient(timeout=1.0) as client:
        while time.monotonic() < deadline:
            for p in ports:
                try:
                    r = await client.get(f"http://127.0.0.1:{p}/status")
                    if r.json().get("raft", {}).get("role") == "leader":
                        return f"http://127.0.0.1:{p}"
                except Exception:
                    pass
            await asyncio.sleep(0.1)
    raise TimeoutError("no leader elected within timeout")


# ---------------------------------------------------------------------------
# lock benchmark
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_benchmark_lock(tmp_path: Path) -> None:
    base = 9401
    ports = [base, base + 1, base + 2]
    for p in ports:
        if not _port_free(p):
            pytest.skip(f"port {p} in use")

    peers = ",".join(f"bl{i+1}@127.0.0.1:{ports[i]}" for i in range(3))
    procs = [
        _start("lock_manager", f"bl{i+1}", ports[i], peers, tmp_path / f"bl{i+1}")
        for i in range(3)
    ]
    try:
        await _wait_healthy(ports)
        await _wait_leader(ports)

        urls = [f"http://127.0.0.1:{p}" for p in ports]
        result = await run_lock(urls, concurrency=8, duration_s=BENCHMARK_DURATION)

        print(f"\n[lock benchmark] {result}")
        assert result["ops"] > 0, "no lock ops completed"
        assert result["throughput_ops_s"] > 0
        assert result["p99_ms"] > 0
    finally:
        _kill_all(procs)


# ---------------------------------------------------------------------------
# queue benchmark
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_benchmark_queue(tmp_path: Path) -> None:
    base = 9411
    ports = [base, base + 1, base + 2]
    for p in ports:
        if not _port_free(p):
            pytest.skip(f"port {p} in use")

    peers = ",".join(f"bq{i+1}@127.0.0.1:{ports[i]}" for i in range(3))
    procs = [
        _start(
            "queue_node",
            f"bq{i+1}",
            ports[i],
            peers,
            tmp_path / f"bq{i+1}",
            extra_env={"QUEUE_REPLICATION_FACTOR": "2"},
        )
        for i in range(3)
    ]
    try:
        await _wait_healthy(ports)

        urls = [f"http://127.0.0.1:{p}" for p in ports]
        result = await run_queue(
            urls, producers=4, consumers=4, duration_s=BENCHMARK_DURATION
        )

        print(f"\n[queue benchmark] {result}")
        assert result["ops"] > 0, "no queue produce ops completed"
        assert result["throughput_ops_s"] > 0
    finally:
        _kill_all(procs)


# ---------------------------------------------------------------------------
# cache benchmark
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_benchmark_cache(tmp_path: Path) -> None:
    base = 9421
    ports = [base, base + 1, base + 2]
    for p in ports:
        if not _port_free(p):
            pytest.skip(f"port {p} in use")

    peers = ",".join(f"bc{i+1}@127.0.0.1:{ports[i]}" for i in range(3))
    procs = [
        _start(
            "cache_node",
            f"bc{i+1}",
            ports[i],
            peers,
            tmp_path / f"bc{i+1}",
            extra_env={"CACHE_CAPACITY": "64"},
        )
        for i in range(3)
    ]
    try:
        await _wait_healthy(ports)

        urls = [f"http://127.0.0.1:{p}" for p in ports]
        result = await run_cache(
            urls,
            concurrency=8,
            duration_s=BENCHMARK_DURATION,
            num_keys=200,
            write_ratio=0.1,
        )

        print(f"\n[cache benchmark] {result}")
        assert result["ops"] > 0, "no cache ops completed"
        assert result["throughput_ops_s"] > 0
        assert 0.0 <= result["hit_rate"] <= 1.0
    finally:
        _kill_all(procs)
