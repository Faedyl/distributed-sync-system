"""End-to-end smoke tests.

Spawns 3-node clusters as subprocesses and drives them via HTTP. Skipped if
any port in the chosen range is busy. Tests are designed to mirror the manual
verification done during development.

Run with:
    pytest -q tests/integration/test_smoke.py
"""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

import httpx
import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]


def _port_free(port: int) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _start(component: str, node_id: str, port: int, peers: str, data_dir: Path,
           extra_env: dict[str, str] | None = None) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env.update({
        "PYTHONPATH": str(REPO_ROOT),
        "NODE_ID": node_id,
        "NODE_BIND": f"127.0.0.1:{port}",
        "PEERS": peers,
        "DATA_DIR": str(data_dir),
        "LOG_LEVEL": "WARNING",
    })
    if extra_env:
        env.update(extra_env)
    py = sys.executable
    return subprocess.Popen(
        [py, "-m", f"src.nodes.{component}"],
        env=env,
        cwd=str(REPO_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


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
                raise TimeoutError(f"port {p} never came up")


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


# ---------------------------------------------------------------------------
# Lock manager
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lock_manager_election_and_acquire(tmp_path: Path) -> None:
    base = 9301
    ports = [base, base + 1, base + 2]
    for p in ports:
        if not _port_free(p):
            pytest.skip(f"port {p} in use")
    peers = ",".join(f"lk{i+1}@127.0.0.1:{ports[i]}" for i in range(3))
    procs = [
        _start("lock_manager", f"lk{i+1}", ports[i], peers, tmp_path / f"lk{i+1}")
        for i in range(3)
    ]
    try:
        await _wait_healthy(ports)
        # find leader
        async with httpx.AsyncClient(timeout=2.0) as client:
            leader_url = None
            for _ in range(40):
                for p in ports:
                    r = await client.get(f"http://127.0.0.1:{p}/status")
                    if r.json().get("raft", {}).get("role") == "leader":
                        leader_url = f"http://127.0.0.1:{p}"
                        break
                if leader_url:
                    break
                await asyncio.sleep(0.1)
            assert leader_url, "no leader elected"

            r = await client.post(
                f"{leader_url}/lock/acquire",
                json={"resource": "r1", "owner": "tx-1", "mode": "exclusive"},
            )
            assert r.status_code == 200, r.text
            assert r.json()["status"] == "granted"

            r = await client.post(
                f"{leader_url}/lock/acquire",
                json={"resource": "r1", "owner": "tx-2", "mode": "exclusive",
                      "timeout_ms": 300},
            )
            assert r.status_code in (200, 408), r.text
            assert r.json().get("status") in ("waiting", "timeout")

            await client.post(f"{leader_url}/lock/release",
                              json={"resource": "r1", "owner": "tx-1"})
    finally:
        _kill_all(procs)


@pytest.mark.asyncio
async def test_lock_deadlock_detected(tmp_path: Path) -> None:
    base = 9311
    ports = [base, base + 1, base + 2]
    for p in ports:
        if not _port_free(p):
            pytest.skip(f"port {p} in use")
    peers = ",".join(f"dl{i+1}@127.0.0.1:{ports[i]}" for i in range(3))
    procs = [
        _start("lock_manager", f"dl{i+1}", ports[i], peers, tmp_path / f"dl{i+1}")
        for i in range(3)
    ]
    try:
        await _wait_healthy(ports)
        async with httpx.AsyncClient(timeout=3.0) as client:
            leader_url = None
            for _ in range(40):
                for p in ports:
                    r = await client.get(f"http://127.0.0.1:{p}/status")
                    if r.json().get("raft", {}).get("role") == "leader":
                        leader_url = f"http://127.0.0.1:{p}"
                        break
                if leader_url:
                    break
                await asyncio.sleep(0.1)
            assert leader_url

            await client.post(f"{leader_url}/lock/acquire",
                              json={"resource": "x", "owner": "t1", "mode": "exclusive"})
            await client.post(f"{leader_url}/lock/acquire",
                              json={"resource": "y", "owner": "t2", "mode": "exclusive"})

            # t1 wants y (will wait), t2 wants x (creates cycle)
            t1_y = asyncio.create_task(client.post(
                f"{leader_url}/lock/acquire",
                json={"resource": "y", "owner": "t1", "mode": "exclusive",
                      "timeout_ms": 2000},
            ))
            await asyncio.sleep(0.2)
            r2 = await client.post(
                f"{leader_url}/lock/acquire",
                json={"resource": "x", "owner": "t2", "mode": "exclusive",
                      "timeout_ms": 2000},
            )
            r1 = await t1_y
            outcomes = {r1.json().get("status"), r2.json().get("status")}
            # exactly one becomes deadlock victim, the other proceeds (granted)
            assert "deadlock" in outcomes
            assert "granted" in outcomes
    finally:
        _kill_all(procs)


# ---------------------------------------------------------------------------
# Queue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_queue_produce_consume_ack(tmp_path: Path) -> None:
    base = 9321
    ports = [base, base + 1, base + 2]
    for p in ports:
        if not _port_free(p):
            pytest.skip(f"port {p} in use")
    peers = ",".join(f"q{i+1}@127.0.0.1:{ports[i]}" for i in range(3))
    procs = [
        _start("queue_node", f"q{i+1}", ports[i], peers, tmp_path / f"q{i+1}",
               extra_env={"QUEUE_REPLICATION_FACTOR": "2"})
        for i in range(3)
    ]
    try:
        await _wait_healthy(ports)
        async with httpx.AsyncClient(timeout=2.0) as client:
            primary_id: str | None = None
            for i in range(5):
                r = await client.post(
                    f"http://127.0.0.1:{ports[0]}/queue/produce",
                    json={"topic": "events", "payload": {"i": i}},
                )
                assert r.status_code == 200
                body = r.json()
                primary_id = body["leader"]
                assert len(body["replicas"]) == 2
            assert primary_id is not None
            primary_port = ports[int(primary_id[1:]) - 1]

            r = await client.post(
                f"http://127.0.0.1:{primary_port}/queue/consume",
                json={"topic": "events", "consumer_id": "c1", "count": 3,
                      "visibility_ms": 5000},
            )
            msgs = r.json()["messages"]
            assert len(msgs) == 3

            await client.post(
                f"http://127.0.0.1:{primary_port}/queue/ack",
                json={"topic": "events", "consumer_id": "c1", "seq": msgs[0]["seq"]},
            )
            r = await client.get(f"http://127.0.0.1:{primary_port}/queue/state")
            stats = r.json()["topics"]["events"]
            assert stats["consumed"] == 1
            assert stats["inflight"] == 2
    finally:
        _kill_all(procs)


@pytest.mark.asyncio
async def test_queue_visibility_timeout_redelivers(tmp_path: Path) -> None:
    base = 9331
    ports = [base, base + 1, base + 2]
    for p in ports:
        if not _port_free(p):
            pytest.skip(f"port {p} in use")
    peers = ",".join(f"qr{i+1}@127.0.0.1:{ports[i]}" for i in range(3))
    procs = [
        _start("queue_node", f"qr{i+1}", ports[i], peers, tmp_path / f"qr{i+1}")
        for i in range(3)
    ]
    try:
        await _wait_healthy(ports)
        async with httpx.AsyncClient(timeout=2.0) as client:
            await client.post(
                f"http://127.0.0.1:{ports[0]}/queue/produce",
                json={"topic": "events", "payload": "x"},
            )
            r = await client.post(
                f"http://127.0.0.1:{ports[0]}/queue/consume",
                json={"topic": "events", "consumer_id": "lazy", "count": 1,
                      "visibility_ms": 200},
            )
            assert len(r.json()["messages"]) == 1
            await asyncio.sleep(0.5)
            r = await client.post(
                f"http://127.0.0.1:{ports[0]}/queue/consume",
                json={"topic": "events", "consumer_id": "eager", "count": 5},
            )
            assert len(r.json()["messages"]) == 1, "redelivery expected"
    finally:
        _kill_all(procs)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_mesi_invalidation(tmp_path: Path) -> None:
    base = 9341
    ports = [base, base + 1, base + 2]
    for p in ports:
        if not _port_free(p):
            pytest.skip(f"port {p} in use")
    peers = ",".join(f"c{i+1}@127.0.0.1:{ports[i]}" for i in range(3))
    procs = [
        _start("cache_node", f"c{i+1}", ports[i], peers, tmp_path / f"c{i+1}",
               extra_env={"CACHE_CAPACITY": "16"})
        for i in range(3)
    ]
    try:
        await _wait_healthy(ports)
        async with httpx.AsyncClient(timeout=2.0) as client:
            # write on node0 -> M
            r = await client.post(f"http://127.0.0.1:{ports[0]}/cache/set",
                                  json={"key": "k1", "value": "v1"})
            assert r.json()["state"] == "M"

            # read on node1 -> S, node0 downgraded to S
            r = await client.post(f"http://127.0.0.1:{ports[1]}/cache/get",
                                  json={"key": "k1"})
            j = r.json()
            assert j["ok"] is True
            assert j["state"] == "S"
            assert j["source"].startswith("peer:")

            # write on node2 invalidates the others
            await client.post(f"http://127.0.0.1:{ports[2]}/cache/set",
                              json={"key": "k1", "value": "v2"})
            for p in (ports[0], ports[1]):
                r = await client.get(f"http://127.0.0.1:{p}/cache/state")
                assert "k1" not in r.json()["lines"]
    finally:
        _kill_all(procs)
