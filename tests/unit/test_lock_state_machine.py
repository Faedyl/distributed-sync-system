"""Unit tests for the LockStateMachine (no Raft, no HTTP)."""

from __future__ import annotations

import asyncio

import pytest

from src.consensus.raft import LogEntry
from src.nodes.lock_manager import EXCLUSIVE, SHARED, LockStateMachine
from src.utils.metrics import Metrics


def _entry(op: str, **kwargs) -> LogEntry:
    return LogEntry(term=1, index=1, command={"op": op, **kwargs})


@pytest.mark.asyncio
async def test_acquire_exclusive_then_shared_waits() -> None:
    sm = LockStateMachine(Metrics())
    r1 = await sm.apply(_entry("acquire", resource="r", owner="A",
                                mode=EXCLUSIVE, request_id="rid1"))
    assert r1["status"] == "granted"
    r2 = await sm.apply(_entry("acquire", resource="r", owner="B",
                                mode=SHARED, request_id="rid2"))
    assert r2["status"] == "waiting"


@pytest.mark.asyncio
async def test_release_grants_waiter() -> None:
    sm = LockStateMachine(Metrics())
    fut = sm.register_pending("rid_b")
    await sm.apply(_entry("acquire", resource="r", owner="A",
                          mode=EXCLUSIVE, request_id="rid_a"))
    await sm.apply(_entry("acquire", resource="r", owner="B",
                          mode=SHARED, request_id="rid_b"))
    await sm.apply(_entry("release", resource="r", owner="A"))
    outcome = await asyncio.wait_for(fut, timeout=1.0)
    assert outcome == "granted"


@pytest.mark.asyncio
async def test_two_shared_compatible() -> None:
    sm = LockStateMachine(Metrics())
    r1 = await sm.apply(_entry("acquire", resource="r", owner="A",
                                mode=SHARED, request_id="a"))
    r2 = await sm.apply(_entry("acquire", resource="r", owner="B",
                                mode=SHARED, request_id="b"))
    assert r1["status"] == "granted"
    assert r2["status"] == "granted"


@pytest.mark.asyncio
async def test_deadlock_detection_picks_youngest_victim() -> None:
    sm = LockStateMachine(Metrics())
    # T1 holds X, T2 holds Y
    await sm.apply(_entry("acquire", resource="X", owner="T1",
                          mode=EXCLUSIVE, request_id="t1x"))
    await sm.apply(_entry("acquire", resource="Y", owner="T2",
                          mode=EXCLUSIVE, request_id="t2y"))
    # T1 wants Y -> waits on T2
    r1 = await sm.apply(_entry("acquire", resource="Y", owner="T1",
                                mode=EXCLUSIVE, request_id="t1y"))
    assert r1["status"] == "waiting"
    # T2 wants X -> cycle. T2 is the younger (more recent) waiter.
    r2 = await sm.apply(_entry("acquire", resource="X", owner="T2",
                                mode=EXCLUSIVE, request_id="t2x"))
    assert r2["status"] == "deadlock"
    assert r2["victim"] == "T2"
