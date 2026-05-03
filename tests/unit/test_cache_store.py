"""Unit tests for the in-process CacheStore (LRU, MESI bookkeeping)."""

from __future__ import annotations

import os
from pathlib import Path

from src.nodes.cache_node import CacheStore, M, E, S


def test_lru_evicts_least_recent(tmp_path: Path) -> None:
    store = CacheStore(capacity=3, policy="LRU",
                       memory_path=str(tmp_path / "memory.json"))
    for k in ["a", "b", "c"]:
        store.put(k, k.upper(), E)
    store.get("a")  # touch a
    evicted = store.put("d", "D", E)
    assert evicted == "b"  # b is now LRU


def test_modified_line_writes_back_on_eviction(tmp_path: Path) -> None:
    mem = tmp_path / "memory.json"
    store = CacheStore(capacity=2, policy="LRU", memory_path=str(mem))
    store.put("k1", "v1", M)
    store.put("k2", "v2", E)
    evicted = store.put("k3", "v3", E)
    assert evicted == "k1"
    # value should be written back to memory
    assert store.memory_get("k1") == "v1"


def test_lfu_picks_least_frequent(tmp_path: Path) -> None:
    store = CacheStore(capacity=3, policy="LFU",
                       memory_path=str(tmp_path / "memory.json"))
    store.put("a", 1, E)
    store.put("b", 2, E)
    store.put("c", 3, E)
    # touch a many times
    for _ in range(5):
        store.get("a")
    store.get("b")
    # c has access_count == 1 (lowest) — should be evicted
    evicted = store.put("d", 4, E)
    assert evicted == "c"
