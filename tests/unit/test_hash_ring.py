"""Unit tests for the consistent hash ring used by the queue."""

from __future__ import annotations

from src.nodes.queue_node import HashRing


def test_ring_is_stable() -> None:
    ring = HashRing(virtual_nodes=64)
    for n in ["a", "b", "c"]:
        ring.add(n)
    assignments_first = {f"key-{i}": ring.primary_for(f"key-{i}") for i in range(200)}
    # rebuild
    ring2 = HashRing(virtual_nodes=64)
    for n in ["a", "b", "c"]:
        ring2.add(n)
    assignments_second = {f"key-{i}": ring2.primary_for(f"key-{i}") for i in range(200)}
    assert assignments_first == assignments_second


def test_ring_remove_only_remaps_subset() -> None:
    ring = HashRing(virtual_nodes=64)
    for n in ["a", "b", "c", "d"]:
        ring.add(n)
    keys = [f"key-{i}" for i in range(500)]
    before = {k: ring.primary_for(k) for k in keys}
    ring.remove("b")
    after = {k: ring.primary_for(k) for k in keys}
    moved = sum(1 for k in keys if before[k] != after[k])
    # only keys previously owned by 'b' should move; expect ~25%
    moved_pct = moved / len(keys)
    assert 0.10 < moved_pct < 0.40, f"moved {moved_pct:.2%}, expected ~25%"


def test_successors_are_distinct_physical_nodes() -> None:
    ring = HashRing(virtual_nodes=128)
    for n in ["a", "b", "c"]:
        ring.add(n)
    succs = ring.successors("orders", 3)
    assert len(succs) == 3
    assert len(set(succs)) == 3
