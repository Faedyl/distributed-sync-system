"""Heartbeat-based failure detector. Threshold-driven liveness."""

from __future__ import annotations

import time
from threading import Lock


class FailureDetector:
    def __init__(self, timeout_ms: int = 1500) -> None:
        self._timeout_s = timeout_ms / 1000.0
        self._last_beat: dict[str, float] = {}
        self._lock = Lock()

    def heartbeat(self, node_id: str) -> None:
        with self._lock:
            self._last_beat[node_id] = time.monotonic()

    def is_alive(self, node_id: str) -> bool:
        with self._lock:
            last = self._last_beat.get(node_id)
        return last is not None and (time.monotonic() - last) < self._timeout_s

    def alive_nodes(self) -> list[str]:
        now = time.monotonic()
        with self._lock:
            return [n for n, t in self._last_beat.items() if (now - t) < self._timeout_s]
