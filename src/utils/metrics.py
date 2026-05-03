"""In-process metrics with Prometheus-compatible output.

Counters and timing histograms; rendered via /metrics endpoint or JSON snapshot.
"""

from __future__ import annotations

import bisect
import time
from collections import defaultdict
from threading import Lock
from typing import Any


class Histogram:
    __slots__ = ("samples_ms",)

    def __init__(self) -> None:
        self.samples_ms: list[float] = []

    def observe(self, ms: float) -> None:
        self.samples_ms.append(ms)
        if len(self.samples_ms) > 100_000:
            self.samples_ms = self.samples_ms[-50_000:]

    def snapshot(self) -> dict[str, Any]:
        if not self.samples_ms:
            return {"count": 0, "sum_ms": 0, "p50_ms": 0, "p95_ms": 0, "p99_ms": 0, "max_ms": 0}
        s = sorted(self.samples_ms)
        n = len(s)

        def pct(p: float) -> float:
            return s[max(0, min(n - 1, int(round((n - 1) * p))))]

        return {
            "count": n,
            "sum_ms": round(sum(s), 3),
            "p50_ms": round(pct(0.5), 3),
            "p95_ms": round(pct(0.95), 3),
            "p99_ms": round(pct(0.99), 3),
            "max_ms": round(s[-1], 3),
        }


class Metrics:
    def __init__(self) -> None:
        self._lock = Lock()
        self._counters: dict[str, int] = defaultdict(int)
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, Histogram] = defaultdict(Histogram)

    def incr(self, name: str, by: int = 1) -> None:
        with self._lock:
            self._counters[name] += by

    def gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def observe_ms(self, name: str, ms: float) -> None:
        with self._lock:
            self._histograms[name].observe(ms)

    def timer(self, name: str) -> "_Timer":
        return _Timer(self, name)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "histograms": {k: v.snapshot() for k, v in self._histograms.items()},
            }

    def render_prometheus(self) -> str:
        lines: list[str] = []
        with self._lock:
            for name, value in self._counters.items():
                safe = _sanitize(name)
                lines.append(f"# TYPE {safe} counter")
                lines.append(f"{safe} {value}")
            for name, value in self._gauges.items():
                safe = _sanitize(name)
                lines.append(f"# TYPE {safe} gauge")
                lines.append(f"{safe} {value}")
            for name, hist in self._histograms.items():
                safe = _sanitize(name)
                snap = hist.snapshot()
                lines.append(f"# TYPE {safe} summary")
                lines.append(f"{safe}_count {snap['count']}")
                lines.append(f"{safe}_sum {snap['sum_ms']}")
                for q, key in (("0.5", "p50_ms"), ("0.95", "p95_ms"), ("0.99", "p99_ms")):
                    lines.append(f'{safe}{{quantile="{q}"}} {snap[key]}')
        return "\n".join(lines) + "\n"


def _sanitize(name: str) -> str:
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)


class _Timer:
    __slots__ = ("metrics", "name", "start")

    def __init__(self, metrics: Metrics, name: str) -> None:
        self.metrics = metrics
        self.name = name
        self.start = time.perf_counter()

    def __enter__(self) -> "_Timer":
        self.start = time.perf_counter()
        return self

    def __exit__(self, *exc: object) -> None:
        elapsed_ms = (time.perf_counter() - self.start) * 1000.0
        self.metrics.observe_ms(self.name, elapsed_ms)
