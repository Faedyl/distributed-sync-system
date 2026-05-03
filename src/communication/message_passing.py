"""Async HTTP/JSON inter-node transport.

Inter-node RPC is JSON over HTTP. Simple, debuggable, easy to compose with the
existing aiohttp servers each node already runs for client APIs.
"""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp


class HttpClient:
    def __init__(self, timeout_ms: int = 800) -> None:
        self._timeout = aiohttp.ClientTimeout(
            total=timeout_ms / 1000.0,
            connect=0.3,
        )
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            async with self._lock:
                if self._session is None or self._session.closed:
                    self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def post_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        sess = await self.session()
        async with sess.post(url, json=payload) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_json(self, url: str) -> dict[str, Any]:
        sess = await self.session()
        async with sess.get(url) as resp:
            resp.raise_for_status()
            return await resp.json()
