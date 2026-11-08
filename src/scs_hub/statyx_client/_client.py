import asyncio
from types import TracebackType
from typing import Self

import aiohttp


class StatyxClient:
    """Thin transport layer: pagination + concurrency. No knowledge of specific endpoints."""

    def __init__(
        self, api_key: str, max_concurrent: int = 5, max_retries: int = 4
    ) -> None:
        self.api_key = api_key
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> Self:
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        self.session = aiohttp.ClientSession(
            connector=connector, headers={"x-api-key": self.api_key}
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.session.close()

    async def _get(self, url: str, params: dict) -> list | dict:
        """
        One GET, retrying on 429. Keyed endpoints fan out a request per player,
        which overruns the account's rate limit (800 per window) long before the
        concurrency cap binds — capping simultaneous connections throttles the
        rate only indirectly, so the retry is what actually makes a 429 survivable.
        Honours Retry-After when the server sends one, else backs off 1s, 2s, 4s...
        """
        for attempt in range(self.max_retries + 1):
            async with self.session.get(url, params=params) as r:
                if r.status == 429 and attempt < self.max_retries:
                    await asyncio.sleep(float(r.headers.get("Retry-After", 2**attempt)))
                    continue
                r.raise_for_status()
                return (await r.json())["data"]

    async def get_paginated(self, url: str, params: dict) -> list:
        """Fetch every page of one list endpoint call."""
        rows = []
        offset = 0
        while True:
            data = await self._get(url, {**params, "limit": 200, "offset": offset})
            rows.extend(data)
            if len(data) < 200:  # last page
                break
            offset += 200
        return rows

    async def get_one(self, url: str, params: dict) -> dict:
        """Fetch a single-object endpoint (no list, no pagination)."""
        return await self._get(url, params)

    async def get_many(self, calls: list[tuple], paginated: bool = True) -> list:
        """
        Run get_paginated (or get_one, if paginated=False) concurrently across many calls.
        calls: list of (key, url, params). `key` is just a label attached to the
        result so callers can match it back up — pass None if there's no key.
        """
        fetch = self.get_paginated if paginated else self.get_one

        async def fetch_one(
            key: object, url: str, params: dict
        ) -> tuple[object, list | dict | None, str | None]:
            try:
                result = await fetch(url, params)
                return key, result, None
            except aiohttp.ClientError as e:
                return key, None, str(e)

        tasks = [fetch_one(*call) for call in calls]
        return await asyncio.gather(*tasks)
