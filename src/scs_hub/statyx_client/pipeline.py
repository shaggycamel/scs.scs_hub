import configparser
import os

import polars as pl

from scs_hub.config import credentials_path, ensure_credentials_file
from scs_hub.statyx_client._async import run_coro
from scs_hub.statyx_client._client import StatyxClient
from scs_hub.statyx_client.endpoints import BASE_URLS, ENDPOINTS


def _load_api_key(api_key: str | None, ini_path: str | None) -> str:
    """
    Resolution order: explicit api_key arg > STATYX_API_KEY env var >
    credentials.ini (section [statyx], key 'key') at `ini_path` or the
    default ~/.config/scs_hub_credentials.ini. `ini_path` is resolved via
    config.credentials_path, so SCS_HUB_CREDENTIALS can point at a mounted
    file instead (containers/CI).
    Matches the credential pattern already used elsewhere (e.g. dataHub's
    _db_connect), so a project's existing credentials.ini keeps working as-is.
    """
    if api_key:
        return api_key

    if env_key := os.environ.get("STATYX_API_KEY"):
        return env_key

    ini_path = credentials_path(ini_path)
    ensure_credentials_file(ini_path)
    parser = configparser.ConfigParser()
    parser.read(ini_path)
    if not parser.has_section("statyx"):
        raise ValueError(
            f"No Statyx API key found. Pass api_key=..., set STATYX_API_KEY, "
            f"or add a [statyx] section with 'key = ...' to {ini_path}"
        )
    return parser["statyx"]["key"]


class StatyxPipeline:
    """Orchestrates fetch + normalize for downstream use.

    Adding a new endpoint/sport is a data change in endpoints.py — no new
    method needed, unless a response needs custom flattening.
    """

    BASE_URLS = BASE_URLS
    ENDPOINTS = ENDPOINTS

    def __init__(
        self,
        sport: str = "nba",
        api_key: str | None = None,
        ini_path: str | None = None,
        max_concurrent: int = 5,
    ) -> None:
        if sport not in self.BASE_URLS:
            raise ValueError(
                f"Unknown sport '{sport}'. Options: {list(self.BASE_URLS)}"
            )

        self.sport = sport
        self.api_key = _load_api_key(api_key, ini_path)
        self.base_url = self.BASE_URLS[sport]
        self.endpoints = self.ENDPOINTS[sport]
        self.max_concurrent = max_concurrent
        self.errors: dict = {}

    def _url(self, endpoint: str, key: object = None) -> str:
        path = self.endpoints[endpoint]["path"]
        return self.base_url + path.format(key=key)

    async def _fetch_keyed(self, endpoint: str, keys: list, params: dict) -> list:
        spec = self.endpoints[endpoint]
        async with StatyxClient(self.api_key, self.max_concurrent) as client:
            calls = [(key, self._url(endpoint, key), params) for key in keys]
            return await client.get_many(calls, paginated=spec["paginated"])

    async def _fetch_single(self, endpoint: str, params: dict) -> list | dict:
        spec = self.endpoints[endpoint]
        async with StatyxClient(self.api_key, self.max_concurrent) as client:
            fetch = client.get_paginated if spec["paginated"] else client.get_one
            return await fetch(self._url(endpoint), params)

    def run(
        self, endpoint: str, params: dict | None = None, keys: list | None = None
    ) -> pl.DataFrame:
        """
        Sync entry point.

        endpoint: name from this pipeline's endpoints (self.endpoints), e.g. "game_stats" —
                  the available set depends on `sport`, passed at construction time.
        params:   query params, e.g. {"season": 2024}. Some endpoints have required
                  params ("assets" needs sport, nfl "weekly_usage" needs season and
                  week) and some reject "season" entirely ("usage_shock") — see the
                  API docs for each.
        keys:     required if the endpoint is keyed (e.g. player_ids); omit otherwise

        On return, self.errors holds any per-key failures ({key: error_message}).
        """
        if endpoint not in self.endpoints:
            raise ValueError(
                f"Unknown endpoint '{endpoint}'. Options: {list(self.endpoints)}"
            )

        spec = self.endpoints[endpoint]
        params = params or {}
        self.errors = {}
        flatten = spec["flatten"]

        if spec["keyed"]:
            if not keys:
                raise ValueError(f"'{endpoint}' requires `keys` (e.g. player_ids)")

            results = run_coro(self._fetch_keyed(endpoint, keys, params))
            key_column = spec["key_column"]

            rows = []
            for key, data, err in results:
                if err is not None:
                    self.errors[key] = err
                    continue

                # data is a list of rows if paginated, a single dict otherwise
                records = data if spec["paginated"] else [data]
                for row in records:
                    expanded = flatten(row) if flatten else [row]
                    for r in expanded:
                        rows.append({key_column: key, **r})
        else:
            data = run_coro(self._fetch_single(endpoint, params))
            records = data if spec["paginated"] else [data]
            rows = []
            for row in records:
                rows.extend(flatten(row) if flatten else [row])

        return pl.DataFrame(rows) if rows else pl.DataFrame()
