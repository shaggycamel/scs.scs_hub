from __future__ import annotations

from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    from scs_hub.db import Database


class FtyHandler:
    """
    Interface for one (sport, platform) fantasy integration. Each handler
    owns everything specific to its combination — how to authenticate, what
    fields/categories that sport's API exposes, and how season is derived
    from its own connection object (not from a shared Context — see
    connect()). FtyComponent only knows how to look a handler up by
    (sport, platform) and call these methods; it doesn't know any of these
    per-combination details itself.
    """

    def __init__(self, db: Database, schema: str = "fty") -> None:
        self.db = db
        self.schema = schema

    def connect(self, league_id: int, season_year: int, creds: dict[str, str]) -> Any:
        """
        Build and return a live API connection/session for this league.
        Implementations should stamp a `.season` attribute (and anything
        else downstream methods need, e.g. `.game_id` for Yahoo) onto the
        returned object here, once, rather than re-deriving it per call.
        """
        raise NotImplementedError

    def get_league(self, con: Any) -> pl.DataFrame:
        raise NotImplementedError

    def get_league_categories(self, con: Any) -> pl.DataFrame:
        raise NotImplementedError

    def get_free_agents(self, con: Any) -> pl.DataFrame:
        raise NotImplementedError

    def get_league_competitor(self, con: Any) -> pl.DataFrame:
        raise NotImplementedError

    def get_league_matchup(self, con: Any) -> pl.DataFrame:
        raise NotImplementedError

    def get_competitor_roster(self, con: Any) -> pl.DataFrame:
        raise NotImplementedError

    def get_recent_activity(self, con: Any) -> pl.DataFrame:
        raise NotImplementedError

    def get_matchup_box_score(
        self, con: Any, matchup_period: int | None = None
    ) -> pl.DataFrame:
        raise NotImplementedError

    def get_matchup_result(
        self, con: Any, matchup_period: int | None = None
    ) -> pl.DataFrame:
        raise NotImplementedError

    def get_league_byes(self, con: Any) -> pl.DataFrame:
        raise NotImplementedError

    def get_league_matchup_dates(self, con: Any) -> pl.DataFrame:
        raise NotImplementedError
