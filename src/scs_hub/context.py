import datetime as dt
import zoneinfo
import polars as pl
import nba_api.stats.library.parameters as nba_parameters
from nba_api.stats.static import teams

class Context:
    """
    Shared, cheap-to-compute reference data that NBA/fty/statyx components
    commonly need (current season, today's date, the NBA team list).
    Computed once and passed around rather than each component recomputing
    or reaching into a shared god-object.

    active_ids() is the exception: it is a method rather than an attribute
    because it is per-platform and per-season, and because it queries a view
    over the box scores rather than being cheap to compute up front.
    """

    def __init__(self, db_con):

        self._db = db_con
        self._active_ids: dict[tuple[str, str], list[int]] = {}

        # temp_season = "2025-26"
        # temp_prev_season = f"{int(temp_season[:4])-1}-{temp_season[2:4]}"
        # self.cur_season = temp_season
        # self.cur_season_year = int(temp_season[0:4])
        # self.prev_season = temp_prev_season
        # self.prev_season_year = int(temp_prev_season[0:4])

        self.cur_season = nba_parameters.Season.current_season
        self.cur_season_year = int(nba_parameters.Season.current_season[0:4])
        self.prev_season = nba_parameters.Season.previous_season
        self.prev_season_year = int(nba_parameters.Season.previous_season[0:4])
        self.date_est = dt.datetime.now(zoneinfo.ZoneInfo("America/New_York")).date()
        self.timestamp_utc = dt.datetime.now(zoneinfo.ZoneInfo("UTC"))
        self.nba_teams = pl.DataFrame(teams.get_teams())

    def active_ids(self, platform: str, season: str | None = None) -> list[int]:
        """
        Every source id one platform reported for a season — the ids to loop when
        fetching that platform's per-player data.

        This replaces the old `active_players` frame, which read
        util.conformed_player_id.is_active: a stored boolean that no code in this
        repo ever wrote and that no util.update_schedule row refreshed. It had
        drifted badly — 124 players with a 2025-26 roster row were not flagged,
        29 had no crosswalk row at all, and the nba loops saw 523 ids where the
        season actually held 683.

        util.active_player_vw derives the answer instead, from tables the daily
        jobs already refresh, so it cannot go stale. It is also deliberately
        wider than "active": a source id with no player_key yet still comes back,
        so a player who arrives mid-season is fetched on the first run that sees
        them rather than waiting on identity resolution.

        Cached per (platform, season) because the statyx component alone asks
        eleven times per run, and the view scans the box scores.
        """
        season = season or self.cur_season
        key = (platform, season)

        if key not in self._active_ids:
            self._active_ids[key] = (
                self._db.read(
                    "SELECT source_id FROM util.active_player_vw "
                    f"WHERE platform = '{platform}' AND season = '{season}' "
                    "ORDER BY source_id"
                )["source_id"].to_list()
            )

        return self._active_ids[key]
