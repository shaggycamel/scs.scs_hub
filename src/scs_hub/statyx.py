import datetime as dt
import logging

import polars as pl
import polars.selectors as cs
import janitor.polars  # noqa: F401  (registers .clean_names() on pl.DataFrame)
from scs_hub.statyx_client import StatyxPipeline, infer_dtypes

logger = logging.getLogger(__name__)


class StatyxComponent:
    """Statyx API data (advanced stats, shot zones, on/off splits) — writes into the statyx.* schema."""

    def __init__(self, db, ctx, sport: str = "nba"):
        self.db = db
        self.ctx = ctx
        self.pipeline = StatyxPipeline(sport=sport, ini_path=db.ini_path)

    def _incremental(self, table: str, season: str, params: dict) -> dict:
        """
        Build fetch params that pick up only games newer than what's stored for
        this season. These tables are append-only, so refetching a whole season
        would duplicate every row already in it. Scoping the cutoff to the season
        also stops a reload pulling the tail of an earlier one and stamping it
        with the wrong label. Anything in params wins, so an explicit since= or
        season= overrides both.
        """
        # to_regclass is null when the table hasn't been created yet, which is the
        # normal state on a first run — selecting from it would raise instead.
        if self.db.read(f"SELECT to_regclass('statyx.{table}') AS t")[0, 0] is None:
            return dict(params)

        newest = self.db.read(
            f"SELECT MAX(game_date) AS m FROM statyx.{table} WHERE season = '{season}'"
        )[0, 0]

        fetch = {}
        if newest is not None:
            fetch["since"] = str(newest + dt.timedelta(days=1))
        return {**fetch, **params}

    def _replace_season(self, table: str, season: str, extra: str | None = None) -> None:
        """
        Delete one season's rows before a wholesale rewrite.

        These tables are season snapshots — the API returns the whole season on
        every call — so replacing keeps manual re-runs idempotent (and cleans any
        duplicates an older append-only run left behind). `extra` narrows the
        delete for scoped fetches (e.g. a single team's contracts).
        """
        where = f"season = '{season.replace(chr(39), chr(39) * 2)}'"
        if extra:
            where += f" AND {extra}"
        self.db.delete_where(table, "statyx", where)

    def get_schedule(self, season: str | None = None, **params):
        """League schedule via the Statyx API."""
        season = season or self.ctx.cur_season
        logger.info("statyx.schedule")
        df = self.pipeline.run("schedule", params={"season": int(season[:4]), **params})

        if self.pipeline.errors:
            logger.warning("failed: %s", self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.schedule: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
            .with_columns(cs.by_dtype(pl.Datetime("us", "UTC")).dt.replace_time_zone(None) )
        )

        # Schedule rows mutate as the season runs — scores land, games move — so
        # the season is replaced wholesale. Deleting by game_id would drop rows:
        # the source repeats a couple of ids per season (1320 rows, 1318 ids).
        self.db.delete_where("schedule", "statyx", f"season = '{season}'")

        self.db.write_ordered(df, "schedule", schema="statyx")

    def get_player_info(self, season: str | None = None, **params):
        """
        Player biographical directory via the Statyx API, one snapshot per season.

        Always pulls the active roster: the endpoint has no season filter, so a
        player is known to have left when they stop appearing in a later season's
        snapshot. That keeps history immutable — earlier seasons are never
        rewritten to mark someone inactive.
        """
        season = season or self.ctx.cur_season

        logger.info("statyx.player_info")
        df = self.pipeline.run("players", params={"active": "true", **params})

        if self.pipeline.errors:
            logger.warning("failed: %s", self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.player_info: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self.db.delete_where("player_info", "statyx", f"season = '{season}'")

        self.db.write_ordered(df, "player_info", schema="statyx")

    def get_team_roster(self, season: str | None = None, **params):
        """
        Team rosters via the Statyx player directory, as open/closed spells.

        A row opens with an entry_date and closes with an exit_date, so a trade
        lands as two rows: the old spell closed today, the new one opened today.
        Rows that haven't changed are left untouched rather than rewritten.

        On the first run for a season entry_date is left null — everyone is
        already on a roster by then and the real join date isn't knowable.
        """
        season = season or self.ctx.cur_season
        today = self.ctx.date_est
        cols = ["season", "team_id", "team_abbr", "team_name",
                "player_id", "full_name", "position", "jersey_number"]

        logger.info("statyx.team_roster")
        df = self.pipeline.run("players", params={"active": "true", **params})

        if self.pipeline.errors:
            logger.warning("failed: %s", self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.team_roster: API returned no rows — nothing written")
            return

        current = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
            .select(cols)
        )

        empty_spells = pl.DataFrame(
            schema={**current.schema, "entry_date": pl.Date, "exit_date": pl.Date}
        )
        if self.db.read("SELECT to_regclass('statyx.team_roster') AS t")[0, 0] is None:
            open_spells = empty_spells
        else:
            open_spells = self.db.read(
                f"SELECT * FROM statyx.team_roster WHERE season = '{season}' AND exit_date IS NULL",
                schema_overrides={"season": pl.String, "team_id": pl.Int64, "player_id": pl.Int64,
                                  "entry_date": pl.Date, "exit_date": pl.Date},
            )
            if open_spells.is_empty():
                # First run for a season: with no rows, Polars types the
                # un-overridden columns as Null, which cannot join against
                # current's String/Int64 keys. Reuse the explicit schema.
                open_spells = empty_spells

        key = ["season", "team_id", "player_id"]
        arrivals = current.join(open_spells, on=key, how="anti")
        departures = open_spells.join(current, on=key, how="anti")

        if departures.height:
            pairs = ", ".join(f"({r['team_id']}, {r['player_id']})" for r in departures.iter_rows(named=True))
            self.db.delete_where(
                "team_roster", "statyx",
                f"season = '{season}' AND exit_date IS NULL AND (team_id, player_id) IN ({pairs})",
            )
            self.db.write_ordered(
                departures.with_columns(pl.lit(today).cast(pl.Date).alias("exit_date")),
                "team_roster", schema="statyx",
            )

        if arrivals.height:
            entry = None if open_spells.is_empty() else today
            self.db.write_ordered(
                arrivals.with_columns([
                    pl.lit(entry).cast(pl.Date).alias("entry_date"),
                    pl.lit(None).cast(pl.Date).alias("exit_date"),
                ]),
                "team_roster", schema="statyx",
            )

        logger.info("statyx.team_roster: %d arrivals, %d departures", arrivals.height, departures.height)

    def get_contracts(self, season: str | None = None, **params):
        """Player contracts via the Statyx API, replaced per season.

        The season label comes from ctx.cur_season unless one is passed, and is
        also sent to the API. Contract seasons can run ahead of the game season,
        so an off-season call without an explicit season can fetch the old label
        and return no rows; pass a season to target the newer one.
        """
        season = season or self.ctx.cur_season
        logger.info("statyx.contracts")
        params = {"season": int(season[:4]), **params}
        df = self.pipeline.run("contracts", params=params)

        if self.pipeline.errors:
            logger.warning("failed: %s", self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.contracts: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            # Contracts are filed under their own season, which runs ahead of the
            # game season, so the API's value is kept rather than ctx's — just
            # rendered as the "YYYY-YY" label the table already stores.
            .with_columns(
                pl.format(
                    "{}-{}", pl.col("season"), (pl.col("season") + 1).cast(pl.String).str.slice(-2)
                ).alias("season")
            )
            .pipe(infer_dtypes)
        )

        team = params.get("team")
        extra = None
        if team:
            extra = f"team_abbreviation = '{str(team).replace(chr(39), chr(39) * 2)}'"
        self._replace_season("contracts", season, extra)

        self.db.write_ordered(df, "contracts", schema="statyx")

    def get_game_stats(self, season: str | None = None, **params):
        """Player per-game stats via the Statyx API."""
        season = season or self.ctx.cur_season
        ls_pl = self.ctx.active_ids("statyx")

        logger.info("statyx.game_stats")
        df = self.pipeline.run(
            "game_stats",
            params={"season": int(season[:4]), **self._incremental("game_stats", season, params)},
            keys=ls_pl,
        )

        if self.pipeline.errors:
            logger.warning("%d player(s) failed: %s", len(self.pipeline.errors), self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.game_stats: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self.db.write_ordered(df, "game_stats", schema="statyx")

    def get_advanced_stats(self, season: str | None = None, **params):
        """Advanced per-game stats via the Statyx API."""
        season = season or self.ctx.cur_season
        ls_pl = self.ctx.active_ids("statyx")

        logger.info("statyx.advanced_stats")
        df = self.pipeline.run(
            "advanced_stats",
            params={"season": int(season[:4]), **self._incremental("advanced_stats", season, params)},
            keys=ls_pl,
        )

        if self.pipeline.errors:
            logger.warning("%d player(s) failed: %s", len(self.pipeline.errors), self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.advanced_stats: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self.db.write_ordered(df, "advanced_stats", schema="statyx")

    def get_standings(self, season: str | None = None, **params):
        """League standings via the Statyx API."""
        season = season or self.ctx.cur_season
        logger.info("statyx.standings")
        df = self.pipeline.run("standings", params={"season": int(season[:4]), **params})

        if self.pipeline.errors:
            logger.warning("failed: %s", self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.standings: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns([
                pl.lit(season).alias("season"),
                pl.lit(self.ctx.date_est).alias("date")
            ])
            .pipe(infer_dtypes)
        )

        # Standings are a daily snapshot, so history is kept per date — only
        # today's rows are replaced, making a same-day re-run idempotent.
        self.db.delete_where(
            "standings", "statyx", f"season = '{season}' AND date = '{self.ctx.date_est}'"
        )

        self.db.write_ordered(df, "standings", schema="statyx")

    def get_play_types(self, season: str | None = None, **params):
        """Player play types via the Statyx API."""
        season = season or self.ctx.cur_season
        ls_pl = self.ctx.active_ids("statyx")

        logger.info("statyx.play_types")
        df = self.pipeline.run("play_types", params={"season": season, **params}, keys=ls_pl)

        if self.pipeline.errors:
            logger.warning("%d player(s) failed: %s", len(self.pipeline.errors), self.pipeline.errors)
            logger.warning("statyx.play_types: partial fetch — not replacing stored data")
            return

        if df.is_empty():
            logger.warning("statyx.play_types: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .pipe(infer_dtypes)
        )

        self._replace_season("play_types", season)

        self.db.write_ordered(df, "play_types", schema="statyx")

    def get_shot_zones(self, season: str | None = None, **params):
        """Player shot-zones via the Statyx API."""
        season = season or self.ctx.cur_season
        ls_pl = self.ctx.active_ids("statyx")

        logger.info("statyx.shot_zones")
        df = self.pipeline.run("shot_zones", params={"season": int(season[:4]), **params}, keys=ls_pl)

        if self.pipeline.errors:
            logger.warning("%d player(s) failed: %s", len(self.pipeline.errors), self.pipeline.errors)
            logger.warning("statyx.shot_zones: partial fetch — not replacing stored data")
            return

        if df.is_empty():
            logger.warning("statyx.shot_zones: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self._replace_season("shot_zones", season)

        self.db.write_ordered(df, "shot_zones", schema="statyx")

    def get_potential_assists(self, season: str | None = None, **params):
        """Player potential assists via the Statyx API."""
        season = season or self.ctx.cur_season
        ls_pl = self.ctx.active_ids("statyx")

        logger.info("statyx.potential_assists")
        df = self.pipeline.run(
            "potential_assists",
            params={"season": season, **self._incremental("potential_assists", season, params)},
            keys=ls_pl,
        )

        if self.pipeline.errors:
            logger.warning("%d player(s) failed: %s", len(self.pipeline.errors), self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.potential_assists: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self.db.write_ordered(df, "potential_assists", schema="statyx")

    def get_shooting_splits(self, season: str | None = None, **params):
        """Player shooting splits via the Statyx API."""
        season = season or self.ctx.cur_season
        ls_pl = self.ctx.active_ids("statyx")

        logger.info("statyx.shooting_splits")
        df = self.pipeline.run("shooting_splits", params={"season": int(season[:4]), **params}, keys=ls_pl)

        if self.pipeline.errors:
            logger.warning("%d player(s) failed: %s", len(self.pipeline.errors), self.pipeline.errors)
            logger.warning("statyx.shooting_splits: partial fetch — not replacing stored data")
            return

        if df.is_empty():
            logger.warning("statyx.shooting_splits: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self._replace_season("shooting_splits", season)

        self.db.write_ordered(df, "shooting_splits", schema="statyx")

    def get_drives(self, season: str | None = None, **params):
        """Player drives via the Statyx API."""
        season = season or self.ctx.cur_season
        ls_pl = self.ctx.active_ids("statyx")

        logger.info("statyx.drives")
        df = self.pipeline.run("drives", params={"season": int(season[:4]), **params}, keys=ls_pl)

        if self.pipeline.errors:
            logger.warning("%d player(s) failed: %s", len(self.pipeline.errors), self.pipeline.errors)
            logger.warning("statyx.drives: partial fetch — not replacing stored data")
            return

        if df.is_empty():
            logger.warning("statyx.drives: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self._replace_season("drives", season)

        self.db.write_ordered(df, "drives", schema="statyx")

    def get_scoring_breakdown(self, season: str | None = None, **params):
        """Player scoring breakdown via the Statyx API."""
        season = season or self.ctx.cur_season
        ls_pl = self.ctx.active_ids("statyx")

        logger.info("statyx.scoring_breakdown")
        df = self.pipeline.run("scoring_breakdown", params={"season": int(season[:4]), **params}, keys=ls_pl)

        if self.pipeline.errors:
            logger.warning("%d player(s) failed: %s", len(self.pipeline.errors), self.pipeline.errors)
            logger.warning("statyx.scoring_breakdown: partial fetch — not replacing stored data")
            return

        if df.is_empty():
            logger.warning("statyx.scoring_breakdown: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self._replace_season("scoring_breakdown", season)

        self.db.write_ordered(df, "scoring_breakdown", schema="statyx")

    def get_assist_profile(self, season: str | None = None, **params):
        """Player assist profile via the Statyx API."""
        season = season or self.ctx.cur_season
        ls_pl = self.ctx.active_ids("statyx")

        logger.info("statyx.assist_profile")
        df = self.pipeline.run("assist_profile", params={"season": int(season[:4]), **params}, keys=ls_pl)

        if self.pipeline.errors:
            logger.warning("%d player(s) failed: %s", len(self.pipeline.errors), self.pipeline.errors)
            logger.warning("statyx.assist_profile: partial fetch — not replacing stored data")
            return

        if df.is_empty():
            logger.warning("statyx.assist_profile: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self._replace_season("assist_profile", season)

        self.db.write_ordered(df, "assist_profile", schema="statyx")

    def get_matchup_history(self, season: str | None = None, **params):
        """Player matchup history via the Statyx API."""
        season = season or self.ctx.cur_season
        ls_pl = self.ctx.active_ids("statyx")

        logger.info("statyx.matchup_history")
        df = self.pipeline.run("matchup_history", params={"season": season, **params}, keys=ls_pl)

        if self.pipeline.errors:
            logger.warning("%d player(s) failed: %s", len(self.pipeline.errors), self.pipeline.errors)
            logger.warning("statyx.matchup_history: partial fetch — not replacing stored data")
            return

        if df.is_empty():
            logger.warning("statyx.matchup_history: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self._replace_season("matchup_history", season)

        self.db.write_ordered(df, "matchup_history", schema="statyx")

    def get_defense_vs_position(self, season: str | None = None, **params):
        """Defense vs position via the Statyx API."""
        season = season or self.ctx.cur_season
        logger.info("statyx.defense_vs_position")
        df = self.pipeline.run("defense_vs_position", params={"season": int(season[:4]), **params})

        if self.pipeline.errors:
            logger.warning("failed: %s", self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.defense_vs_position: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self._replace_season("defense_vs_position", season)

        self.db.write_ordered(df, "defense_vs_position", schema="statyx")

    def get_play_type_defense(self, season: str | None = None, **params):
        """Play type defense via the Statyx API."""
        season = season or self.ctx.cur_season
        logger.info("statyx.play_type_defense")
        df = self.pipeline.run("play_type_defense", params={"season": int(season[:4]), **params})

        if self.pipeline.errors:
            logger.warning("failed: %s", self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.play_type_defense: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self._replace_season("play_type_defense", season)

        self.db.write_ordered(df, "play_type_defense", schema="statyx")

    def get_shot_zone_defense(self, season: str | None = None, **params):
        """Shot zone defense via the Statyx API."""
        season = season or self.ctx.cur_season
        logger.info("statyx.shot_zone_defense")
        df = self.pipeline.run("shot_zone_defense", params={"season": int(season[:4]), **params})

        if self.pipeline.errors:
            logger.warning("failed: %s", self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.shot_zone_defense: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self._replace_season("shot_zone_defense", season)

        self.db.write_ordered(df, "shot_zone_defense", schema="statyx")

    def get_usage_shock(self, season: str | None = None, **params):
        """Usage shock via the Statyx API."""
        season = season or self.ctx.cur_season
        # /usage-shock takes no season param — it grades a rolling window the
        # API picks itself (window_start/window_end come back in the response).
        logger.info("statyx.usage_shock")
        df = self.pipeline.run("usage_shock", params=params)

        if self.pipeline.errors:
            logger.warning("failed: %s", self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.usage_shock: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        windows = df.select("window_start", "window_end").unique()
        if windows.null_count().sum_horizontal().item(0) > 0:
            logger.warning("statyx.usage_shock: null window in response — not replacing stored data")
            return
        pairs = ", ".join(
            f"('{r['window_start'].isoformat()}', '{r['window_end'].isoformat()}')"
            for r in windows.iter_rows(named=True)
        )
        self.db.delete_where(
            "usage_shock", "statyx",
            f"season = '{season.replace(chr(39), chr(39) * 2)}' "
            f"AND (window_start, window_end) IN ({pairs})",
        )

        self.db.write_ordered(df, "usage_shock", schema="statyx")

    def get_team_assist_defense(self, season: str | None = None, **params):
        """Team assist defense via the Statyx API."""
        season = season or self.ctx.cur_season
        logger.info("statyx.team_assist_defense")
        df = self.pipeline.run("team_assist_defense", params={"season": int(season[:4]), **params})

        if self.pipeline.errors:
            logger.warning("failed: %s", self.pipeline.errors)

        if df.is_empty():
            logger.warning("statyx.team_assist_defense: API returned no rows — nothing written")
            return

        df = (
            df.clean_names(case_type="snake")
            .with_columns(pl.lit(season).alias("season"))
            .pipe(infer_dtypes)
        )

        self._replace_season("team_assist_defense", season)

        self.db.write_ordered(df, "team_assist_defense", schema="statyx")