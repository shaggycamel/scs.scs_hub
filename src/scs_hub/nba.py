import time
import zoneinfo
import datetime as dt
import logging
import requests
import dateutil.parser
import polars as pl
import polars.selectors as cs
import janitor.polars  # noqa: F401  (registers .clean_names() on pl.DataFrame)
import nbainjuries
import nba_api.stats.endpoints as nba_ep

logger = logging.getLogger(__name__)

_SCHEDULE_URL = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"

# Columns _upcoming_games produces end to end. Returned with zero rows when the
# CDN is unusable, so the concat and conform in get_game_schedule still see a
# frame of the right shape.
_UPCOMING_GAMES_SCHEMA = {
    "season_type": pl.String,
    "game_id": pl.Int64,
    "game_date": pl.Date,
    "matchup": pl.String,
    "season": pl.String,
    "team_winner": pl.String,
    "team_loser": pl.String,
    "team": pl.String,
    "opponent": pl.String,
    "home": pl.Boolean,
}


class NBAComponent:
    """Official NBA stats — writes into the nba.* schema."""

    def __init__(self, db, ctx):
        self.db = db
        self.ctx = ctx

    def get_player_season_stats(self):
        """Season stats (totals)"""
        ls_pl = self.ctx.active_ids("nba")

        logger.info("nba.player_season_stats")
        dfs = []
        failed = []
        empty = []
        for ix, player in enumerate(ls_pl):
            # Two distinct outcomes to survive, and continue rather than the
            # break get_player_box_score uses, because neither is exceptional
            # here: util.player_directory_vw errs toward including a questionable
            # nba id (see its comment on the guard), so a handful of
            # pre-season-only non-NBA participants reach this loop each season.
            # Breaking on the first would mean one such id costs the whole fetch.
            try:
                player_season = nba_ep.playercareerstats.PlayerCareerStats(player_id=str(player))
                frame = pl.from_pandas(player_season.data_sets[0].get_data_frame())
            except Exception:
                logger.exception("player_id %s failed", player)
                failed.append(player)
                time.sleep(1)
                continue

            # An id with no career rows comes back as a 0-row frame rather than
            # as an error, and pandas infers every column of it as object, so
            # PLAYER_ID arrives as String where a real player's is Int64. Letting
            # that reach pl.concat raises SchemaError and loses the entire run —
            # which is what actually happens for a non-NBA id, not an exception.
            if frame.is_empty():
                empty.append(player)
            else:
                dfs.append(frame)
            if ix % 50 == 0:
                logger.debug("player: %d / %d", ix, len(ls_pl))
            time.sleep(1)
        logger.debug("player: %d / %d", len(ls_pl), len(ls_pl))

        if empty:
            logger.info(
                "player_season_stats: %d id(s) returned no rows (not NBA players): %s",
                len(empty), empty,
            )

        if failed:
            logger.warning(
                "player_season_stats: %d player(s) failed: %s", len(failed), failed
            )

        if not dfs:
            logger.error("player_season_stats: nothing fetched, skipping write")
            return

        df = (
            pl.concat(dfs)
            .clean_names(case_type="snake")
            .rename({"season_id": "season"})
            .with_columns(pl.lit(self.ctx.cur_season).alias("season"))
        )

        self.db.write_ordered(df, "player_season_stats", schema="nba")

    def get_player_info(self):
        ls_pl = self.ctx.active_ids("nba")

        logger.info("nba.player_info")
        dfs = []
        failed = []
        empty = []
        for ix, player in enumerate(ls_pl):
            # Same two outcomes as get_player_season_stats; see its comments.
            try:
                player_info = nba_ep.commonplayerinfo.CommonPlayerInfo(player_id=str(player))
                frame = pl.from_pandas(player_info.data_sets[0].get_data_frame())
            except Exception:
                logger.exception("player_id %s failed", player)
                failed.append(player)
                time.sleep(1)
                continue

            if frame.is_empty():
                empty.append(player)
            else:
                dfs.append(frame)
            if ix % 50 == 0:
                logger.debug("player: %d / %d", ix, len(ls_pl))
            time.sleep(1)
        logger.debug("player: %d / %d", len(ls_pl), len(ls_pl))

        if empty:
            logger.info(
                "player_info: %d id(s) returned no rows (not NBA players): %s",
                len(empty), empty,
            )

        if failed:
            logger.warning("player_info: %d player(s) failed: %s", len(failed), failed)

        if not dfs:
            logger.error("player_info: nothing fetched, skipping write")
            return

        # diagonal_relaxed because CommonPlayerInfo's column set is not stable
        # across players: it omits SUPPLEMENTAL_STATUS for some ids, so frames
        # arrive 33 or 34 wide and a plain concat raises ShapeError. Missing
        # columns become null, and write_ordered conforms to the registration
        # afterwards regardless. get_player_season_stats keeps a strict concat —
        # its frames are a consistent 27 wide and its empty-frame guard is what
        # that loop needs.
        df = (
            pl.concat(dfs, how="diagonal_relaxed")
            .clean_names(case_type="snake")
            .with_columns(pl.col("height").str.split("-"))
            .with_columns(
                [
                    pl.lit(self.ctx.cur_season).alias("season"),
                    (pl.col("weight").cast(pl.Float64, strict=False) / 2.2046)
                    .round(3)
                    .alias("weight_kg"),
                    # strict=False and null_on_oob to match how weight above is
                    # already handled. A player with no listed height comes back
                    # with height "", which splits to [""] — one element, not two,
                    # and not castable — so a strict cast raises and loses the
                    # whole run rather than nulling one derived value. That is
                    # reachable for the non-NBA ids the directory guard lets
                    # through, and for any genuine player with the field blank.
                    (
                        (
                            pl.col("height").list.get(0, null_on_oob=True)
                            .cast(pl.Float64, strict=False) * 12
                            + pl.col("height").list.get(1, null_on_oob=True)
                            .cast(pl.Float64, strict=False)
                        )
                        * 2.54
                    )
                    .round(2)
                    .alias("height_cm"),
                ]
            )
            .rename({"person_id": "player_id"})
        )

        self.db.write_ordered(df, "player_info", schema="nba")
        

    def get_team_injuries(self, force_date=None):
        dt_est = self.ctx.date_est if force_date is None else force_date

        teams_true = self.db.read(
            "SELECT CONCAT(team_long, ' ', team_name) AS team, team_slug FROM nba.teams",
        )

        game_ids = self.db.read(
            f"SELECT game_date, game_id, matchup FROM nba.league_game_schedule WHERE game_date BETWEEN '{dt_est}' AND '{dt_est + dt.timedelta(days=2)}' AND matchup LIKE '%@%'",
        )

        player_ids = self.db.read(
            "SELECT source_name AS nba_name, source_id AS nba_id "
            "FROM util.player_source_id WHERE platform = 'nba'"
        ).with_columns(
            pl.col("nba_name").str.normalize("NFKD").str.replace_all("[^\\x00-\\x7F]", "")
        )

        dt_est = dt.datetime.combine(
            dt_est, dt.time(0, 0), tzinfo=zoneinfo.ZoneInfo("America/New_York")
        )
        times = [dt_est + dt.timedelta(minutes=(60 / 4) * i) for i in range(24 * 4)]
        times = [tm.replace(tzinfo=None) for tm in times]
        times.reverse()

        def get_valid_time():
            for tm in times:
                try:
                    nbainjuries._parser.validate_injrepurl(nbainjuries.injury.gen_url(tm))
                    return tm
                except Exception:
                    continue
            return None

        # No valid report URL means either the slate is dark or nbainjuries' URL
        # scheme has moved; either way there is nothing to write, and passing the
        # None through raised TypeError inside get_reportdata and failed the run.
        valid_time = get_valid_time()
        if valid_time is None:
            logger.warning(
                "nba.injuries: no injury report available for %s, skipping",
                dt_est.date(),
            )
            return

        df = (
            pl.from_pandas(nbainjuries.injury.get_reportdata(valid_time, return_df=True))
            .clean_names(case_type="snake")
            .filter(
                (pl.col("reason") != "NOT YET SUBMITTED") & (pl.col("player_name").is_not_null())
            )
            .with_columns(pl.col("player_name").str.split(", "))
            .with_columns(
                [
                    pl.col("game_date").str.to_date(format="%m/%d/%Y"),
                    pl.col("matchup").str.replace_all("@", " @ "),
                    (
                        pl.col("player_name").list.get(1) + " " + pl.col("player_name").list.get(0)
                    ).alias("player_name"),
                ]
            )
            .join(teams_true, how="left", on="team")
            .join(player_ids, how="left", left_on="player_name", right_on="nba_name")
            .join(game_ids, how="left", on=["game_date", "matchup"])
            .rename({"current_status": "status"})
        )

        for row in df.iter_rows(named=True):
            game_date = row["game_date"]
            game_id = row["game_id"]
            player_name = row["player_name"].replace("'", "''")
            self.db.execute(
                f"DELETE FROM nba.injuries WHERE game_date = '{game_date}' AND game_id = {game_id} AND player_name = '{player_name}'",

            )

        self.db.write_ordered(df, "injuries", schema="nba")

    def get_player_box_score(self):
        bs_max_dt = (
            self.db.read(
                "select max(game_date) from nba.player_box_score_vw where min is not null",

            )
            .item()
            .strftime("%Y-%m-%d")
        )
        game_ids = self.db.read(
            f"SELECT DISTINCT game_id FROM nba.league_game_schedule WHERE game_date > '{bs_max_dt}' AND game_date <= '{self.ctx.date_est}'",
        )
        cols_trad = self.db.read(
            "select * from util.table_column_order where schema = 'nba' and table_name = 'player_box_score_traditional' order by column_order",
        )
        cols_adv = self.db.read(
            "select * from util.table_column_order where schema = 'nba' and table_name = 'player_box_score_advanced' order by column_order",
        )

        logger.info("nba.player_box_score")
        dfs = []
        failed = []
        g_ids = game_ids.get_column("game_id").to_list()
        for game_id in g_ids:
            game_id = "00" + str(int(game_id))
            logger.debug("processing game_id %s", game_id)

            # Traditional stats
            try:
                bst = (
                    pl.from_pandas(
                        nba_ep.boxscoretraditionalv3.BoxScoreTraditionalV3(
                            game_id=game_id
                        ).get_data_frames()[0]
                    )
                    .rename(
                        dict(
                            zip(
                                cols_trad.drop_nulls()["origin_name"],
                                cols_trad.drop_nulls()["column_name"],
                            )
                        )
                    )
                    .with_columns(cs.string().replace("", None))
                    .with_columns(pl.col("game_id").cast(pl.Int64))
                    .with_columns(
                        pl.col("min").str.replace(r":.*", "").cast(pl.Int64, strict=False)
                    )
                    .with_columns(
                        (pl.col("firstName") + " " + pl.col("familyName")).alias("player_name")
                    )
                    .select(cols_trad["column_name"].to_list())
                )
            except Exception:
                logger.exception("game_id %s (traditional stats) failed", game_id)
                failed.append(game_id)
                break

            # Advanced stats
            try:
                bsa = (
                    pl.from_pandas(
                        nba_ep.boxscoreadvancedv3.BoxScoreAdvancedV3(
                            game_id=game_id
                        ).get_data_frames()[0]
                    )
                    .rename(
                        dict(
                            zip(
                                cols_adv.drop_nulls()["origin_name"],
                                cols_adv.drop_nulls()["column_name"],
                            )
                        )
                    )
                    .with_columns(cs.string().replace("", None))
                    .with_columns(pl.col("game_id").cast(pl.Int64))
                    .select(cols_adv["column_name"].to_list())
                )
            except Exception:
                logger.exception("game_id %s (advanced stats) failed", game_id)
                failed.append(game_id)
                break

            dfs.append(bst.join(bsa, on=["game_id", "player_id"], how="left"))

        if failed:
            logger.warning("player_box_score: %d game(s) failed: %s", len(failed), failed)

        if not dfs:
            logger.error("player_box_score: nothing fetched, skipping write")
            return

        df = pl.concat(dfs)
        self.db.write(df, "player_box_score", schema="nba")
        logger.info("nba.player_box_score have been updated (%d rows)", len(df))

    def get_team_box_score(self):
        bs_max_dt = (
            self.db.read(
                "select max(game_date) from nba.team_box_score_vw where min is not null",

            )
            .item()
            .strftime("%Y-%m-%d")
        )
        game_ids = self.db.read(
            f"SELECT DISTINCT game_id FROM nba.league_game_schedule WHERE game_date > '{bs_max_dt}' AND game_date <= '{self.ctx.date_est}'",
        )
        cols_trad = self.db.read(
            "select * from util.table_column_order where schema = 'nba' and table_name = 'team_box_score_traditional' order by column_order",
        )
        cols_adv = self.db.read(
            "select * from util.table_column_order where schema = 'nba' and table_name = 'team_box_score_advanced' order by column_order",
        )

        logger.info("nba.team_box_score")
        dfs = []
        failed = []
        g_ids = game_ids.get_column("game_id").to_list()
        for game_id in g_ids:
            game_id = "00" + str(int(game_id))
            logger.debug("processing game_id %s", game_id)

            # Traditional stats
            try:
                bst = (
                    pl.from_pandas(
                        nba_ep.boxscoretraditionalv3.BoxScoreTraditionalV3(
                            game_id=game_id
                        ).get_data_frames()[1]
                    )
                    .rename(
                        dict(
                            zip(
                                cols_trad.drop_nulls()["origin_name"],
                                cols_trad.drop_nulls()["column_name"],
                            )
                        )
                    )
                    .with_columns(cs.string().replace("", None))
                    .with_columns(pl.col("game_id").cast(pl.Int64))
                    .with_columns(
                        pl.col("min").str.replace(r":.*", "").cast(pl.Int64, strict=False)
                    )
                    .group_by(["game_id", "team_id", "team_abbreviation"])
                    .agg(cs.numeric().sum())
                    .with_columns(
                        [
                            (pl.col("fgm") / pl.col("fga")).alias("fg_pct"),
                            (pl.col("fg3_m") / pl.col("fg3_a")).alias("fg3_pct"),
                            (pl.col("ftm") / pl.col("fta")).alias("ft_pct"),
                        ]
                    )
                )

                bst = (
                    bst.join_where(
                        bst.select("game_id", "team_id", "pts"),
                        pl.col("game_id") == pl.col("game_id_t"),
                        pl.col("team_id") != pl.col("team_id_t"),
                        suffix="_t",
                    )
                    .with_columns((pl.col("pts") - pl.col("pts_t")).alias("plus_minus"))
                    .select(cols_trad["column_name"].to_list())
                )

            except Exception:
                logger.exception("game_id %s (traditional stats) failed", game_id)
                failed.append(game_id)
                break

            # Advanced stats
            try:
                bsa = (
                    pl.from_pandas(
                        nba_ep.boxscoreadvancedv3.BoxScoreAdvancedV3(
                            game_id=game_id
                        ).get_data_frames()[1]
                    )
                    .rename(
                        dict(
                            zip(
                                cols_adv.drop_nulls()["origin_name"],
                                cols_adv.drop_nulls()["column_name"],
                            )
                        )
                    )
                    .with_columns(cs.string().replace("", None))
                    .with_columns(pl.col("game_id").cast(pl.Int64))
                    .select(cols_adv["column_name"].to_list())
                )
            except Exception:
                logger.exception("game_id %s (advanced stats) failed", game_id)
                failed.append(game_id)
                break

            dfs.append(bst.join(bsa, on=["game_id", "team_id"], how="left"))

        if failed:
            logger.warning("player_box_score: %d game(s) failed: %s", len(failed), failed)

        if not dfs:
            logger.error("player_box_score: nothing fetched, skipping write")
            return

        df = pl.concat(dfs)
        self.db.write(df, "team_box_score", schema="nba")
        logger.info("nba.team_box_score have been updated (%d rows)", len(df))

    def _played_games(self, season: str) -> pl.DataFrame:
        """Completed games for a season, from LeagueGameLog — one call per season type."""
        year = int(season[:4])
        dfs = []
        for type_season in ["Regular Season", "Pre Season", "Playoffs", "All Star"]:
            hist_game_schedule = nba_ep.leaguegamelog.LeagueGameLog(
                season_type_all_star=type_season, season=year
            )
            dfs.append(
                (
                    pl.from_pandas(hist_game_schedule.get_data_frames()[0]).with_columns(
                        pl.lit(type_season).alias("season_type")
                    )
                )
            )
            time.sleep(1)

        frames = [d for d in dfs if len(d) > 0]
        if not frames:
            # Pre-season: nothing played yet, so the schedule is upcoming games only.
            logger.info("nba.league_game_schedule: no games played yet for %s", season)
            return pl.DataFrame(schema={"game_id": pl.Int64})

        df = (
            pl.concat(frames)
            .clean_names(case_type="snake")
            .with_columns(
                [
                    pl.col("game_id").cast(pl.Int64),
                    pl.col("game_date").str.to_date(),
                    pl.lit(season).alias("season"),
                    pl.col("matchup").str.replace_all(r" @ | vs\.? ", "-").alias("opponent"),
                ]
            )
            .with_columns(pl.col("opponent").str.split("-"))
            .with_columns(
                pl.when(pl.col("team_abbreviation") == pl.col("opponent").list.get(0))
                .then(pl.col("opponent").list.get(1))
                .otherwise(pl.col("opponent").list.get(0))
                .alias("opponent")
            )
            .with_columns(
                [
                    (
                        pl.when(pl.col("wl") == "W")
                        .then(pl.col("team_abbreviation"))
                        .otherwise(pl.col("opponent"))
                        .alias("team_winner")
                    ),
                    (
                        pl.when(pl.col("wl") == "L")
                        .then(pl.col("team_abbreviation"))
                        .otherwise(pl.col("opponent"))
                        .alias("team_loser")
                    ),
                    (
                        pl.when(pl.col("matchup").str.contains("vs."))
                        .then(pl.lit(True))
                        .otherwise(pl.lit(False))
                        .alias("home")
                    ),
                ]
            )
            .rename({"team_abbreviation": "team"})
        )

        return df

    def _upcoming_games(self, season: str, played_ids: pl.DataFrame) -> pl.DataFrame | None:
        """Not-yet-played games for a season, from the NBA's static schedule JSON.

        Returns None when the CDN is unusable, so the caller can tell "fetch
        failed" apart from "no upcoming games" and avoid wiping stored rows.
        """
        request = None
        try:
            request = requests.get(_SCHEDULE_URL, timeout=30)
            request.raise_for_status()
            payload = request.json()
        except (requests.exceptions.RequestException, ValueError):
            logger.warning(
                "nba.league_game_schedule: schedule CDN unusable "
                "(status=%s, body starts %r) — keeping stored upcoming games",
                getattr(request, "status_code", None),
                getattr(request, "content", b"")[:200],
            )
            return None

        key_dates = self.db.read(f"SELECT * FROM nba.key_dates WHERE season = '{season}'")

        dfs = []
        for game_date in payload["leagueSchedule"]["gameDates"]:
            for game in game_date["games"]:
                dfs.append(
                    {
                        "game_id": int(game["gameId"]),
                        "game_date": dateutil.parser.parse(game_date["gameDate"]).date(),
                        "matchup": game["homeTeam"]["teamTricode"]
                        + " vs. "
                        + game["awayTeam"]["teamTricode"],
                    }
                )

        df = (
            pl.concat(
                [
                    pl.DataFrame(dfs),
                    (
                        pl.DataFrame(dfs)
                        .with_columns(pl.col("matchup").str.split_exact(" ", n=2))
                        .unnest(pl.col("matchup"))
                        .with_columns(
                            (pl.col("field_2") + " @ " + pl.col("field_0"))
                            .str.strip_chars()
                            .alias("matchup")
                        )
                        .drop(cs.starts_with("field"))
                    ),
                ]
            )
            .with_columns(pl.col("matchup").str.split_exact(" ", n=2).alias("temp"))
            .unnest(pl.col("temp"))
            .with_columns(
                [
                    pl.lit(season).alias("season"),
                    pl.lit(None).cast(pl.String).alias("team_winner"),
                    pl.lit(None).cast(pl.String).alias("team_loser"),
                    pl.col("field_0").alias("team"),
                    pl.col("field_2").alias("opponent"),
                    pl.when(pl.col("field_1") == "vs.")
                    .then(pl.lit(True))
                    .otherwise(pl.lit(False))
                    .alias("home"),
                ]
            )
            .with_columns(
                [
                    pl.when(pl.col("matchup").str.strip_chars().is_in(["@", "vs."]))
                    .then(pl.lit("undetermined") if col == "matchup" else pl.lit(None))
                    .otherwise(pl.col(col))
                    .alias(col)
                    for col in ["team", "opponent", "matchup"]
                ]
            )
            .join_where(
                key_dates,
                pl.col("game_date") >= pl.col("begin_date"),
                pl.col("game_date") <= pl.col("end_date"),
            )
            .join(
                played_ids.select("game_id").with_columns(pl.col("game_id").cast(pl.Int64)),
                on="game_id",
                how="anti",
            )
            .filter(
                pl.col("team").is_in(self.ctx.nba_teams.get_column("abbreviation").to_list())
                | pl.col("opponent").is_in(self.ctx.nba_teams.get_column("abbreviation").to_list())
                | (pl.col("matchup") == "undetermined")
            )
        )

        return df

    def get_game_schedule(self, season: str | None = None, upcoming_only: bool = False):
        """
        League schedule for one season — played and upcoming games in one write.

        Played games come from LeagueGameLog (four calls, one per season type) and
        carry a winner; upcoming games come from the NBA's static schedule JSON and
        have a null one. Writing them together means the table is never left half
        populated, and the delete is scoped to the season actually asked for.

        upcoming_only skips the four played calls and replaces just the undecided
        rows — the cheap daily refresh once a season is under way.
        """
        season = season or self.ctx.cur_season

        logger.info("nba.league_game_schedule")

        if upcoming_only:
            played = self.db.read(
                f"SELECT game_id FROM nba.league_game_schedule "
                f"WHERE season = '{season}' AND team_winner IS NOT NULL"
            )
            df = self._upcoming_games(season, played)
            if df is None:
                # The CDN refused the request; keep the undecided rows we already
                # have rather than replacing them with nothing.
                logger.warning("nba.league_game_schedule: upcoming refresh skipped")
                return
            self.db.delete_where(
                "league_game_schedule", "nba", f"season = '{season}' AND team_winner IS NULL"
            )
        else:
            played = self._played_games(season)
            upcoming = self._upcoming_games(season, played)
            if upcoming is None:
                upcoming = pl.DataFrame(schema=_UPCOMING_GAMES_SCHEMA)
            df = pl.concat([played, upcoming], how="diagonal_relaxed")
            self.db.delete_where("league_game_schedule", "nba", f"season = '{season}'")

        self.db.write_ordered(df, "league_game_schedule", schema="nba")

    def get_team_roster(self, pre_season=False):
        teams = self.ctx.nba_teams["id"].to_list()

        logger.info("nba.team_roster")
        dfs = []
        failed = []
        for ix, team in enumerate(teams):
            # stats.nba.com intermittently answers CommonTeamRoster with an empty
            # body, which nba_api surfaces as a JSONDecodeError. Unguarded, one
            # such team aborts the loop and loses every roster in the run, so a
            # bad team is skipped and named rather than ending the fetch.
            try:
                common_teamroster = nba_ep.commonteamroster.CommonTeamRoster(
                    season=self.ctx.cur_season_year, team_id=team
                )
                dfs.append(pl.from_pandas(common_teamroster.get_data_frames()[0]))
            except Exception:
                logger.exception("team_id %s failed", team)
                failed.append(team)
            if ix % 5 == 0:
                logger.debug("team: %d / %d", ix, len(teams))
            time.sleep(1)
        logger.debug("team: %d / %d", len(teams), len(teams))

        if failed:
            logger.warning("team_roster: %d team(s) failed: %s", len(failed), failed)

        if not dfs:
            logger.error("team_roster: nothing fetched, skipping write")
            return

        df = (
            # diagonal_relaxed for the same reason as get_player_info: the
            # CommonTeamRoster column set is not stable across teams, so a plain
            # concat raises ShapeError on the first pair that disagrees.
            pl.concat(dfs, how="diagonal_relaxed")
            .clean_names(case_type="snake")
            .with_columns(
                [
                    pl.col("num").str.replace("", None),
                    pl.lit(self.ctx.cur_season).alias("season"),
                    pl.lit(None).cast(pl.Float64).alias("salary"),
                    pl.lit(None if pre_season else self.ctx.date_est).cast(pl.Date).alias("entry_date"),
                    pl.lit(None).cast(pl.Date).alias("exit_date"),
                ]
            )
            .join(self.ctx.nba_teams, left_on="team_id", right_on="id", how="left")
            .rename({"abbreviation": "team_slug"})
        )

        # Shape to the table now, not just at write time: the traded-player branch
        # below concats this frame against the stored table, and the raw API frame
        # is far wider than the registered columns, so an unconformed concat raises
        # ShapeError before any write is reached.
        df = self.db.conform(df, "team_roster", "nba")

        if pre_season:
            self.db.write_ordered(df, "team_roster", schema="nba")
        else:
            df_existing = self.db.read(
                f"SELECT * FROM nba.team_roster WHERE season = '{self.ctx.cur_season}' AND exit_date IS NULL",

                schema_overrides={
                    "team_id": pl.Int64,
                    "player_id": pl.Int64,
                    "entry_date": pl.Date,
                    "exit_date": pl.Date,
                },
            ).with_columns(
                pl.when(pl.col("exit_date").is_null())
                .then(pl.lit(self.ctx.date_est))
                .otherwise(pl.col("exit_date"))
                .alias("exit_date")
            )

            if df_existing.is_empty():
                # No snapshot stored for the season yet — the very first
                # non-preseason run, or a season the table has never seen.
                # There are no trades to diff against, so persist this snapshot;
                # concat below would otherwise join against a 0-row frame whose
                # inferred Null dtypes cannot match the new frame's String season.
                self.db.write_ordered(df, "team_roster", schema="nba")
                return

            df_traded = (
                pl.concat([df, df_existing])
                .join(
                    (
                        df.join(
                            df_existing, on=["season", "team_id", "player_id"], how="anti"
                        ).select("player_id")
                    ),
                    on="player_id",
                    how="inner",
                )
                .with_columns(pl.col("salary").backward_fill().forward_fill().over("player_id"))
            )

            if len(df_traded) > 0:
                del_ids = df_traded.unique("player_id")["player_id"].to_list()
                del_ids = ", ".join(map(str, del_ids))

                self.db.execute(
                    f"DELETE FROM nba.team_roster WHERE season = '{self.ctx.cur_season}' AND player_id IN ({del_ids}) AND exit_date IS NULL",
    
                )

                self.db.write(df_traded, "team_roster", schema="nba")
                logger.info("nba.team_roster traded players have been updated (%d rows)", len(df_traded))
            else:
                logger.info("nba.team_roster: nothing to update")

    def get_play_by_play(self):
        """
        Play-by-play events for games not yet in nba.play_by_play.
        
        Follows get_player_box_score() pattern:
        - Query view for max(game_date)
        - Fetch all games after that date
        - Append new data
        """
        pbp_max_dt = (
            self.db.read(
                "SELECT MAX(game_date) FROM nba.play_by_play_vw WHERE action_number IS NOT NULL"
            )
            .item()
        )
        
        if pbp_max_dt is None:
            where_clause = "1=1"
        else:
            where_clause = f"game_date > '{pbp_max_dt.strftime('%Y-%m-%d')}'"
        
        game_ids = self.db.read(
            f"""
            SELECT DISTINCT game_id
            FROM nba.league_game_schedule
            WHERE {where_clause}
              AND game_date <= '{self.ctx.date_est}'
              AND team_winner IS NOT NULL
            """
        )
        
        if game_ids.is_empty():
            logger.info("nba.play_by_play: no new games to fetch")
            return
        
        logger.info("nba.play_by_play: fetching %d games", len(game_ids))
        
        dfs = []
        failed = []
        
        for idx, game_id in enumerate(game_ids["game_id"].to_list()):
            game_id_str = f"00{game_id}"
            
            logger.debug("processing game_id %s (%d/%d)", game_id_str, idx + 1, len(game_ids))
            
            try:
                pbp = nba_ep.playbyplayv3.PlayByPlayV3(game_id=game_id_str)
                
                df = (
                    pl.from_pandas(pbp.play_by_play.get_data_frame())
                    .clean_names(case_type="snake")
                    .with_columns(pl.col("game_id").cast(pl.Int64))
                )
                
                dfs.append(df)
                
            except Exception:
                logger.exception("game_id %s failed", game_id_str)
                failed.append(game_id_str)
            
            time.sleep(1)
            
            if (idx + 1) % 10 == 0:
                logger.info("play_by_play: %d/%d games processed", idx + 1, len(game_ids))
        
        if failed:
            logger.warning("play_by_play: %d game(s) failed: %s", len(failed), failed)
        
        if not dfs:
            logger.error("play_by_play: nothing fetched, skipping write")
            return
        
        df = pl.concat(dfs)
        self.db.write_ordered(df, "play_by_play", schema="nba")
        logger.info("nba.play_by_play has been updated (%d rows)", len(df))

    def backfill_play_by_play(self, seasons=None):
        """
        Backfill play-by-play for historical seasons.
        
        Args:
            seasons: List of season strings like ["2023-24", "2024-25"].
                     If None, backfills from 2000-01 to current season.
        
        Writes data by game_date (not by season) for better crash recovery:
        if interrupted, only the current date's games are lost, and the rest
        of the season is already persisted.
        
        Idempotent: deletes existing data for each date before writing,
        so safe to re-run if interrupted.
        """
        if seasons is None:
            start_year = 2009
            end_year = self.ctx.cur_season_year
            seasons = [f"{year}-{str(year + 1)[2:]}" for year in range(start_year, end_year + 1)]
        
        logger.info("play_by_play backfill: processing %d seasons", len(seasons))
        
        total_rows = 0
        failed_dates = []
        
        for season in seasons:
            logger.info("play_by_play backfill: processing season %s", season)
            
            # Get all game_dates for this season
            game_dates = self.db.read(
                f"""
                SELECT DISTINCT game_date
                FROM nba.league_game_schedule
                WHERE season = '{season}'
                  AND team_winner IS NOT NULL
                ORDER BY game_date
                """
            )
            
            if game_dates.is_empty():
                logger.warning("play_by_play backfill: no games found for %s", season)
                continue
            
            logger.info("play_by_play backfill: %d game dates in %s", len(game_dates), season)
            
            # Process each date
            for date_idx, game_date in enumerate(game_dates["game_date"].to_list()):
                # Get games for this specific date
                game_ids = self.db.read(
                    f"""
                    SELECT DISTINCT game_id
                    FROM nba.league_game_schedule
                    WHERE season = '{season}'
                      AND game_date = '{game_date}'
                      AND team_winner IS NOT NULL
                    ORDER BY game_id
                    """
                )
                
                dfs = []
                failed_games = []
                
                for idx, game_id in enumerate(game_ids["game_id"].to_list()):
                    game_id_str = f"00{game_id}"
                    
                    logger.debug("backfill %s %s: game_id %s (%d/%d)", 
                                season, game_date, game_id_str, idx + 1, len(game_ids))
                    
                    try:
                        pbp = nba_ep.playbyplayv3.PlayByPlayV3(game_id=game_id_str)
                        
                        df = (
                            pl.from_pandas(pbp.play_by_play.get_data_frame())
                            .clean_names(case_type="snake")
                            .with_columns(pl.col("game_id").cast(pl.Int64))
                        )
                        
                        if len(df) > 0:
                            dfs.append(df)
                        else:
                            # API returns empty for very old games - log once per date
                            if idx == 0:
                                logger.debug("backfill %s %s: API returns empty data (game too old?)",
                                           season, game_date)
                        
                    except Exception:
                        logger.exception("backfill %s %s: game_id %s failed", 
                                       season, game_date, game_id_str)
                        failed_games.append(game_id_str)
                    
                    time.sleep(1)
                
                if failed_games:
                    logger.warning("backfill %s %s: %d game(s) failed", 
                                 season, game_date, len(failed_games))
                    failed_dates.append(f"{season} {game_date}")
                
                if not dfs:
                    logger.warning("backfill %s %s: nothing fetched, skipping", 
                                 season, game_date)
                    continue
                
                # Debug: check what we have before concat
                logger.info("backfill %s %s: %d dataframes to concat, sizes: %s",
                           season, game_date, len(dfs), [len(d) for d in dfs])
                
                # Delete existing data for these game_ids (idempotent)
                game_id_list = ", ".join(str(g) for g in game_ids["game_id"].to_list())
                self.db.execute(
                    f"DELETE FROM nba.play_by_play WHERE game_id IN ({game_id_list})"
                )
                
                # Write this date's data immediately
                df = pl.concat(dfs)
                logger.info("backfill %s %s: writing %d rows from %d game(s)",
                           season, game_date, len(df), len(dfs))
                self.db.write_ordered(df, "play_by_play", schema="nba")
                
                total_rows += len(df)
                
                if (date_idx + 1) % 10 == 0:
                    logger.info("backfill %s: %d/%d dates processed, %d total rows", 
                              season, date_idx + 1, len(game_dates), total_rows)
        
        logger.info("play_by_play backfill complete: %d total rows across %d seasons", 
                    total_rows, len(seasons))
        
        if failed_dates:
            logger.warning("backfill had failures on %d date(s): %s", 
                         len(failed_dates), failed_dates[:10])


