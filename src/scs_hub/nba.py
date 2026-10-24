import time
import zoneinfo
import datetime as dt
import logging
import polars as pl
import polars.selectors as cs
import janitor.polars  # noqa: F401  (registers .clean_names() on pl.DataFrame)
import nbainjuries
import nba_api.stats.endpoints as nba_ep

logger = logging.getLogger(__name__)

# Only the season types this component stores. All-Star (003), the play-in
# tournament (005) and the NBA Cup final (006) are intentionally out of scope.
_SCHEDULE_SEASON_TYPES = {
    "001": "Pre Season",
    "002": "Regular Season",
    "004": "Playoffs",
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
            schema_overrides={"game_date": pl.Date, "game_id": pl.Int64, "matchup": pl.String},
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

        report = nbainjuries.injury.get_reportdata(valid_time, return_df=True)
        name_col = next(
            (
                c for c in report.columns
                if str(c).strip().lower().replace(" ", "_") == "player_name"
            ),
            None,
        )
        if name_col is not None:
            # A report with no usable names infers the column as float, and the
            # list split below then fails; nullable string turns empties into NA.
            report[name_col] = report[name_col].astype("string")

        df = (
            pl.from_pandas(report)
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
            # No scheduled game in the window (e.g. a pre-season report with no
            # upcoming games stored yet): the left join leaves game_id null.
            game_predicate = "game_id IS NULL" if game_id is None else f"game_id = {game_id}"
            self.db.execute(
                f"DELETE FROM nba.injuries WHERE game_date = '{game_date}' AND {game_predicate} AND player_name = '{player_name}'",

            )

        self.db.write_ordered(df, "injuries", schema="nba")

    def get_player_box_score(self):
        game_ids = self.db.read(
            f"""
            SELECT DISTINCT g.game_id
            FROM nba.league_game_schedule g
            WHERE g.season = '{self.ctx.cur_season}'
              AND g.game_date <= '{self.ctx.date_est}'
              AND g.team_winner IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM nba.player_box_score p WHERE p.game_id = g.game_id
              )
            ORDER BY g.game_id
            """
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
        game_ids = self.db.read(
            f"""
            SELECT DISTINCT g.game_id
            FROM nba.league_game_schedule g
            WHERE g.season = '{self.ctx.cur_season}'
              AND g.game_date <= '{self.ctx.date_est}'
              AND g.team_winner IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM nba.team_box_score t WHERE t.game_id = g.game_id
              )
            ORDER BY g.game_id
            """
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
            logger.warning("team_box_score: %d game(s) failed: %s", len(failed), failed)

        if not dfs:
            logger.error("team_box_score: nothing fetched, skipping write")
            return

        df = pl.concat(dfs)
        self.db.write(df, "team_box_score", schema="nba")
        logger.info("nba.team_box_score have been updated (%d rows)", len(df))

    def _scheduleleaguev2_games(self, season: str) -> pl.DataFrame | None:
        try:
            raw = nba_ep.scheduleleaguev2.ScheduleLeagueV2(
                league_id="00", season=season
            ).get_data_frames()[0]
            if raw is None or len(raw) == 0:
                logger.warning(
                    "nba.league_game_schedule: ScheduleLeagueV2 returned no rows"
                )
                return None

            df = (
                pl.from_pandas(raw)
                .with_columns(
                    pl.col("gameId").str.slice(0, 3).alias("prefix"),
                    pl.col("gameStatus").cast(pl.Int64).alias("game_status"),
                    pl.col("homeTeam_teamTricode")
                    .cast(pl.String)
                    .str.strip_chars()
                    .alias("home_team"),
                    pl.col("awayTeam_teamTricode")
                    .cast(pl.String)
                    .str.strip_chars()
                    .alias("away_team"),
                    pl.col("homeTeam_score").cast(pl.Int64).alias("home_score"),
                    pl.col("awayTeam_score").cast(pl.Int64).alias("away_score"),
                )
                .filter(pl.col("prefix").is_in(list(_SCHEDULE_SEASON_TYPES)))
                .with_columns(
                    pl.col("prefix")
                    .replace_strict(_SCHEDULE_SEASON_TYPES)
                    .alias("season_type")
                )
                .filter(
                    pl.col("home_team").is_not_null()
                    & (pl.col("home_team") != "")
                    & pl.col("away_team").is_not_null()
                    & (pl.col("away_team") != "")
                )
            )

            nba_teams = self.ctx.nba_teams.get_column("abbreviation").to_list()
            df = df.filter(
                pl.col("home_team").is_in(nba_teams)
                & pl.col("away_team").is_in(nba_teams)
            )

            final = (
                (pl.col("game_status") == 3)
                & pl.col("home_score").is_not_null()
                & pl.col("away_score").is_not_null()
            )
            home_win = final & (pl.col("home_score") > pl.col("away_score"))
            away_win = final & (pl.col("away_score") > pl.col("home_score"))

            df = df.with_columns(
                pl.when(home_win)
                .then(pl.col("home_team"))
                .when(away_win)
                .then(pl.col("away_team"))
                .otherwise(None)
                .cast(pl.String)
                .alias("team_winner"),
                pl.when(home_win)
                .then(pl.col("away_team"))
                .when(away_win)
                .then(pl.col("home_team"))
                .otherwise(None)
                .cast(pl.String)
                .alias("team_loser"),
            )

            home = df.select(
                pl.col("gameId").cast(pl.Int64).alias("gameId"),
                pl.col("gameDate")
                .str.slice(0, 10)
                .str.to_date("%m/%d/%Y")
                .alias("gameDate"),
                pl.col("seasonYear"),
                pl.col("season_type"),
                pl.concat_str(
                    [pl.col("home_team"), pl.lit(" vs. "), pl.col("away_team")]
                ).alias("matchup"),
                pl.col("home_team").alias("team"),
                pl.col("away_team").alias("opponent"),
                pl.lit(True).alias("home"),
                pl.col("team_winner"),
                pl.col("team_loser"),
            )
            away = df.select(
                pl.col("gameId").cast(pl.Int64).alias("gameId"),
                pl.col("gameDate")
                .str.slice(0, 10)
                .str.to_date("%m/%d/%Y")
                .alias("gameDate"),
                pl.col("seasonYear"),
                pl.col("season_type"),
                pl.concat_str(
                    [pl.col("away_team"), pl.lit(" @ "), pl.col("home_team")]
                ).alias("matchup"),
                pl.col("away_team").alias("team"),
                pl.col("home_team").alias("opponent"),
                pl.lit(False).alias("home"),
                pl.col("team_winner"),
                pl.col("team_loser"),
            )

            df = pl.concat([home, away]).sort(["gameId", "team"])
            dupes = df.height - df.unique(subset=["gameId", "team"]).height
            if dupes:
                logger.error(
                    "nba.league_game_schedule: %d duplicate game/team rows", dupes
                )
                return None
            return df
        except Exception:
            logger.exception("nba.league_game_schedule: ScheduleLeagueV2 failed")
            return None

    def get_game_schedule(self, season: str | None = None):
        """
        Replace one season's league schedule from ScheduleLeagueV2.

        Defaults to the current season. Passing another season is a best-effort
        backfill: only Pre Season/Regular Season/Playoffs are stored, and the
        team filter uses current abbreviations, so relocated franchises can be
        dropped.

        The endpoint returns the full season — played and upcoming games — in one
        call. A failed or empty fetch raises before any delete, so the stored
        season is left untouched.
        """
        season = season or self.ctx.cur_season
        if season != self.ctx.cur_season:
            logger.warning(
                "nba.league_game_schedule: backfilling %s uses current team "
                "abbreviations; relocated franchises and non-stored season "
                "types may be omitted",
                season,
            )

        logger.info("nba.league_game_schedule")
        df = self._scheduleleaguev2_games(season)
        if df is None or df.is_empty():
            logger.error(
                "nba.league_game_schedule: no games returned for %s — "
                "leaving stored rows untouched",
                season,
            )
            raise RuntimeError(
                f"nba.league_game_schedule: no games returned for {season}"
            )

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
        Play-by-play events for completed current-season games missing from
        nba.play_by_play.

        Selects scheduled games that have no rows in the table, so a game that
        failed to write at all is retried on the next pass.
        """
        game_ids = self.db.read(
            f"""
            SELECT DISTINCT g.game_id
            FROM nba.league_game_schedule g
            WHERE g.season = '{self.ctx.cur_season}'
              AND g.game_date <= '{self.ctx.date_est}'
              AND g.team_winner IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM nba.play_by_play p WHERE p.game_id = g.game_id
              )
            ORDER BY g.game_id
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
                
                if df.is_empty():
                    logger.debug("game_id %s returned no play-by-play events", game_id_str)
                else:
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
                     If None, backfills from 2009-10 to current season.

        Writes data by game_date (not by season) for better crash recovery:
        if interrupted, only the current date's games are lost, and the rest
        of the season is already persisted.

        Idempotent: skips games already in nba.play_by_play and appends each
        date's new rows, so it is safe to re-run if interrupted.
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
                all_game_ids = self.db.read(
                    f"""
                    SELECT DISTINCT game_id
                    FROM nba.league_game_schedule
                    WHERE season = '{season}'
                      AND game_date = '{game_date}'
                      AND team_winner IS NOT NULL
                    ORDER BY game_id
                    """
                )
                
                # Check which games are already in the table
                existing_games = self.db.read(
                    f"""
                    SELECT DISTINCT game_id 
                    FROM nba.play_by_play 
                    WHERE game_id IN ({", ".join(str(g) for g in all_game_ids["game_id"].to_list())})
                    """
                )
                
                # Only fetch games not already in the table
                if existing_games.is_empty():
                    game_ids = all_game_ids
                else:
                    game_ids = all_game_ids.join(
                        existing_games, on="game_id", how="anti"
                    )
                
                if game_ids.is_empty():
                    logger.debug("backfill %s %s: all games already in table, skipping",
                               season, game_date)
                    continue
                
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
                    logger.info("backfill %s %s: no new games to fetch (all already in table or too old)",
                              season, game_date)
                    continue
                
                # Write this date's new data (append only, no delete needed)
                df = pl.concat(dfs)
                logger.info("backfill %s %s: %d rows from %d new game(s)",
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


