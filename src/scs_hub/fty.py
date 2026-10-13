import configparser
import logging

import polars as pl
from scs_hub.fty_handlers import HANDLERS

logger = logging.getLogger(__name__)


class FtyComponent:
    """
    Fantasy platform integration — writes into the schema named by `schema`
    (default "fty", the only fantasy schema since the fty_dev promotion).

    Credentials come from the same schema as everything else — customer_platform
    is replicated into each working schema — so a schema is self-contained and
    can stand in for fty wholesale.

    Connections are scoped per LEAGUE, not per customer: multiple customers
    can belong to the same league, and fetching that league's data once
    (rather than once per customer) avoids redundant API calls. Customer
    attribution is a separate join against customer_league downstream,
    not something this component filters by.

    Each (sport, platform) combination has its own handler (see
    fty_handlers/) — ESPN and Yahoo are different APIs, and different
    sports within the same platform have different field/category shapes,
    so each combination owns its full behavior rather than branching
    inside shared methods. Season is derived per-connection by each
    handler (see FtyHandler.connect), not read from Context — Context only
    holds things genuinely generic across every sport/platform.

    NOTE: assumes league has a `sport` column so leagues can be routed
    to the right handler. The original schema this was ported from was
    NBA-only and didn't have one — add it before running this for real.
    """

    def __init__(self, db, ctx, sport: str, leagues: pl.DataFrame, schema: str = "fty"):
        self.db = db
        self.ctx = ctx
        self.sport = sport
        self.schema = schema
        self.handlers = {key: cls(db, schema) for key, cls in HANDLERS.items()}
        self.leagues = self._connect_leagues(leagues)

    def connect_leagues(self, leagues: pl.DataFrame | None = None, season: str | None = None) -> None:
        """
        (Re)connect to a set of leagues after this component has already
        been constructed — e.g. when `leagues` isn't known until after
        `hub = SportsHub()` has run. Replaces any leagues currently
        connected; call this instead of reaching into `_connect_leagues`
        or `self.leagues` directly.

        With no arguments this connects every league registered for the
        current season. Pass `leagues` to connect an arbitrary subset (one
        league while debugging, say), or `season` to target another year.
        """
        if leagues is None:
            leagues = self._registered_leagues(season or self.ctx.cur_season)

        self.leagues = self._connect_leagues(leagues)
        self.league_ids = ', '.join(map(str, {league_id for _, _, league_id in self.leagues}))

    def _registered_leagues(self, season: str) -> pl.DataFrame:
        """
        Every league with credentials for a season, shaped for _connect_leagues.

        Credentials hang off the customer rather than the league, so this joins
        through customer_platform; DISTINCT because several customers can share
        one league and it only needs connecting once.

        The league's finish date (MAX(matchup_end) in league_matchup_dates) is
        carried out so a finished CURRENT-season league can be logged and skipped
        rather than connected. Only the current season is filtered: an explicit
        season — a backfill — connects regardless, because a finished historical
        season is exactly what it wants. A league with no derived dates yet
        (pre-season, or a platform with no date deriver such as Yahoo) is kept.
        The boundary is the finish date plus one day: a league still connects
        through the day after its last matchup, then is skipped.
        """
        leagues = self.db.read(
            "SELECT DISTINCT cl.season, cl.platform, cl.league_id, cp.credentials, "
            "       md.finish_date "
            f"FROM {self.schema}.customer_league cl "
            f"JOIN {self.schema}.customer_platform cp "
            "  ON cp.customer_id = cl.customer_id AND cp.platform = cl.platform "
            f"LEFT JOIN (SELECT season, platform, league_id, "
            f"                   MAX(matchup_end) AS finish_date "
            f"            FROM {self.schema}.league_matchup_dates "
            "             GROUP BY season, platform, league_id) md "
            "  ON md.season = cl.season AND md.platform = cl.platform "
            "     AND md.league_id = cl.league_id "
            f"WHERE cl.season = '{season}'"
        )

        if season == self.ctx.cur_season:
            # Cast first: a season with no derived dates yet (pre-season) makes the
            # LEFT JOIN's finish_date an all-null column, which Polars types as Null —
            # and Null + duration is an InvalidOperationError, not a null. Casting to
            # Date keeps the arithmetic valid (all-null stays all-null) so the
            # is_not_null guard can do its job.
            finish_date = pl.col("finish_date").cast(pl.Date)
            finished = (
                finish_date.is_not_null()
                & ((finish_date + pl.duration(days=1)) < pl.lit(self.ctx.date_est))
            )
            for r in leagues.filter(finished).iter_rows(named=True):
                logger.info(
                    "%s;%s: season %s finished on %s — skipping connection",
                    r["platform"], r["league_id"], season, r["finish_date"],
                )
            leagues = leagues.filter(~finished)

        return leagues.drop("finish_date")

    def _season_year_for(self, sport: str) -> int:
        # Season semantics are sport-specific. Only NBA is wired up today —
        # add a branch here (not inside a handler) when a new sport's season
        # source is decided, so this stays the one place that knowledge lives.
        if sport == "nba":
            return self.ctx.cur_season_year
        raise NotImplementedError(f"No season source configured for sport '{sport}'")

    @staticmethod
    def _season_year_from(season: str) -> int:
        """
        Opening year of a "YYYY-YY" season label, matching Context's convention.
        Kept separate from _season_year_for, which answers the different question
        of which season is current for a sport.
        """
        return int(str(season)[:4])

    def _connect_leagues(self, leagues: pl.DataFrame) -> dict:
        """
        Build one live API connection per row of `leagues` (columns:
        league_id, platform, credentials — a per-league struct of
        secret values, e.g. {swid, espn_s2} for ESPN).

        credentials.ini (same file `db` and `statyx` use) holds one
        section per platform (e.g. [espn_api], [yahoo_api]) that is a
        PLACEHOLDER TEMPLATE only — no real secrets committed to it.
        Every value is a token like `{swid}` or `{league_id}`. At
        connect time, each row's `credentials` struct is merged with
        `league_id`/`cur_year` into a substitution context, and every
        `{key}` placeholder found in the template is replaced with that
        context's value for `key`. Plain string replacement is used
        (not str.format()) since real values — e.g. ESPN's `swid`,
        which is itself wrapped in literal curly braces like
        "{65841A9D-...}" — get substituted into the template text
        rather than the other way around, so there's no risk of them
        being misparsed as format fields.

        Keyed by (sport, platform, league_id) so callers can dispatch to
        the right handler and know which league a result belongs to.
        """
        if leagues.is_empty():
            return {}

        parser = configparser.ConfigParser()
        parser.read(self.db.ini_path)

        connected = {}
        for row in leagues.iter_rows(named=True):
            platform, league_id, row_creds = row["platform"], row["league_id"], row["credentials"]
            key = (self.sport, platform)

            if key not in self.handlers:
                logger.warning("No handler registered for %s (league %s) — skipping", key, league_id)
                continue

            # The row's own season wins, so a league registered for 2023-24
            # connects to 2023-24. Before this, season_year came only from
            # _season_year_for (i.e. ctx.cur_season_year), which meant
            # connect_leagues(season=...) chose WHICH leagues to connect but
            # still connected every one of them to the current season — the
            # returned con was stamped con.season = ctx.cur_season and ESPN
            # reported seasonId for the current year regardless of what was
            # asked for. Harmless in the daily path, but it made any backfill of
            # an earlier season silently a no-op.
            row_season = row.get("season")
            season_year = (
                self._season_year_from(row_season) if row_season
                else self._season_year_for(self.sport)
            )
            context = {**row_creds, "league_id": league_id, "cur_year": season_year}

            section = platform.lower() + "_api"
            creds = {}
            for k, template_val in parser.items(section):
                value = template_val
                for ctx_key, ctx_val in context.items():
                    value = value.replace("{" + ctx_key + "}", str(ctx_val))
                creds[k] = value

            con = self.handlers[key].connect(league_id, season_year, creds)
            connected[(self.sport, platform, league_id)] = con

        return connected

    def _dispatch(self, method_name: str) -> list:
        dfs = []
        for (sport, platform, league_id), con in self.leagues.items():
            logger.info(f"%s;%s {self.schema}.%s", platform, league_id, method_name)
            handler = self.handlers[(sport, platform)]
            dfs.append(getattr(handler, method_name)(con))
        return dfs

    def _delete_connected(self, table: str) -> None:
        """
        Clear exactly the (season, league_id) pairs currently connected, before
        the dispatch methods rewrite them.

        These deletes used to read `season = ctx.cur_season AND league_id IN
        (...)`, which was harmless only because every connection was built
        against the current season regardless of what was asked for. Now that a
        league connects at its own season, that form would delete the CURRENT
        season's rows and insert another season's in their place. Scoping to the
        connections' own seasons is what makes a per-season rerun safe.
        """
        pairs = ", ".join(
            f"('{con.season}', {league_id})"
            for (_sport, _platform, league_id), con in self.leagues.items()
        )
        if not pairs:
            return

        self.db.execute(
            f"DELETE FROM {self.schema}.{table} WHERE (season, league_id) IN ({pairs})"
        )

    def get_league(self):
        if not self.leagues:
            logger.warning("No leagues connected — skipping get_league")
            return

        self._delete_connected("league")
        df = pl.concat(self._dispatch("get_league"))
        self.db.write(df, "league", schema=self.schema)
        logger.info(f"{self.schema}.league has been updated (%d rows)", len(df))

    def get_league_categories(self):
        if not self.leagues:
            logger.warning("No leagues connected — skipping get_league_categories")
            return

        self._delete_connected("league_categories")
        df = pl.concat(self._dispatch("get_league_categories"))
        self.db.write(df, "league_categories", schema=self.schema)
        logger.info(f"{self.schema}.league_categories has been updated (%d rows)", len(df))

    def get_free_agents(self):
        if not self.leagues:
            logger.warning("No leagues connected — skipping get_free_agents")
            return

        self._delete_connected("free_agents")
        df = pl.concat(self._dispatch("get_free_agents"))
        self.db.write(df, "free_agents", schema=self.schema)
        logger.info(f"{self.schema}.free_agents has been updated (%d rows)", len(df))

    def get_league_competitor(self):
        if not self.leagues:
            logger.warning("No leagues connected — skipping get_league_competitor")
            return

        self._delete_connected("league_competitor")
        df = pl.concat(self._dispatch("get_league_competitor"))
        self.db.write(df, "league_competitor", schema=self.schema)
        logger.info(f"{self.schema}.league_competitor has been updated (%d rows)", len(df))

    def get_league_matchup(self):
        if not self.leagues:
            logger.warning("No leagues connected — skipping get_league_matchup")
            return

        self._delete_connected("league_matchup")
        df = pl.concat(self._dispatch("get_league_matchup"))
        self.db.write(df, "league_matchup", schema=self.schema)
        logger.info(f"{self.schema}.league_matchup has been updated (%d rows)", len(df))

    def get_league_matchup_dates(self):
        """
        Derive matchup period dates for the connected leagues.

        Works from self.leagues like its neighbours, so the season comes from
        connect_leagues: no argument for the current season at season start, or
        connect_leagues(season="2023-24") first to backfill an earlier one.

        Derived once per season rather than daily, so it has no
        util.update_schedule row — like `league`, `league_categories` and
        `league_competitor`.

        Dispatches BEFORE deleting, unlike the methods above. The handler raises
        when nba.key_dates has no opener for the season, and deleting first would
        leave the league with no dates at all — which silently removes it from
        league_schedule_vw, since that view joins them.
        """
        if not self.leagues:
            logger.warning("No leagues connected — skipping get_league_matchup_dates")
            return

        df = pl.concat(self._dispatch("get_league_matchup_dates"))
        self._delete_connected("league_matchup_dates")
        self.db.write(df, "league_matchup_dates", schema=self.schema)
        logger.info(f"{self.schema}.league_matchup_dates has been updated (%d rows)", len(df))

    def get_recent_activity(self):
        df = pl.concat(self._dispatch("get_recent_activity"))
        self.db.write(df, "recent_activity", schema=self.schema)
        logger.info(f"{self.schema}.recent_activity has been updated (%d rows)", len(df))

    def get_competitor_roster(self):
        if not self.leagues:
            logger.warning("No leagues connected — skipping get_competitor_roster")
            return

        df_mup = self.db.read(
            "SELECT * "
            f"FROM {self.schema}.league_matchup_dates "
            f"WHERE '{self.ctx.date_est}' BETWEEN matchup_start AND matchup_end",
        )

        self.db.execute(
            f"DELETE FROM {self.schema}.competitor_roster "
            f"WHERE assigned_date = '{self.ctx.date_est}' AND league_id IN ({self.league_ids})",
        )

        df = (
            pl.concat(self._dispatch("get_competitor_roster"))
            .with_columns(pl.lit(self.ctx.date_est).alias("assigned_date"))
            .join(df_mup, on=["platform", "league_id"], how="left")
        )

        self.db.write_ordered(df, "competitor_roster", schema=self.schema)

    def get_matchup_box_score(self, matchup_period: int | None = None):
        # Kept per-league (not batched into one _dispatch call) since leagues
        # can be on different matchup periods — matches the original
        # method's own comment about why this stays league-specific.
        if not self.leagues:
            logger.warning("No leagues connected — skipping get_matchup_box_score")
            return

        for (sport, platform, league_id), con in self.leagues.items():
            logger.info(f"%s;%s {self.schema}.matchup_box_score", platform, league_id)
            handler = self.handlers[(sport, platform)]
            df = handler.get_matchup_box_score(con, matchup_period)

            if df.is_empty():
                logger.info(f"%s;%s {self.schema}.matchup_box_score: nothing returned — skipped", platform, league_id)
                continue

            self.db.execute(
                f"DELETE FROM {self.schema}.matchup_box_score "
                f"WHERE season = '{con.season}' "
                f"AND platform = '{platform}' AND league_id = {league_id} "
                f"AND matchup = {df['matchup'][0]}",
            )
            self.db.write(df, "matchup_box_score", schema=self.schema)
            logger.info(f"%s;%s {self.schema}.matchup_box_score has been updated (%d rows)", platform, league_id, len(df))

    def get_matchup_result(self, matchup_period: int | None = None):
        """
        The outcome of the current matchup period, one row per competitor.

        Kept per-league rather than batched for the same reason as
        get_matchup_box_score: leagues can be on different matchup periods, so
        the delete has to be scoped to the period each league actually returned.
        """
        if not self.leagues:
            logger.warning("No leagues connected — skipping get_matchup_result")
            return

        for (sport, platform, league_id), con in self.leagues.items():
            logger.info(f"%s;%s {self.schema}.matchup_result", platform, league_id)
            handler = self.handlers[(sport, platform)]
            df = handler.get_matchup_result(con, matchup_period)

            if df.is_empty():
                logger.info(f"%s;%s {self.schema}.matchup_result: nothing returned — skipped", platform, league_id)
                continue

            self.db.execute(
                f"DELETE FROM {self.schema}.matchup_result "
                f"WHERE season = '{con.season}' "
                f"AND platform = '{platform}' AND league_id = {league_id} "
                f"AND matchup = {df['matchup'][0]}",
            )
            self.db.write(df, "matchup_result", schema=self.schema)
            logger.info(f"%s;%s {self.schema}.matchup_result has been updated (%d rows)", platform, league_id, len(df))

    def backfill_matchups(self, periods=None) -> None:
        """
        Refetch box scores and results for every matchup period a league has
        played, rather than only the current one.

        Needed because both per-period methods default to
        con.currentMatchupPeriod, so a new table starts out holding one period
        per league and nothing historical. The period list comes from each
        league's own con.matchup_ids, capped at its current period — periods
        beyond that have no games played, and ESPN returns an undecided shell
        for them rather than an error, which would otherwise write empty rows.

        Periods that come back empty are skipped rather than deleted, so a
        league with a bye or an unplayed period keeps whatever it already has.

        con.matchup_ids comes back empty for some older seasons (2023-24 ESPN,
        for one), so currentMatchupPeriod is the fallback — it is populated even
        when the id map is not.
        """
        if not self.leagues:
            logger.warning("No leagues connected — skipping backfill_matchups")
            return

        for (sport, platform, league_id), con in self.leagues.items():
            available = periods or sorted(
                p for p in con.matchup_ids if p <= con.currentMatchupPeriod
            ) or list(range(1, con.currentMatchupPeriod + 1))
            logger.info(
                "%s;%s backfilling matchups %s-%s", platform, league_id,
                min(available, default=0), max(available, default=0),
            )
            for matchup_period in available:
                for table in ("matchup_box_score", "matchup_result"):
                    handler = self.handlers[(sport, platform)]
                    df = getattr(handler, f"get_{table}")(con, matchup_period)
                    if df.is_empty():
                        continue
                    self.db.execute(
                        f"DELETE FROM {self.schema}.{table} "
                        f"WHERE season = '{con.season}' AND platform = '{platform}' "
                        f"AND league_id = {league_id} AND matchup = {matchup_period}",
                    )
                    self.db.write(df, table, schema=self.schema)
            logger.info("%s;%s backfill complete", platform, league_id)

    def get_league_byes(self):
        if not self.leagues:
            logger.warning("No leagues connected — skipping get_league_byes")
            return

        for (sport, platform, league_id), con in self.leagues.items():
            logger.info(f"%s;%s {self.schema}.league_byes", platform, league_id)
            handler = self.handlers[(sport, platform)]
            df = handler.get_league_byes(con)

            self.db.execute(
                f"DELETE FROM {self.schema}.league_byes "
                f"WHERE season = '{con.season}' "
                f"AND platform = '{platform}' AND league_id = {league_id}"
            )
            self.db.write(df, "league_byes", schema=self.schema)
            logger.info(f"%s;%s {self.schema}.league_byes has been updated (%d rows)", platform, league_id, len(df))