import json
import datetime as dt
import polars as pl
import espn_api.basketball as bb
from espn_api.basketball.constant import POSITION_MAP, STATS_MAP

from scs_hub.fty_handlers.base import FtyHandler

# ESPN reports the winning side of a matchup, not each competitor's result.
# UNDECIDED (an unplayed or in-progress period) is absent deliberately, so it
# resolves to None rather than to a decided result.
RESULT_MAP = {
    "HOME": {"home": "W", "away": "L"},
    "AWAY": {"home": "L", "away": "W"},
    "TIE":  {"home": "T", "away": "T"},
}

# ESPN's lineupSlotCounts is keyed by slot id. Slots 0-11 are the starting
# lineup (PG, SG, SF, PF, C, G, F, SG/SF, G/F, PF/C, F/C, UT); 12 is bench,
# 13 IR and 14 unused. 15 (rookie) is not used by NBA leagues.
STARTING_SLOT_IDS = frozenset(range(12))

# Fixed shape so a league with no activity (a new league, or one in the off-season)
# still yields a frame of the object's own columns, rather than the 0-column frame
# pl.DataFrame([]) gives — which the anti-join and the concat can't use.
_RECENT_ACTIVITY_SCHEMA = {
    "season": pl.String,
    "platform": pl.String,
    "league_id": pl.Int64,
    "timestamp": pl.Datetime("us", "UTC"),
    "competitor_id": pl.Int64,
    "action": pl.String,
    "player": pl.String,
}


class EspnNbaHandler(FtyHandler):
    NAME = "ESPN"

    def connect(self, league_id, season_year, creds):

        con = bb.League(
            league_id=int(league_id),
            year=int(season_year) + 1,
            espn_s2=creds["espn_s2"],
            swid=creds["swid"],
        )
        con.season = f"{season_year}-{str(season_year + 1)[-2:]}"
        return con

    def get_league(self, con) -> pl.DataFrame:
        # BaseSettings doesn't parse `isPublic` out of ESPN's raw response
        # (only scoring/schedule/trade settings get extracted), so it's
        # pulled directly from the raw league JSON instead of con.settings.
        raw = con.espn_request.get_league()
        roster_settings = raw["settings"]["rosterSettings"]
        slot_counts = {
            int(slot_id): int(count)
            for slot_id, count in roster_settings.get("lineupSlotCounts", {}).items()
        }
        return pl.DataFrame(
            [
                {
                    "season": con.season,
                    "platform": self.NAME,
                    "league_id": con.league_id,
                    "league_name": con.settings.name,
                    "scoring_type": con.settings.scoring_type,
                    "team_count": con.settings.team_count,
                    "is_public": raw["settings"]["isPublic"],
                    "roster_size": sum(slot_counts.values()),
                    "starter_count": sum(
                        count
                        for slot_id, count in slot_counts.items()
                        if slot_id in STARTING_SLOT_IDS
                    ),
                }
            ]
        )

    def get_league_categories(self, con) -> pl.DataFrame:
        # `points` is only meaningful for points-format leagues (scoring_type
        # "H2H_POINTS") — for category leagues (H2H_CATEGORY, ROTOTOTAL) this
        # is forced to None explicitly below, rather than relying on ESPN's
        # scoringItems happening not to carry a points value for those formats.
        #
        # schema_overrides forces `points` to Float64 even when every value
        # in this league is None — otherwise an all-None column infers as
        # Null dtype, which pl.concat can't reconcile against a Float64
        # points column from a different (points-format) league.
        dfs = []
        for item in con.settings._raw_scoring_settings.get("scoringItems", []):
            stat_id = str(item["statId"])
            dfs.append(
                {
                    "season": con.season,
                    "platform": self.NAME,
                    "league_id": con.league_id,
                    "category": STATS_MAP.get(stat_id, f"unknown({stat_id})"),
                    "points": item.get("points") if con.settings.scoring_type == "H2H_POINTS" else None,
                }
            )
        return pl.DataFrame(dfs, schema_overrides={"points": pl.Float64})

    def get_free_agents(self, con) -> pl.DataFrame:
        dfs = []
        for free_agent in con.free_agents(size=1000):
            dfs.append(
                {
                    "season": con.season,
                    "platform": self.NAME,
                    "league_id": con.league_id,
                    "timestamp": dt.datetime.now(dt.timezone.utc),
                    "player_id": free_agent.playerId,
                    "player_name": free_agent.name,
                    "player_team": free_agent.proTeam.replace("PHL", "PHI").replace("PHO", "PHX"),
                    "player_injury_status": None
                    if len(free_agent.injuryStatus) == 0
                    else free_agent.injuryStatus,
                    "player_position": free_agent.position,
                }
            )
        return pl.DataFrame(dfs)

    def get_league_competitor(self, con) -> pl.DataFrame:
        # ESPN always reports at least one division, even when the league has no
        # real split: the single entry is a default standings group (often named
        # "A team", "League Standings" or similar) covering every team. A league
        # only uses divisions when ESPN reports more than one.
        has_divisions = len(con.settings.division_map) > 1
        dfs = []
        for competitor in con.teams:
            division_name = (
                con.settings.division_map.get(competitor.division_id)
                if has_divisions
                else None
            )
            division_id = competitor.division_id if has_divisions else None
            dfs.append(
                {
                    "season": con.season,
                    "platform": self.NAME,
                    "league_id": con.league_id,
                    "competitor_id": competitor.team_id,
                    "competitor_abbrev": competitor.team_abbrev,
                    "competitor_name": competitor.team_name,
                    "division_id": division_id,
                    "division_name": division_name,
                }
            )
        return pl.DataFrame(dfs)

    def get_league_matchup(self, con) -> pl.DataFrame:
        """
        One row per competitor per period they have an opponent, with the period
        taken from ESPN's own matchupPeriodId.

        Real matchups only — a bye produces no row here, and league_byes carries
        the complement. The two are disjoint, so a complete competitor-by-period
        grid is a UNION ALL of the pair and its row count is exactly
        competitors x periods.

        This used to enumerate con.teams[].schedule and take the period from list
        POSITION, which is why it had to read league_byes first and splice a None
        into the list: espn_api omits a bye from that list in some seasons, so
        every later entry sat one period early unless padded. Reading
        matchupPeriodId removes the need entirely — a missing bye is simply an
        absent row, nothing shifts, and the write order between this and
        get_league_byes no longer matters.

        Each schedule entry is emitted from both sides, so a competitor appears
        as `competitor_id` in its own row and as `opponent_id` in its opponent's.
        """
        schedule = con.espn_request.get_league().get("schedule") or []

        dfs = []
        for entry in schedule:
            home, away = entry.get("home"), entry.get("away")
            # One-sided entries are byes and belong to get_league_byes.
            if not (home and away):
                continue

            home_id, away_id = home.get("teamId"), away.get("teamId")
            if home_id is None or away_id is None:
                continue

            for competitor_id, opponent_id in ((home_id, away_id), (away_id, home_id)):
                dfs.append(
                    {
                        "season": con.season,
                        "platform": self.NAME,
                        "league_id": con.league_id,
                        "matchup_period": entry["matchupPeriodId"],
                        "competitor_id": competitor_id,
                        "opponent_id": opponent_id,
                    }
                )

        return pl.DataFrame(
            dfs,
            schema={
                "season": pl.String,
                "platform": pl.String,
                "league_id": pl.Int64,
                "matchup_period": pl.Int64,
                "competitor_id": pl.Int64,
                "opponent_id": pl.Int64,
            },
        ).sort("matchup_period", "competitor_id")

    def get_competitor_roster(self, con) -> pl.DataFrame:
        dfs = []
        for competitor in con.teams:
            for player in competitor.roster:
                dfs.append(
                    {
                        "season": con.season,
                        "platform": self.NAME,
                        "league_id": con.league_id,
                        "timestamp": dt.datetime.now(dt.timezone.utc),
                        "matchup_period": con.currentMatchupPeriod,
                        "competitor_id": competitor.team_id,
                        "player_fantasy_id": player.playerId,
                        "player_name": player.name,
                        "player_team": player.proTeam.replace("PHL", "PHI").replace("PHO", "PHX"),
                        "player_injury_status": player.injuryStatus,
                        "player_acquisition_type": player.acquisitionType,
                    }
                )
        return pl.DataFrame(dfs)

    def get_recent_activity(self, con) -> pl.DataFrame:
        dfs = []
        for activity in con.recent_activity(size=50):
            for action in activity.actions:
                dfs.append(
                    {
                        "season": con.season,
                        "platform": self.NAME,
                        "league_id": con.league_id,
                        "timestamp": dt.datetime.fromtimestamp(
                            activity.date / 1000, tz=dt.timezone.utc
                        ),
                        "competitor_id": action[0].team_id,
                        "action": action[1],
                        "player": action[2],
                    }
                )

        df = pl.DataFrame(dfs, schema=_RECENT_ACTIVITY_SCHEMA)

        # Nothing to add, or nothing stored to de-duplicate against yet — either
        # way the anti-join would be against a key set the left frame lacks (or an
        # all-Null right side), so skip it and return the frame as-is.
        if df.is_empty():
            return df

        df_already_done = self.db.read(
            f"SELECT * FROM {self.schema}.recent_activity WHERE season = '{con.season}' AND platform = 'ESPN' AND league_id = {con.league_id}",
        )
        if df_already_done.is_empty():
            return df
        return df.join(df_already_done, on=df_already_done.columns, how="anti")

    # Categories that get stored, keyed by ESPN's own label. Ratio categories
    # are excluded: FG%/FT% are derived from their components downstream, so a
    # stored row would duplicate them — and, unlike a count, could not be summed
    # across matchups. Anything ESPN reports with no mapping is dropped rather
    # than stored under a platform-native name.
    def _stored_categories(self) -> dict:
        rows = self.db.read(
            "SELECT pc.platform_category, pc.nba_category "
            f"FROM {self.schema}.platform_category pc "
            f"JOIN {self.schema}.category_label cl ON cl.nba_category = pc.nba_category "
            f"WHERE pc.platform = '{self.NAME}' AND cl.numerator IS NULL"
        )
        return dict(zip(rows["platform_category"], rows["nba_category"]))

    def _raw_matchup_rosters(self, con, matchup_period: int) -> dict:
        """
        {team_id: [per-player raw stat dict]} for a matchup period, read from
        ESPN's own response rather than through BoxPlayer.

        Needed only for points leagues. BoxPlayer cannot serve them:
        points_breakdown prefers ESPN's `appliedStats`, which are already
        multiplied by the league's weights, and BoxPlayer keeps no copy of the
        raw `stats` it passed over. Storing weighted values as `value` would
        break the one property this table relies on — that a row means the same
        thing whatever format the league runs.

        Mirrors the request League.box_scores makes, so the roster returned is
        the same one; only the parsing differs. Bench and IR slots are skipped
        since they do not score, and only statSourceId 0 (actual, not
        projected) is read.
        """
        scoring_period = (
            con.matchup_ids[matchup_period][-1] if matchup_period in con.matchup_ids else 1
        )
        data = con.espn_request.league_get(
            params={"view": ["mMatchupScore", "mScoreboard"], "scoringPeriodId": scoring_period},
            headers={
                "x-fantasy-filter": json.dumps(
                    {"schedule": {"filterMatchupPeriodIds": {"value": [matchup_period]}}}
                )
            },
        )

        rosters = {}
        for matchup in data.get("schedule", []):
            for side in ("home", "away"):
                if side not in matchup:
                    continue
                roster = matchup[side].get("rosterForMatchupPeriod", {})
                players = []
                for entry in roster.get("entries", []):
                    if POSITION_MAP.get(entry.get("lineupSlotId")) in ("BE", "IR"):
                        continue
                    player = (
                        entry["playerPoolEntry"]["player"]
                        if "playerPoolEntry" in entry
                        else entry["player"]
                    )
                    for stat in player.get("stats", []):
                        if stat.get("statSourceId") == 0:
                            players.append(stat.get("stats", {}))
                            break
                rosters[matchup[side]["teamId"]] = players
        return rosters

    @staticmethod
    def _totals_from_team_stats(team_stats: dict, stored: dict) -> dict:
        """Team totals a category league already reports, per category."""
        return {
            stored[label]: stat["value"]
            for label, stat in team_stats.items()
            if label in stored
        }

    @staticmethod
    def _totals_from_players(players: list, stored: dict) -> dict:
        """
        Team totals summed from the lineup, for a points league.

        UNVERIFIED against live data: the only registered points league is
        2026-27, whose season has not started, so box_scores currently returns
        empty lineups. The stat shape read here (statSourceId 0, raw `stats`
        keyed by ESPN stat id) is what a category league's players carry and is
        not format-specific, but confirm the totals against ESPN's own
        appliedStatTotal before trusting them.
        """
        totals = {}
        for player_stats in players:
            for stat_id, value in player_stats.items():
                label = STATS_MAP.get(str(stat_id))
                if label in stored and value is not None:
                    totals[stored[label]] = totals.get(stored[label], 0) + value
        return totals

    def get_matchup_box_score(self, con, matchup_period: int | None = None) -> pl.DataFrame:
        """
        One row per (competitor, category) for the current matchup period.

        Raw values only. Weighting and comparison are both derived downstream —
        from league_categories.points and category_label — so nothing is scored
        here and the rows are identical in shape whatever format the league runs.

        The two formats take different routes because ESPN hands them different
        shapes, not because the output differs: H2HCategoryBoxScore carries
        team-level cumulativeScore.scoreByStat, while H2HPointsBoxScore carries
        only a team total and a lineup, so its categories have to be summed from
        the players. Presence of the `_stats` attribute is the test, since that
        is the actual difference between the two classes.
        """
        matchup_period = matchup_period or con.currentMatchupPeriod
        box_scores = con.box_scores(matchup_period=matchup_period)
        stored = self._stored_categories()
        rosters = None

        dfs = []
        for box_score in box_scores:
            for side in ("home", "away"):
                competitor = getattr(box_score, side + "_team")
                if competitor == 0:          # bye: no competitor this period
                    continue

                if hasattr(box_score, side + "_stats"):
                    totals = self._totals_from_team_stats(
                        getattr(box_score, side + "_stats"), stored
                    )
                else:
                    if rosters is None:      # one extra request per league, not per team
                        rosters = self._raw_matchup_rosters(con, matchup_period)
                    totals = self._totals_from_players(
                        rosters.get(competitor.team_id, []), stored
                    )

                for category, value in totals.items():
                    dfs.append(
                        {
                            "season": con.season,
                            "platform": self.NAME,
                            "league_id": con.league_id,
                            "matchup": matchup_period,
                            "competitor_id": competitor.team_id,
                            "category": category,
                            "value": value,
                        }
                    )

        return pl.DataFrame(
            dfs,
            schema={
                "season": pl.String,
                "platform": pl.String,
                "league_id": pl.Int64,
                "matchup": pl.Int64,
                "competitor_id": pl.Int64,
                "category": pl.String,
                "value": pl.Float64,
            },
        )

    def get_matchup_result(self, con, matchup_period: int | None = None) -> pl.DataFrame:
        """
        One row per competitor per matchup: the outcome as ESPN reports it,
        not recomputed here.

        `score` is deliberately format-agnostic so that standings read one
        column without branching — categories won for a category league (ESPN's
        own wins + ties/2), the fantasy point total for a points league. The
        won/lost/tied breakdown is category-only and stays null otherwise.

        Note ESPN reports totalPoints as 0.0 for category leagues, so it cannot
        stand in as a general score; each format's own field is read instead.
        """
        matchup_period = matchup_period or con.currentMatchupPeriod
        box_scores = con.box_scores(matchup_period=matchup_period)

        dfs = []
        for box_score in box_scores:
            for side, other in (("home", "away"), ("away", "home")):
                competitor = getattr(box_score, side + "_team")
                if competitor == 0:
                    continue
                opponent = getattr(box_score, other + "_team")

                if hasattr(box_score, side + "_wins"):       # category league
                    won = getattr(box_score, side + "_wins")
                    lost = getattr(box_score, side + "_losses")
                    tied = getattr(box_score, side + "_ties")
                    score = won + tied / 2
                    opponent_score = (
                        None if opponent == 0
                        else getattr(box_score, other + "_wins")
                        + getattr(box_score, other + "_ties") / 2
                    )
                else:                                        # points league
                    won = lost = tied = None
                    score = getattr(box_score, side + "_score")
                    opponent_score = None if opponent == 0 else getattr(box_score, other + "_score")

                dfs.append(
                    {
                        "season": con.season,
                        "platform": self.NAME,
                        "league_id": con.league_id,
                        "matchup": matchup_period,
                        "competitor_id": competitor.team_id,
                        "opponent_id": None if opponent == 0 else opponent.team_id,
                        "score": float(score),
                        "opponent_score": None if opponent_score is None else float(opponent_score),
                        "cat_won": won,
                        "cat_lost": lost,
                        "cat_tied": tied,
                        "result": RESULT_MAP.get(box_score.winner, {}).get(side),
                    }
                )

        return pl.DataFrame(
            dfs,
            schema={
                "season": pl.String,
                "platform": pl.String,
                "league_id": pl.Int64,
                "matchup": pl.Int64,
                "competitor_id": pl.Int64,
                "opponent_id": pl.Int64,
                "score": pl.Float64,
                "opponent_score": pl.Float64,
                "cat_won": pl.Int64,
                "cat_lost": pl.Int64,
                "cat_tied": pl.Int64,
                "result": pl.String,
            },
        )

    def get_league_matchup_dates(self, con) -> pl.DataFrame:
        """
        Start and end dates for every matchup period, derived rather than entered.

        Two inputs, both already available:

        1. A season-level week grid. Week 1 begins on the MONDAY of the week
           containing the regular-season opener (nba.key_dates), and every week
           runs Monday to Sunday. The week containing the All-Star break absorbs
           the one after it, giving a 14-day period.
        2. This league's own `settings.scheduleSettings.matchupPeriods`, which
           maps each matchup period to a list of weeks. That is where per-league
           differences live: in 2025-26 league 95537 groups its playoff rounds as
           [18, 19] and [20, 21] while 24608 plays each week as its own period.
           A period's start is its first week's start, its end its last week's
           end.

        Anchoring week 1 on the Monday rather than on the opener itself is
        deliberate. No single rule fits the openers — 2023-24 and 2024-25 both
        began their week on the Monday BEFORE the opener, while 2025-26 began on
        the opener, a Tuesday — and the Monday form is right for two of the three
        outright. For 2025-26 it moves period 1's start back one day to
        2025-10-20, a date with no NBA games at all, so nothing it contains
        changes.

        Verified against the six hand-entered league-seasons: 2023-24 and 2024-25
        reproduce exactly, and 2025-26's four leagues differ only in that one
        inert day. Three of the six group several weeks into a period, so the
        mapping is exercised, not just the 1:1 case.
        """
        key_dates = self.db.read(
            "SELECT season_type, begin_date FROM nba.key_dates "
            f"WHERE season = '{con.season}'"
        )
        dates = {r["season_type"]: r["begin_date"] for r in key_dates.iter_rows(named=True)}

        if "Regular Season" not in dates:
            # Without an opener there is no grid to build. Raising beats writing
            # an empty frame, because the caller deletes the league's existing
            # rows around this call.
            raise ValueError(
                f"nba.key_dates has no 'Regular Season' row for {con.season} — "
                "add it before deriving matchup dates"
            )

        opener, all_star = dates["Regular Season"], dates.get("All Star")

        settings = con.espn_request.get_league()["settings"]["scheduleSettings"]
        periods = {int(k): v for k, v in settings["matchupPeriods"].items()}

        week_start = opener - dt.timedelta(days=opener.weekday())
        grid = {}
        for week in range(1, max(max(v) for v in periods.values()) + 1):
            week_end = week_start + dt.timedelta(days=6 - week_start.weekday())
            if all_star and week_start <= all_star <= week_end:
                week_end += dt.timedelta(days=7)
            grid[week] = (week_start, week_end)
            week_start = week_end + dt.timedelta(days=1)

        return pl.DataFrame(
            [
                {
                    "season": con.season,
                    "platform": self.NAME,
                    "league_id": con.league_id,
                    "matchup_period": period,
                    "matchup_start": grid[min(weeks)][0],
                    "matchup_end": grid[max(weeks)][1],
                }
                for period, weeks in sorted(periods.items())
            ],
            schema={
                "season": pl.String,
                "platform": pl.String,
                "league_id": pl.Int64,
                "matchup_period": pl.Int64,
                "matchup_start": pl.Date,
                "matchup_end": pl.Date,
            },
        )

    def get_league_byes(self, con) -> pl.DataFrame:
        """
        Periods where a competitor has no opponent, taken from ESPN's own
        matchupPeriodId.

        ESPN represents a bye as a schedule entry carrying only one side: a
        `home` with no `away` (or the reverse). That entry's matchupPeriodId is
        the authoritative period, so byes are read straight off the raw league
        payload rather than inferred.

        The previous implementation compared each competitor's `schedule` LENGTH
        against a scaffold of every period and treated the shortfall as a bye.
        That can only ever attribute a bye to the TRAILING period, so a
        mid-season bye was reported at the wrong index — and since
        get_league_matchup pads a None at that index, the error propagated into
        league_matchup. Every 2025-26 row it produced was two periods late:
        league 24608's bye is at 19 and it said 21, 95537's is at 17 and it said
        19, 1382487116's is at 20 and it said 22, 1966813226's is at 18 and it
        said 20. Verified against the one-sided entries for all ten known ESPN
        league-seasons, four of which have no byes at all.

        con.teams[].schedule is not used here: espn_api omits the bye from that
        list in some seasons and inserts None in others, which is what made a
        length comparison look plausible in the first place.
        """
        schedule = con.espn_request.get_league().get("schedule") or []

        rows = []
        for entry in schedule:
            home, away = entry.get("home"), entry.get("away")
            if home and away:
                continue

            side = home or away
            if not side or side.get("teamId") is None:
                continue

            rows.append(
                {
                    "season": con.season,
                    "platform": self.NAME,
                    "league_id": con.league_id,
                    "matchup_period": entry["matchupPeriodId"],
                    "competitor_id": side["teamId"],
                }
            )

        # Explicit schema so a league with no byes still returns a writable,
        # correctly typed empty frame rather than a shapeless one.
        return pl.DataFrame(
            rows,
            schema={
                "season": pl.String,
                "platform": pl.String,
                "league_id": pl.Int64,
                "matchup_period": pl.Int64,
                "competitor_id": pl.Int64,
            },
        ).sort("matchup_period", "competitor_id")