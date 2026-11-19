import datetime as dt
import re
import unittest
from unittest import mock

import pandas as pd
import polars as pl

from scs_hub.nba import NBAComponent


class FakeDB:
    def __init__(self, open_rows: pl.DataFrame) -> None:
        self.open_rows = open_rows
        self.read_queries: list[str] = []
        self.deleted: list[tuple[str, str, str]] = []
        self.written: list[tuple[pl.DataFrame, str, str]] = []

    def conform(self, df: pl.DataFrame, table: str, schema: str) -> pl.DataFrame:
        return df

    def read(self, query: str, **kwargs) -> pl.DataFrame:
        self.read_queries.append(query)
        match = re.search(r"team_id IN \(([^)]*)\)", query)
        team_ids = [int(value) for value in match.group(1).split(", ")]
        return self.open_rows.filter(pl.col("team_id").is_in(team_ids))

    def delete_where(self, table: str, schema: str, where: str) -> None:
        self.deleted.append((table, schema, where))

    def write_ordered(self, df: pl.DataFrame, table: str, schema: str) -> None:
        self.written.append((df, table, schema))


class FakeCtx:
    cur_season = "2026-27"
    cur_season_year = 2026
    date_est = dt.date(2026, 11, 17)
    nba_teams = pl.DataFrame(
        {"id": [1, 2], "abbreviation": ["AAA", "BBB"]},
        schema_overrides={"id": pl.Int64},
    )


class FakeRoster:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df

    def get_data_frames(self) -> list[pd.DataFrame]:
        return [self.df]


def roster_frame(team_id: int, player_ids: list[int]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "num": ["" for _ in player_ids],
            "team_id": [team_id for _ in player_ids],
            "player_id": player_ids,
            "player": [f"Player {player_id}" for player_id in player_ids],
        }
    )


def open_rows(*pairs: tuple[int, int]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": ["2026-27" for _ in pairs],
            "team_id": [team for team, _ in pairs],
            "player_id": [player for _, player in pairs],
            "entry_date": [dt.date(2026, 10, 3) for _ in pairs],
            "exit_date": [None for _ in pairs],
        },
        schema_overrides={
            "team_id": pl.Int64,
            "player_id": pl.Int64,
            "entry_date": pl.Date,
            "exit_date": pl.Date,
        },
    )


class TeamRosterFailureTest(unittest.TestCase):
    def run_roster(
        self, db: FakeDB, rosters: dict[int, pd.DataFrame], pre_season: bool = False
    ) -> None:
        def fetch(season: int, team_id: int) -> FakeRoster:
            if team_id not in rosters:
                raise RuntimeError("CommonTeamRoster returned an empty body")
            return FakeRoster(rosters[team_id])

        with (
            mock.patch(
                "scs_hub.nba.nba_ep.commonteamroster.CommonTeamRoster",
                side_effect=fetch,
            ),
            mock.patch("scs_hub.nba.time.sleep"),
        ):
            NBAComponent(db, FakeCtx()).get_team_roster(pre_season=pre_season)

    def test_failed_team_is_not_treated_as_departures(self) -> None:
        db = FakeDB(open_rows((1, 101), (2, 201)))
        rosters = {1: roster_frame(1, [101])}

        with self.assertRaises(RuntimeError) as raised:
            self.run_roster(db, rosters)

        self.assertIn("BBB", str(raised.exception))
        self.assertIn("team_id IN (1)", db.read_queries[0])
        self.assertNotIn("team_id IN (1, 2)", db.read_queries[0])
        self.assertEqual(db.deleted, [])
        self.assertEqual(db.written, [])

    def test_empty_roster_is_a_failure(self) -> None:
        db = FakeDB(open_rows((1, 101), (2, 201)))
        rosters = {1: pd.DataFrame(), 2: roster_frame(2, [201])}

        with self.assertRaises(RuntimeError) as raised:
            self.run_roster(db, rosters)

        self.assertIn("AAA", str(raised.exception))
        self.assertIn("team_id IN (2)", db.read_queries[0])
        self.assertEqual(db.deleted, [])
        self.assertEqual(db.written, [])

    def test_pre_season_partial_fetch_does_not_replace(self) -> None:
        db = FakeDB(open_rows((1, 101), (2, 201)))
        rosters = {1: roster_frame(1, [101])}

        with self.assertRaises(RuntimeError):
            self.run_roster(db, rosters, pre_season=True)

        self.assertEqual(db.deleted, [])
        self.assertEqual(db.written, [])

    def test_transient_failure_is_retried_and_kept(self) -> None:
        db = FakeDB(open_rows((1, 101), (2, 201)))
        rosters = {1: roster_frame(1, [101]), 2: roster_frame(2, [201])}
        attempts: list[int] = []

        def fetch(season: int, team_id: int) -> FakeRoster:
            attempts.append(team_id)
            if team_id == 2 and attempts.count(2) == 1:
                raise TimeoutError("read timed out")
            return FakeRoster(rosters[team_id])

        with (
            mock.patch(
                "scs_hub.nba.nba_ep.commonteamroster.CommonTeamRoster",
                side_effect=fetch,
            ),
            mock.patch("scs_hub.nba.time.sleep"),
        ):
            NBAComponent(db, FakeCtx()).get_team_roster()

        self.assertEqual(attempts.count(2), 2)
        self.assertIn("team_id IN (1, 2)", db.read_queries[0])
        self.assertEqual(db.deleted, [])
        self.assertEqual(db.written, [])

    def test_retry_happens_only_once(self) -> None:
        db = FakeDB(open_rows((1, 101), (2, 201)))
        rosters = {1: roster_frame(1, [101])}
        attempts: list[int] = []

        def fetch(season: int, team_id: int) -> FakeRoster:
            attempts.append(team_id)
            if team_id == 2:
                raise TimeoutError("read timed out")
            return FakeRoster(rosters[team_id])

        with (
            mock.patch(
                "scs_hub.nba.nba_ep.commonteamroster.CommonTeamRoster",
                side_effect=fetch,
            ),
            mock.patch("scs_hub.nba.time.sleep"),
            self.assertRaises(RuntimeError) as raised,
        ):
            NBAComponent(db, FakeCtx()).get_team_roster()

        self.assertIn("BBB", str(raised.exception))
        self.assertEqual(attempts.count(2), 2)
        self.assertIn("team_id IN (1)", db.read_queries[0])
        self.assertEqual(db.written, [])

    def test_successful_teams_still_diff(self) -> None:
        db = FakeDB(open_rows((1, 101), (1, 102), (2, 201)))
        rosters = {1: roster_frame(1, [102, 103]), 2: roster_frame(2, [201])}

        self.run_roster(db, rosters)

        self.assertIn("team_id IN (1, 2)", db.read_queries[0])
        self.assertEqual(len(db.deleted), 1)
        self.assertEqual(len(db.written), 2)
        self.assertEqual(db.written[0][0]["player_id"].to_list(), [101])
        self.assertEqual(db.written[1][0]["player_id"].to_list(), [103])


if __name__ == "__main__":
    unittest.main()
