# Known failures

Runtime failures observed from the scheduled updater, newest first. Each entry
gives the symptom, the code path, the cause, and the fix. `scs.nba.database`
(`__main__.py`) records one `util.update_log` row per object, so a failure is the
object's row with `success = false` plus the run exiting non-zero.

## 2026-10-12 — `nba.injuries`: `TypeError` when no injury report is available

Found in local testing against `postgres`.

- **Symptom**

  ```
  TypeError: '<' not supported between instances of 'NoneType' and 'datetime.datetime'
  ```

  raised inside `nbainjuries.injury.get_reportdata`.

- **Where** — `src/scs_hub/nba.py:219`, `NBAComponent.get_team_injuries`.

- **Cause** — `get_valid_time()` scans the day's report URLs and returns the
  first that validates, but if none do it falls off the end and returns `None`
  implicitly. That value is passed straight to `get_reportdata`, which compares
  it to a datetime and raises.

- **Fix** — return `None` explicitly, check it, and when absent log a warning and
  skip the write, so a dark slate (or a moved nbainjuries URL scheme) degrades
  the object instead of failing the run.

## 2026-10-12 — `nba.team_roster`: `JSONDecodeError` (and two latent crashes behind it)

Found in local testing against `postgres` — the next failures once the `teamid`
bug below was fixed.

- **Symptom**

  ```
  json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
  ```

  raised inside `nba_api`'s `commonteamroster` response parsing. With the loop
  guarded, the same method instead raised:

  ```
  polars.exceptions.ShapeError: unable to append to a DataFrame of width 26 with a DataFrame of width 11
  polars.exceptions.SchemaError: datatypes of join keys don't match - `season`: str on left does not match `season`: null on right
  ```

- **Where** — `src/scs_hub/nba.py`, `NBAComponent.get_team_roster`.

- **Cause** — three separate problems, each hidden until the one before it was
  fixed:

  1. `stats.nba.com` intermittently answers `CommonTeamRoster` with an empty
     body, which `nba_api` surfaces as `JSONDecodeError`. The per-team loop had
     no guard, so one bad team aborted the whole fetch.
  2. `pl.concat(dfs)` is strict and `CommonTeamRoster`'s column set is not stable
     across teams — the same hazard `get_player_info` already documents — so one
     narrower frame raises `ShapeError`.
  3. The traded-player branch concats the raw API frame (26 columns) against the
     stored table (11 columns), and — when the season has no stored snapshot —
     joins a `String` `season` against a `Null`-typed one.

- **Fix**

  1. try/except per team, collect `failed`, `continue`; when no frame survives,
     skip the write (mirrors `get_player_season_stats`).
  2. `pl.concat(dfs, how="diagonal_relaxed")`.
  3. `df = self.db.conform(df, "team_roster", "nba")` before the traded branch,
     and when `df_existing.is_empty()` write the snapshot and return.

## 2026-10-12 — `nba.team_roster`: `ColumnNotFoundError: unable to find column "teamid"`

Observed on the NUC running `./cron.sh daily` (`job_kickoff` `2026-10-11T22:46:30Z`,
image `scs.nba.database:latest`, `DB_CON=cockroach-write`).

- **Symptom**

  ```
  polars.exceptions.ColumnNotFoundError: unable to find column "teamid";
  valid columns: ["team_id", "season", "league_id", "player", "nickname",
  "player_slug", ...]
  Did you mean "team_id"?
  ```

  `nba.team_roster` logged `success = false`; the run exited 1.

- **Where** — `src/scs_hub/nba.py:649`, `NBAComponent.get_team_roster`.

- **Cause** — line 639 runs `.clean_names(case_type="snake")` (registered by
  `janitor.polars`), which normalises nba_api's `TeamID` to **`team_id`**. Lines
  649–650 still use the pre-normalisation spelling `teamid`:

  ```python
  .join(self.ctx.nba_teams, left_on="teamid", right_on="id", how="left")
  .rename({"teamid": "team_id", "abbreviation": "team_slug"})
  ```

  Because `clean_names` already produced `team_id`, the join key `teamid` does not
  exist and the join raises; the `rename` targeting `teamid` is dead too. This is
  deterministic — it fails on every run, not intermittently. (Downstream code
  already assumes the normalised name: line 677 joins on `["season", "team_id",
  "player_id"]`.)

- **Fix**

  ```python
  .join(self.ctx.nba_teams, left_on="team_id", right_on="id", how="left")
  .rename({"abbreviation": "team_slug"})
  ```

## 2026-10-12 — `nba.league_game_schedule`: `JSONDecodeError` from the static schedule CDN

Observed in the same run as above.

- **Symptom**

  ```
  requests.exceptions.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
  ```

  Raised via `requests.models.Response.json`. `nba.league_game_schedule` logged
  `success = false`; the run exited 1.

- **Where** — `src/scs_hub/nba.py:517`, `NBAComponent._upcoming_games`, called
  from `get_game_schedule` (line 616).

- **Cause** — the fetch is unguarded:

  ```python
  request = requests.get("https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json")
  ...
  for game_date in request.json()["leagueSchedule"]["gameDates"]:
  ```

  There is no `timeout`, no `raise_for_status()`, and no handling for a
  non-JSON/empty body, so a bad response from the CDN (empty body, error page,
  or a status that still returns HTML) raises `JSONDecodeError`. `get_game_schedule`
  builds the frame (line 616) before its delete (line 617), so the failure leaves
  `nba.league_game_schedule` untouched — the table is not corrupted, but nothing is
  written for the run.

  Re-observed 2026-10-12 in local testing: the CDN answers with **HTTP 403
  Access Denied** and an HTML body, not JSON. Still 403 with a browser
  `User-Agent` and `Referer`, so the earlier `JSONDecodeError` was the same
  refusal — the endpoint is declining the request, not returning a moved
  payload. The 403 is independent of the request shape, so the guard below is
  what keeps the run alive; it does not restore the missing games.

- **Fix** — guard the fetch: pass `timeout=`, call `raise_for_status()`, and treat
  an empty/non-JSON body as "no upcoming games" — log the `status_code` and the
  first bytes of the body and skip, so one CDN hiccup degrades the object to
  played-only instead of failing it and exiting the whole run non-zero.

  **Fixed** — `_upcoming_games` now wraps the fetch (with `timeout=30`,
  `raise_for_status()`, and `.json()`) in a guard for
  `requests.exceptions.RequestException` and `ValueError`, logging a warning and
  returning a zero-row frame of the object's own schema
  (`_UPCOMING_GAMES_SCHEMA`) so the concat and conform downstream still hold.
