# Known failures

Runtime failures observed from the scheduled updater, newest first. Each entry
gives the symptom, the code path, the cause, and the fix. `scs.nba.database`
(`__main__.py`) records one `util.update_log` row per object, so a failure is the
object's row with `success = false` plus the run exiting non-zero.

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
  written for the run. Whether this specific occurrence was transient or the
  endpoint has moved is not yet known.

- **Fix** — guard the fetch: pass `timeout=`, call `raise_for_status()`, and treat
  an empty/non-JSON body as "no upcoming games" — log the `status_code` and the
  first bytes of the body and skip, so one CDN hiccup degrades the object to
  played-only instead of failing it and exiting the whole run non-zero.
