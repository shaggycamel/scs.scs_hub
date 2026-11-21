# Team Roster Updater Instructions

Status: 2026-11-11. Branch: `team-roster-instructions`.

The `get_team_roster` fix is already on `main` at `402b344` and is installed in
the NUC database image (`scs.nba.database:latest`, image `adc91645e43d`).

## What changed

`scs_hub.nba.get_team_roster` now:

- leaves `entry_date NULL` on the first snapshot of a season;
- leaves `exit_date NULL` while the player is still on the roster;
- stamps midseason arrivals with `entry_date = date_est`;
- closes pure departures with `exit_date = date_est`;
- replaces the current season on `pre_season=True` instead of appending
  duplicates.

This preserves the intentional null-date convention:

- `entry_date IS NULL` -> already on the roster at season start.
- `exit_date IS NULL` -> still on the roster at season end/open.

## Known limitation

Same-team re-signs between two snapshots are not detected: the diff key is
`(season, team_id, player_id)`, so a player waived and re-signed by the same
team without a snapshot in between looks unchanged. A transaction-log backfill
is the fallback for historical gaps.

## Ordered steps

1. **Review the code diff:**
   ```bash
   git show 402b344 -- src/scs_hub/nba.py
   ```
2. **Add a test harness.** `scs_hub` currently has no test suite. Test at least:
   - first pre-season snapshot -> all `entry_date` null;
   - first in-season snapshot -> all `entry_date` null;
   - midseason arrival -> `entry_date = date_est`;
   - waiver/departure -> old row gets `exit_date = date_est`;
   - trade A -> B -> old row closed, new row opened;
   - same-team re-sign -> documented/known limitation.
3. **Exercise against local Postgres** with a temporary season/team fixture;
   verify no duplicate open rows and no overlapping stints.
4. **Verify the cleaned table** still satisfies:
   - no invalid `exit_date <= entry_date`;
   - no overlapping player-season stints;
   - all played player-games covered.
5. **Keep the image current.** The NUC image is already rebuilt with
   `scs-hub@402b344`; rebuild again only if `scs_hub` changes.
6. **Monitor the next scheduled runs** through `util.update_log`:
   - task `injury_duration.job`/database job success;
   - row counts;
   - no unexpected roster growth.
7. **Rollback:** retain the previous image tag
   (`scs.nba.database:pre-0f19867`) and the `team_roster` backup in `scratch`.

## Related

- Historical backfill: `nba/database/scripts/backfill_team_roster.py`.
- View-name cleanup: `nba/database` branch `views-cleanup`.
- Injury view wiring: `nba/mod/injury_duration` branch `views-fetch`.
