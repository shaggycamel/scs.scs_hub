# Systemd migration plan

- **Status:** proposal
- **Date:** 2026-11-24
- **Repo path:** `scs_hub/docs/SYSTEMD_MIGRATION.md`
- **Scope owner:** SCS ops
- **Related issue:** stale injury status on the league dashboard because the
  daily base snapshot predates the NBA's final injury report and intraday
  builds reuse that base.

## 1. Purpose

Move the production scheduling on the NUC (and the single Jetson dump job) from
Vixie cron to systemd timers. The migration is primarily an operational
reliability change:

- schedule in the timezone that matches the job (UTC or `America/New_York`);
- order dependent jobs instead of relying on fixed minute offsets;
- catch up missed runs after downtime;
- get per-unit status, logs and failure handling;
- control long-running jobs and resource use.

It is **not** a fix for the stale-injury bug by itself. The data-layer fix
(refreshing injury status in the league build) remains a prerequisite for the
evening chain described in phase 2.

## 2. Current state

### 2.1 NUC user crontab

All times are UTC. The host timezone is `Etc/UTC`; Vixie cron ignores
`CRON_TZ`.

| # | Job | Current cron | Command / env |
| --- | --- | --- | --- |
| 1 | Database weekly | Mon 12:00 | `DB_CON=jetson-write nba/database/cron.sh weekly` |
| 2 | Database daily | daily 13:00 | `DB_CON=jetson-write nba/database/cron.sh daily` |
| 3 | Database intraday | 07:00, 16:00, 22:00 | `DB_CON=jetson-write nba/database/cron.sh intraday` |
| 4 | Database late intraday | 03:45 | same command, `intraday` |
| 5 | League dashboard daily | 16:30 | `PROVISION=1 NBA_DB_SECTION=jetson-read NBA_UPDATE_LOG_SECTION=jetson-write fty/league_dash/deploy/cron.sh daily` |
| 6 | League dashboard intraday | 04:05, 08:05, 22:05 | same plus `SKIP_BASE=1`, `intraday` |
| 7 | Model runner weekly | Mon 14:00 | `SCS_MOD_DB_SECTION=jetson-write nba/mod/_runner/cron.sh weekly` |
| 8 | Model runner intraday | 16:15, 22:15 | same, `intraday` |
| 9 | NUC backup copy | daily 14:45 | `nba/database/deploy/backups/nuc_backup.sh` |
| 10 | Backup freshness check | daily 15:15 | `nba/database/deploy/backups/check_backups.py` |
| 11 | Restore drill | 1st of month 15:30 | `nba/database/deploy/backups/restore_drill.sh` |
| 12 | Registry publish | Mon 12:30 | `nba/database/scripts/publish_registry.py` (database venv) |
| 13 | Model dashboard deploy | daily 23:15 | `HF_SPACE_HARDWARE=cpu-upgrade nba/mod/model_dash/deploy/cron.sh daily` |
| 14 | Forex jobs | disabled | out of scope |

The disabled forex lines stay commented out and are not migrated.

### 2.2 Jetson user crontab

| # | Job | Current cron | Command |
| --- | --- | --- | --- |
| 15 | Jetson database dump | daily 14:15 | `nba/database/deploy/backups/jetson_dump.sh` |

The Jetson has systemd 249 and no user timers. Its dump job is in scope as a
small, separate unit (phase 3 or a parallel mini-migration).

### 2.3 Host facts

- NUC: systemd 259, Ubuntu, user `oli` in the `docker` group, `Linger=no`,
  no user timers. Docker 29.1, `uv` in `~/.local/bin`, `rclone` 1.76 in
  `~/.local/bin`, `age` installed. Journal currently uses 2.1 GB.
- Jetson: systemd 249, user `oli` in the `docker` group, one cron job.
- Cron uses `SHELL=/bin/bash`. The scripts set their own `PATH`, open their
  own DB connections, and use `flock` internally.
- Deploy jobs source `~/.config/scs_deploy.env` when stdout is not a terminal.
- Database and backup jobs use `~/.config/scs_hub_credentials.ini` (mode 600).
- Job outcomes are recorded in `util.update_log`; systemd does not replace
  that table.

### 2.4 Pain points this addresses

1. **Fixed offsets are guesses.** The dashboard at 22:05 is five minutes after
   the database at 22:00; if the database run is slow, the dashboard reads old
   data. The model at 16:15 and dashboard at 16:30 have the same race.
2. **UTC-only schedules drift against the NBA day.** The final injury report is
   tied to `America/New_York`; the late intraday snapshot is described as
   "after the last game starts" but is fixed at 03:45 UTC, which moves by an
   hour across DST.
3. **Missed runs are invisible.** `flock -w 3600` either queues or silently
   skips a tick; cron email is not configured; the only record is a log file.
4. **Overlap protection is per script.** Each job has its own lock, but the
   scheduler does not understand the pipeline as a whole.
5. **Schedule drift in comments.** The league dashboard comment says 08:30 and
   22:30 while the crontab runs 08:05 and 22:05.

## 3. Goals and non-goals

### Goals

- One systemd service and one timer per logical job, with the same commands and
  environment as cron.
- Start at boot without a login session, and order after Docker and the
  network.
- Preserve `util.update_log` as the system of record for run results.
- Keep the existing `flock` locks as the cross-cadence safety net.
- Make the evening chain timezone-correct and dependency-ordered.
- Provide a documented, tested rollback to cron.

### Non-goals

- Rewriting the job scripts or the database schedule table.
- Changing the dashboard data model or fixing the injury refresh in this
  plan (that is a separate change in `scs.nba.fty.league_dash`).
- Migrating the Mac development environment.
- Adding high-availability or a second scheduler host.
- Replacing `util.update_log` with journald.

## 4. Design decisions

### 4.1 System units, running as `oli`

Use **system-level units** with `User=oli`, `Group=oli` and
`SupplementaryGroups=docker`:

- they start at boot without `loginctl enable-linger`;
- they can order after `docker.service` and `network-online.target`;
- `journalctl -u` and `systemctl status` are system-wide;
- the scripts still run as `oli`, so `~/.config`, `~/.ssh` and the Docker
  group work as before.

Set `Environment=HOME=/home/oli` because a system service does not inherit a
login environment. Keep `ProtectHome`/`ProtectSystem` unset in the first pass;
Docker and the credential files need access.

If the team prefers user units, see Appendix B for the additional linger and
Docker-wait changes. Do not mix the two models.

### 4.2 One service + one timer per logical job

Split by command and environment, not by cron line:

- database: weekly, daily, intraday (07/16/22), late (03:45);
- league dashboard: daily, intraday;
- model runner: weekly, intraday;
- model dashboard: daily;
- backups: dump (Jetson), copy, check, drill;
- registry publish.

The database intraday and late units share the default
`/tmp/scs.nba.database.lock`, so they still serialize. The same applies to the
dashboard daily/intraday and the model weekly/intraday.

### 4.3 Keep UTC for phase 1, use ET for the evening chain in phase 2

Phase 1 is a mechanical 1:1 migration: same times, same commands, same
environment, UTC. This proves the systemd plumbing without changing behaviour.

Phase 2 introduces `America/New_York` for the game-day evening and late
chains, where the NBA report and the "after the last game" intent are defined
in ET.

### 4.4 Timers use `OnCalendar`, not relative intervals

The schedule is wall-clock and game-day driven. Use `OnCalendar=` with an
explicit timezone suffix (`UTC` or `America/New_York`), `Persistent=true` and
`RandomizedDelaySec` for independent jobs. Do not use `OnBootSec` or
`OnUnitActiveSec` for production cadences.

### 4.5 Logging: journald primary, `util.update_log` for results

Use `StandardOutput=journal` and `StandardError=journal`, with a
`SyslogIdentifier=` per unit. `journalctl -u <unit>` replaces the per-job
`cron.log` for stdout/stderr; `util.update_log` remains the run history.

During the parallel period, optionally mirror to the existing log file with
`StandardOutput=append:` for familiarity. Remove the mirror after cutover.

Journald is already at 2.1 GB. Before enabling all units, set a retention
policy (for example `SystemMaxUse=2G` or `MaxRetentionSec=30d`) or verify the
existing `/etc/systemd/journald.conf` policy.

### 4.6 Keep the script-level locks

Do not remove `flock` from the scripts. Systemd prevents a second instance of
the same unit, but different units (daily vs intraday) and manual runs can
still overlap. The scripts' locks are the only protection for the shared
Docker container name (`update_tables`) and the image-build working tree.

### 4.7 Failures do not restart automatically

Use `Type=oneshot` and `Restart=no`. A timer retry is safer than a restart
loop for jobs that write data. Add `OnFailure=` only after a notifier exists.
Set `TimeoutStartSec` generously and `KillMode=control-group` so Docker and
SSH children are cleaned up on timeout.

### 4.8 Resource controls for the heavy jobs

- Database ingestion: default priority; it is the source of truth.
- League dashboard, model runner, model dashboard: `Nice=10`,
  `IOSchedulingClass=best-effort`, `IOSchedulingPriority=7`.
- NUC backup copy: `IOSchedulingClass=idle` if it competes with the database;
  otherwise best-effort with a low priority.
- Backups and restore drill: no CPU constraint; the restore drill is monthly
  and runs in a throwaway Docker container.

## 5. Unit layout and naming

All names use an `scs-` prefix so `systemctl list-timers 'scs-*'` shows the
whole estate.

```text
scs-db-weekly.{service,timer}
scs-db-daily.{service,timer}
scs-db-intraday.{service,timer}
scs-db-late.{service,timer}
scs-mod-weekly.{service,timer}
scs-mod-intraday.{service,timer}
scs-dash-daily.{service,timer}
scs-dash-intraday.{service,timer}
scs-model-dash.{service,timer}
scs-registry.{service,timer}
scs-backup-jetson.{service,timer}   # Jetson host
scs-backup-nuc.{service,timer}
scs-backup-check.{service,timer}
scs-backup-drill.{service,timer}
```

Proposed repository layout:

- Each repo owns its unit files under `deploy/systemd/` and an `install.sh`
  that copies them to `/etc/systemd/system`.
- `scs_hub` holds this plan and the shared environment-file example
  (`scs_hub/deploy/systemd/scs-systemd.env.example`), because it is the shared
  operations reference.
- Do not symlink unit files from a working tree into `/etc/systemd/system`;
  copy them with `install -m 644`. Re-run the installer after changes.

The shared non-secret environment file lives at
`/home/oli/.config/scs-systemd.env` (mode 600):

```ini
PATH=/home/oli/.local/bin:/usr/local/bin:/usr/bin:/bin
SCS_HUB_CREDENTIALS=/home/oli/.config/scs_hub_credentials.ini
SCS_DEPLOY_ENV=/home/oli/.config/scs_deploy.env
NBA_DB_SECTION=jetson-read
NBA_UPDATE_LOG_SECTION=jetson-write
SCS_MOD_DB_SECTION=jetson-write
```

Job-specific variables stay on the unit (`DB_CON=jetson-write`,
`PROVISION=1`, `SKIP_BASE=1`, `HF_SPACE_HARDWARE=cpu-upgrade`) so the common
file cannot accidentally change another job.

Secrets stay in `~/.config/scs_deploy.env` and
`~/.config/scs_hub_credentials.ini`; never put tokens in unit files or in
`scs-systemd.env`.

## 6. Example units

### 6.1 Database intraday

`/etc/systemd/system/scs-db-intraday.service`:

```ini
[Unit]
Description=SCS NBA database intraday update
After=network-online.target docker.service
Wants=network-online.target
ConditionPathExists=/home/oli/shaggy_camel_sports/nba/database/cron.sh

[Service]
Type=oneshot
User=oli
Group=oli
SupplementaryGroups=docker
Environment=HOME=/home/oli
EnvironmentFile=-/home/oli/.config/scs-systemd.env
Environment=DB_CON=jetson-write
WorkingDirectory=/home/oli/shaggy_camel_sports/nba/database
ExecStart=/home/oli/shaggy_camel_sports/nba/database/cron.sh intraday
TimeoutStartSec=3600
KillMode=control-group
Restart=no
SyslogIdentifier=scs-db-intraday
```

`/etc/systemd/system/scs-db-intraday.timer`:

```ini
[Unit]
Description=SCS NBA database intraday update (07:00, 16:00, 22:00 UTC)

[Timer]
OnCalendar=*-*-* 07:00:00 UTC
OnCalendar=*-*-* 16:00:00 UTC
OnCalendar=*-*-* 22:00:00 UTC
Persistent=true
RandomizedDelaySec=30s
AccuracySec=1s
Unit=scs-db-intraday.service

[Install]
WantedBy=timers.target
```

### 6.2 League dashboard daily

`/etc/systemd/system/scs-dash-daily.service`:

```ini
[Unit]
Description=SCS league dashboard daily build and deploy
After=network-online.target docker.service
Wants=network-online.target
ConditionPathExists=/home/oli/shaggy_camel_sports/nba/fty/league_dash/deploy/cron.sh

[Service]
Type=oneshot
User=oli
Group=oli
SupplementaryGroups=docker
Environment=HOME=/home/oli
EnvironmentFile=-/home/oli/.config/scs-systemd.env
Environment=PROVISION=1
Environment=NBA_DB_SECTION=jetson-read
Environment=NBA_UPDATE_LOG_SECTION=jetson-write
WorkingDirectory=/home/oli/shaggy_camel_sports/nba/fty/league_dash
ExecStart=/home/oli/shaggy_camel_sports/nba/fty/league_dash/deploy/cron.sh daily
TimeoutStartSec=7200
KillMode=control-group
Restart=no
Nice=10
IOSchedulingClass=best-effort
IOSchedulingPriority=7
SyslogIdentifier=scs-dash-daily
```

`/etc/systemd/system/scs-dash-daily.timer`:

```ini
[Unit]
Description=SCS league dashboard daily build and deploy (16:30 UTC)

[Timer]
OnCalendar=*-*-* 16:30:00 UTC
Persistent=true
RandomizedDelaySec=30s
AccuracySec=1s
Unit=scs-dash-daily.service

[Install]
WantedBy=timers.target
```

The other units follow the same shape. The full `OnCalendar` map is in
section 7.

### 6.3 Environment probing before install

Use `systemd-run` to test the environment without installing a unit:

```bash
sudo systemd-run --unit=scs-env-test --property=User=oli \
  --property=SupplementaryGroups=docker --property=WorkingDirectory=/home/oli \
  --property=Environment=HOME=/home/oli \
  --property=EnvironmentFile=-/home/oli/.config/scs-systemd.env \
  /bin/bash -lc 'id; docker info >/dev/null && echo docker-ok; \
  /home/oli/.local/bin/uv --version; ssh -o BatchMode=yes jetson true && echo ssh-ok'
```

## 7. Proposed schedule map

Phase 1 keeps the current times. Multiple `OnCalendar=` lines are used when a
job has several daily slots.

| Unit | `OnCalendar` (phase 1) | Command |
| --- | --- | --- |
| `scs-db-weekly.timer` | `Mon *-*-* 12:00:00 UTC` | `database/cron.sh weekly` |
| `scs-db-daily.timer` | `*-*-* 13:00:00 UTC` | `database/cron.sh daily` |
| `scs-db-intraday.timer` | `07:00`, `16:00`, `22:00 UTC` | `database/cron.sh intraday` |
| `scs-db-late.timer` | `*-*-* 03:45:00 UTC` | `database/cron.sh intraday` |
| `scs-dash-daily.timer` | `*-*-* 16:30:00 UTC` | `league_dash/deploy/cron.sh daily` |
| `scs-dash-intraday.timer` | `04:05`, `08:05`, `22:05 UTC` | `SKIP_BASE=1 ... intraday` |
| `scs-mod-weekly.timer` | `Mon *-*-* 14:00:00 UTC` | `_runner/cron.sh weekly` |
| `scs-mod-intraday.timer` | `16:15`, `22:15 UTC` | `_runner/cron.sh intraday` |
| `scs-backup-jetson.timer` | `*-*-* 14:15:00 UTC` | `jetson_dump.sh` (Jetson host) |
| `scs-backup-nuc.timer` | `*-*-* 14:45:00 UTC` | `nuc_backup.sh` |
| `scs-backup-check.timer` | `*-*-* 15:15:00 UTC` | `check_backups.py` |
| `scs-backup-drill.timer` | `*-*-01 15:30:00 UTC` | `restore_drill.sh` |
| `scs-registry.timer` | `Mon *-*-* 12:30:00 UTC` | `publish_registry.py` |
| `scs-model-dash.timer` | `*-*-* 23:15:00 UTC` | `model_dash/deploy/cron.sh daily` |

## 8. Phase 2: dependency-ordered, ET-aware evening chain

This phase is where systemd earns its keep for the original injury problem.

### 8.1 Companion data change (prerequisite)

In `scs.nba.fty.league_dash`, refresh injury status in the league layer on
every build:

- query `nba.injuries_vw` fresh in `data-raw/_generate_league.R`;
- update the future `inj_status` / `inj_status_pred` columns in
  `dfs_rolling_stats`;
- rebuild `ls_injuries`;
- add tests for a status change between base and league builds.

Without this, an evening dashboard run still reuses the stale base and the
schedule change has no effect.

### 8.2 Evening chain in ET

Replace the fixed `22:00 / 22:15 / 22:05 UTC` chain with a chain scheduled
against the NBA report:

- database intraday at **17:15 ET** (after the 5:00 PM ET final report);
- model intraday after the database;
- league dashboard intraday after the model.

Options:

1. **Three timers with `After=`/`Wants=`.** Hard to get right because a timer
   starts a unit in its own transaction; `After=` alone does not pull in the
   database unit.
2. **One orchestrator service per slot (recommended).** A
   `scs-pipeline-evening.service` runs a small script:

   ```bash
   #!/bin/bash
   set -euo pipefail
   /home/oli/shaggy_camel_sports/nba/database/cron.sh intraday
   /home/oli/shaggy_camel_sports/nba/mod/_runner/cron.sh intraday
   /home/oli/shaggy_camel_sports/nba/fty/league_dash/deploy/cron.sh intraday
   ```

   The timer runs the pipeline at 17:15 ET. The script can stop on the first
   failure (shown) or continue and record the failure per stage. The individual
   services remain available for manual runs.

The same pattern applies to the late chain:

- database late snapshot at **22:45 ET**;
- dashboard intraday at **23:05 ET** (or immediately after the database in a
  pipeline).

This keeps the "after the last game starts" intent stable across DST and
removes the 03:45/04:05 UTC drift.

The daytime chain (16:00 UTC database, 16:15 model, 16:30 dashboard) can stay
in UTC; it is not tied to the final report. The daily/weekly/monthly jobs stay
in UTC.

### 8.3 DST verification

For every ET `OnCalendar`, check both sides of the DST boundary:

```bash
systemd-analyze calendar '*-*-* 17:15:00 America/New_York' --iterations=5
systemd-analyze calendar '*-*-* 22:45:00 America/New_York' --iterations=5
```

Verify the next elapse after the March and November transitions. UTC jobs do
not need this check.

## 9. Migration procedure

### Phase 0: preparation

1. Back up the crontabs:
   ```bash
   crontab -l > ~/crontab.nuc.$(date +%F).bak
   ssh jetson 'crontab -l' > ~/crontab.jetson.$(date +%F).bak
   ```
2. Confirm `sudo`, `docker`, `ssh jetson`, `uv`, `rclone` and `age` work as
   `oli`.
3. Add the unit files and installers to the owning repos on a branch.
4. Create `/home/oli/.config/scs-systemd.env` (mode 600) with the shared
   non-secret values. Do not put tokens in it.
5. Add `scs_hub/docs/SYSTEMD_MIGRATION.md` (this plan) and the shared env
   example.

### Phase 1: install without enabling timers

1. Copy units:
   ```bash
   sudo install -m 644 deploy/systemd/*.service /etc/systemd/system/
   sudo install -m 644 deploy/systemd/*.timer /etc/systemd/system/
   sudo systemctl daemon-reload
   ```
2. Validate:
   ```bash
   systemd-analyze verify /etc/systemd/system/scs-*.service
   systemd-analyze verify /etc/systemd/system/scs-*.timer
   systemctl list-timers --all 'scs-*'
   ```
3. Run each service manually at an off-peak time, with dry-run where
   supported:
   - backups: `DRY_RUN=1` for the copy, `--no-email` for the check;
   - registry: `--dry-run`;
   - model runner: `--dry-run`;
   - dashboard/model dashboard: `DRY_RUN=1 BUILD_ONLY=1`;
   - database: run a normal `daily`/`intraday` at a quiet time.
4. Verify `systemctl status`, `journalctl -u`, the script's own log if kept,
   and the corresponding `util.update_log` rows.

### Phase 2: cut over one job at a time

Do not run cron and systemd for the same job. The scripts' locks protect
against corruption, but the database's `docker rm -f` and the dashboard's
image builds are disruptive.

Suggested order (low risk first):

1. backups (NUC copy, check, drill) and registry publish;
2. database weekly and daily;
3. model runner weekly and intraday;
4. model dashboard;
5. league dashboard daily and intraday;
6. database intraday and late.

For each job:

1. Comment out the cron line(s).
2. `sudo systemctl enable --now scs-<job>.timer`.
3. Wait for the next scheduled run (or start the service manually once).
4. Check `util.update_log`, `journalctl -u`, and the output artifacts.
5. Only then move to the next job.

A quiet maintenance window is strongly preferred for the database and
dashboard cutovers.

### Phase 3: Jetson dump

The Jetson has one cron job. Migrate it separately:

1. Install `scs-backup-jetson.{service,timer}` with `User=oli`,
   `SupplementaryGroups=docker`, `After=docker.service`, `HOME=/home/oli`.
2. Test `sudo systemctl start scs-backup-jetson.service`.
3. Disable the cron line, enable the timer.
4. Verify the dump appears under `/srv/backups` and the NUC copy/check still
   pass.

### Phase 4: decommission cron

1. Remove the old cron lines; keep the backup file in `~/.config` or a repo
   doc.
2. Update the docs listed in section 12.
3. Decide whether to keep the `cron.log` files. If journald is the primary
   log, stop writing them.
4. Run one full week of normal cadence and close the migration.

## 10. Validation and acceptance criteria

- `systemctl list-timers --all 'scs-*'` shows every timer with the expected
  next and last elapse.
- `journalctl -u scs-<unit>` shows a clean start and exit for each run;
  `systemctl status` is `inactive (dead)` with `Result=success` for oneshot
  runs.
- `util.update_log` has one row per job per run, with the expected `source`,
  `job_kickoff`, `cadence` and `row_count`. No duplicate `job_kickoff` rows
  from a single run.
- No overlapping runs: check `ps`/`docker ps` and the `flock` lock files
  during the first week.
- Backups: `backup.log` or journal shows the dump, copy and check; the check
  exits 0; the MEGA daily listing contains the encrypted file; the monthly
  restore drill passes.
- Dashboards: `last_update` matches the run; the league dashboard shows the
  expected injury status; the model dashboard reflects the latest model run.
- Reboot test: reboot the NUC once; all timers are active and the next runs
  happen without a login session.
- DST test: for ET timers, `systemd-analyze calendar` shows the correct next
  elapse after the March and November transitions.
- Rollback is tested once on a non-critical job before the database/dashboard
  cutover.

## 11. Rollback

1. Disable the affected systemd timers:
   ```bash
   sudo systemctl disable --now scs-<job>.timer
   ```
2. Restore the cron lines from the backup:
   ```bash
   crontab ~/crontab.nuc.YYYY-MM-DD.bak
   ```
3. `sudo systemctl daemon-reload`; optionally remove the unit files.
4. Verify the next cron run in `util.update_log` and the job log.

Because the scripts are unchanged, rollback is low risk. Keep the crontab
backup and the old unit files until the migration is signed off.

## 12. Documentation updates

- `scs_hub/docs/SYSTEMD_MIGRATION.md` — this plan.
- `scs_hub/deploy/systemd/scs-systemd.env.example` — shared env template.
- Root `AGENTS.md` — replace the cron references in the scheduling section
  with the unit names and `systemctl`/`journalctl` commands.
- `nba/database/README.md`, `nba/database/deploy/backups/README.md` — replace
  the cron snippets with the unit names and install commands.
- `nba/fty/league_dash/AGENTS.md` and `docs/NUC_DEPLOY.md` — deploy cadence
  and the evening-chain phase.
- `nba/mod/_runner/README.md` — replace the crontab example.
- `nba/mod/model_dash` deploy docs — replace the cron line.
- `nba/database/known_failures.md` — add an entry if a migration issue is
  found and fixed.
- `nba/database/docs/JETSON_CUTOVER_NOTES.md` — note the Jetson dump unit if
  it is migrated.

## 13. Effort and timeline

| Phase | Work | Estimate |
| --- | --- | --- |
| 0 | Inventory, decisions, env file, unit drafts | 0.5–1 day |
| 1 | Unit files, installer, `systemd-analyze` checks | 1–2 days |
| 1 | Manual tests and dry runs | 1 day |
| 2 | Cutover one job at a time, monitoring | 1 week calendar |
| 2 | Evening-chain orchestrator + ET schedules | 1–2 days |
| 3 | Jetson dump | 0.5 day |
| 4 | Docs, cron removal, sign-off | 0.5–1 day |

Roughly **5–8 person-days** over **3–4 weeks** part-time, with the cutover
spread across normal cadences.

## 14. Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Jobs do not start at boot | Use system units with `User=oli`; test a reboot. If user units are chosen, enable `loginctl enable-linger oli`. |
| Docker permission denied | `SupplementaryGroups=docker`; test with `systemd-run`; verify `docker info`. |
| Missing `HOME`/`PATH`/SSH agent | Set `HOME`, `PATH` and `EnvironmentFile`; test `ssh jetson` from the unit environment. The model runner uses `BatchMode` and a key file, not an agent. |
| Missing deploy tokens | The scripts source `~/.config/scs_deploy.env` when stdout is not a terminal; test a real deploy with `DRY_RUN=1` first. |
| Duplicate runs during migration | Cut over one job at a time; disable the cron line before enabling the timer; keep the script locks. |
| `Persistent=true` stampede after downtime | `RandomizedDelaySec=30s`, script locks, and an orchestrator for dependent jobs. |
| Long jobs hit `TimeoutStartSec` | Set 1–2 hours; use `KillMode=control-group`; check Docker/SSH children are gone. |
| DST/timezone mistakes | Explicit `UTC`/`America/New_York`; `systemd-analyze calendar` at both transitions. |
| Journald disk growth | Set `SystemMaxUse`/`MaxRetentionSec` before enabling all units; monitor `journalctl --disk-usage`. |
| Secret leakage in units | Secrets stay in `~/.config` files; units only reference the paths; never `Environment=DOCKERHUB_TOKEN=...`. |
| Schedule drift between docs and units | Unit `Description=` includes the cadence; docs point to `systemctl list-timers` as the source of truth. |

## 15. Open questions

1. **System vs user units.** This plan recommends system units with
   `User=oli`. Confirm that the sudo/install workflow is acceptable.
2. **Journald vs per-job logs.** Recommend journald plus `util.update_log`.
   Keep `StandardOutput=append:` only during the parallel period.
3. **Orchestrator scope.** Recommend an orchestrator for the evening and late
   chains only; keep independent timers for the rest.
4. **Jetson dump.** Include it in this project or defer to a follow-up? It is
   small and can be done in phase 3.
5. **Alerting.** Add `OnFailure=` email/ntfy in phase 1 or phase 4? The model
   runner already emails; the database/backup check write logs. A shared
   notifier is a separate, small project.
6. **Evening time.** Confirm 17:15 ET for the database intraday and 17:30 ET
   for the dashboard after the injury-refresh change is implemented.

## Appendix A: Install and operate

```bash
# Install from a repo checkout (example)
sudo install -m 644 deploy/systemd/scs-db-*.service /etc/systemd/system/
sudo install -m 644 deploy/systemd/scs-db-*.timer /etc/systemd/system/
sudo systemctl daemon-reload

# Enable/disable
sudo systemctl enable --now scs-db-daily.timer
sudo systemctl disable --now scs-db-daily.timer

# Inspect
systemctl list-timers --all 'scs-*'
systemctl status scs-db-daily.service
journalctl -u scs-db-daily.service -n 200 --no-pager
journalctl -u scs-db-daily.service -f

# Manual run
sudo systemctl start scs-db-daily.service

# Validate calendar expressions
systemd-analyze calendar 'Mon *-*-* 12:00:00 UTC'
systemd-analyze calendar '*-*-* 17:15:00 America/New_York' --iterations=5

# Validate units
systemd-analyze verify /etc/systemd/system/scs-*.service
```

## Appendix B: If user units are preferred

Use `~/.config/systemd/user/` and:

```bash
sudo loginctl enable-linger oli
loginctl show-user oli -p Linger
systemctl --user daemon-reload
systemctl --user enable --now scs-db-daily.timer
systemctl --user list-timers --all 'scs-*'
journalctl --user -u scs-db-daily.service -n 200
```

Changes required:

- Remove `User=`, `Group=`, `SupplementaryGroups=` and `Environment=HOME=`
  from the unit files.
- User units cannot order against system units such as `docker.service`.
  Replace `After=docker.service` with an `ExecStartPre` retry that waits for
  `/var/run/docker.sock` and `docker info`, or accept the script's own
  `docker info` failure and retry via the timer.
- Keep `EnvironmentFile=%h/.config/scs-systemd.env`.
- Enable linger before the reboot test.

## Appendix C: Files to create or change

```text
scs_hub/
  docs/SYSTEMD_MIGRATION.md                  # this plan
  deploy/systemd/scs-systemd.env.example     # shared non-secret env

nba/database/
  deploy/systemd/scs-db-*.{service,timer}
  deploy/systemd/scs-backup-*.{service,timer}
  deploy/systemd/scs-registry.{service,timer}
  deploy/systemd/install.sh

nba/fty/league_dash/
  deploy/systemd/scs-dash-*.{service,timer}
  deploy/systemd/install.sh

nba/mod/_runner/
  deploy/systemd/scs-mod-*.{service,timer}
  deploy/systemd/install.sh

nba/mod/model_dash/
  deploy/systemd/scs-model-dash.{service,timer}
  deploy/systemd/install.sh

# Jetson checkout
nba/database/
  deploy/systemd/scs-backup-jetson.{service,timer}
```

The plan itself is the only file this document requires in `scs_hub`; the
unit files belong with the scripts they run.
