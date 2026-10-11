# fty_dev — points leagues in the fantasy tables

Working area for extending the `fty` schema beyond category leagues. All of it
targets **`fty_dev`** on Postgres; `fty` is read as a source and never written to.

ESPN only. Yahoo is out of scope, and ESPN's vocabulary (`H2H_CATEGORY`,
`H2H_POINTS`, …) is the standard other platforms will be conformed to later.

## Rebuilding the schema

Two steps, and the second is not optional.

`build_fty_dev.sql` defines the schema and is idempotent — it opens with
`DROP SCHEMA IF EXISTS fty_dev CASCADE`. It creates `matchup_box_score` and
`matchup_result` **empty** and does not seed them from `fty.matchup_box_score`,
because those rows are partial (see Verification). A rebuild alone therefore
leaves you with reference data and no matchup history.

```python
from scs_hub.db import Database
from scs_hub.context import Context
from scs_hub.fty import FtyComponent

db = Database(db_con="postgres")
raw = db.engine.raw_connection()
try:
    cur = raw.cursor()
    cur.execute(open("dev/build_fty_dev.sql").read())   # no vars: psycopg2 then
    raw.commit()                                        # leaves the FG% / FT%
finally:                                                # literals alone
    raw.close()

# then refill from ESPN — a few minutes, roughly 250 requests
for year, league_id in [(2023, 1966813226), (2024, 95537), (2024, 1966813226),
                        (2025, 24608), (2025, 95537), (2025, 1382487116),
                        (2025, 1966813226)]:
    ctx = Context(db)
    ctx.cur_season_year = year
    leagues = db.read(
        "SELECT DISTINCT cl.platform, cl.league_id, cp.credentials "
        "FROM fty_dev.customer_league cl "
        "JOIN fty_dev.customer_platform cp "
        "  ON cp.customer_id = cl.customer_id AND cp.platform = cl.platform "
        f"WHERE cl.league_id = {league_id}")
    FtyComponent(db, ctx, "nba", leagues, schema="fty_dev").backfill_matchups()
```

Passing parameters makes psycopg2 read the `%` in `'FG%'` as a placeholder, so
the script has to run with no `vars` argument. To test a change to the script
without disturbing `fty_dev`, run it with `fty_dev` replaced by a scratch schema
name and drop that afterwards.

The customer objects are replicated too, `customer_platform` included, so a
schema is self-contained and can stand in for `fty` wholesale — `FtyComponent`
reads credentials from whichever schema it was given. The trade-off is that the
credentials now exist in two schemas until `fty` is retired.

## The design

Category and points leagues disagree about how a category is scored, not about
what gets recorded. So `fty_dev.matchup_box_score` is long —

```
season · platform · league_id · matchup · competitor_id · category · value
```

— and produces identical rows whichever format the league runs. Scoring is a
join against `league_categories` (weights) and `category_label` (direction,
ratio components), not a second pipeline.

Anything derivable was left out, on the rule that the table holds observations
and scoring definitions but nothing computed:

| not stored | derivation |
|---|---|
| `fantasy_points` | `value × league_categories.points`; the league carries no `pointsOverrides` |
| `result` (W/L/T) | a comparison — needs an opponent, so it belongs in a query |
| `fg_pct`, `ft_pct` | `fgm/fga`, `ftm/fta`; ESPN's own value matches to 5e-9 |
| `is_scored` | redundant once the padding rows were removed — row presence says it |
| `nba_category` | lookup through `platform_category` (see below) |

Dropping the ratio rows is what makes the rest work: **every stored value is
additive**, so any window is `SUM(value)` with no per-category special case.
Averaging matchup percentages instead was off by up to 32 basis points.

That rule is about redundancy **within a grain** — a computed value sitting in
the same row as the facts it derives from. It is not an argument against
aggregates: `matchup_result` is a separate relation at the matchup grain with its
own key, so it stays a table even though its contents are reproducible from the
category grain. Storing a summary of a finer grain is not the same thing as
denormalising a column into the fact table beside its own inputs.

The reproducibility is still useful, as a check rather than a reason to drop it.
Deriving `cat_won/lost/tied` from `matchup_box_score` — comparing each scored
category against the opponent from `league_matchup`, directed by
`higher_is_better` — reproduces ESPN's `cumulativeScore` exactly: all ten
competitors of 2025-26 Let's Get Tropical matchup 20, all three counts. ESPN's
`ineligible` flag is never set (0 across 5,574 scored category-sides over three
leagues), and the only scored categories missing a `result` are the single
in-progress matchup per league. So that derivation is a safe regression test, and
it is the one worth running against a points league once its season starts: the
first matchup where summed lineup totals fail to reproduce ESPN's own
`home_score` is a bug in the lineup aggregation, not a tie to break.

`H2H_MOST_CATEGORIES` is treated as a category league. It maps through
`fty_dev.scoring_format` rather than by rewriting `league.scoring_type`, so that
column stays faithful to ESPN and other platforms become rows, not branches.

## Conforming platform vocabularies

Two things a platform names in its own way, and one mapping table each. Adding a
platform means inserting rows; no schema change and no code branch.

| platform says | table | conforms to |
|---|---|---|
| `H2H_CATEGORY`, `H2H_POINTS`, … | `scoring_format` | `category` / `points` |
| `PTS`, `3PM`, `FG%`, … | `platform_category` | `nba_category` |

`category_label` was doing both jobs with one `fty_category` column — ESPN's
abbreviation *and* the join key for `league_categories.category` — which
silently assumed every platform spells a category the same way. It happens to
hold for Yahoo's nine, but only by luck. So:

- **`category_label`** is now the conformed, platform-neutral vocabulary:
  `nba_category` (PK), `fmt_category`, `display_order`, `higher_is_better`,
  `numerator`, `denominator`.
- **`platform_category`** is `(platform, platform_category) → nba_category`,
  unique both ways, with an FK onto the vocabulary.

`league_categories.category` stays the platform's own label, as fetched, and
conforms on read. `fty_categories_vw` joins through the map with an inner join,
so an unmapped label drops a row rather than silently propagating a
platform-native name — check `league_categories` row count against the view
after adding a platform.

ESPN's vocabulary is the standard, and it is complete: across every league with
box scores, `scoreByStat` returns nothing outside the 15 labels in the map.

Left for later: if a second **sport** is added, the vocabulary needs a sport
dimension (`nba_category` is NBA-specific by name and content) and
`platform_category` becomes `(sport, platform, platform_category)`. Not built.

## Reference-data repairs

The build applies four fixes, all sourced from ESPN rather than inferred:

- **24 padding rows dropped.** `FGM/FGA/FTM/FTA` were never in ESPN's
  `scoringItems` for these category leagues. They had been added so the box
  score fetch — which reads its stat list from `league_categories` — would still
  pull the FG%/FT% inputs. Unnecessary: `scoreByStat` returns 13 keys against 9
  scoring items. Scoped to `points IS NULL` so the points league, where those
  four are genuine scoring items, is untouched.
- **2024-25 Let's Get Tropical restored.** It held only the four padding rows;
  its nine real categories were gone and `fty.league` had no row for the season.
- **`scoring_type` corrected** on 2023-24 Let's Get Tropical and 2025-26 Paid In
  Full — both are `H2H_MOST_CATEGORIES`, stored as `H2H_CATEGORY`.
- **Keys added** that `fty` never had: PKs on `category_label`,
  `platform_category`, `league_categories`, `matchup_box_score` and
  `matchup_result`, plus the FK from `platform_category` onto the vocabulary.

All ten league-seasons now match ESPN's live `scoringItems` exactly.

## Verification

The check that matters is whether the stored values agree with ESPN's own
adjudication. Deriving the category record from the box score and comparing it
against `matchup_result` (fetched separately from ESPN):

| source of box score values | rows compared | disagreements with ESPN |
|---|---|---|
| `fty.matchup_box_score` | 1,342 | **262** |
| `fty_dev.matchup_box_score` (refetched) | 1,534 | **0** |

`fty`'s historical captures are unreliable — poorly formatted and in places just
wrong — so they are not worth reconciling or explaining. The build script does not
seed from them; `backfill_matchups()` refetches from ESPN instead, and that output
agrees with ESPN on every row.

`fty_dev.fty_matchup_box_score_vw` therefore no longer matches
`fty.matchup_box_score`, by design.

### Zero attempts are 0%, not undefined

`fg_pct` was initially `fgm / NULLIF(fga, 0)`, which makes a 0-attempt matchup
NULL and drops the category from any comparison. ESPN instead reports
`FG% = 0.0` with `result: LOSS`. Confirmed on 2023-24 matchup 20, where one
competitor posted an all-zero line (a forfeit): ESPN scored it 1-8, the NULL
version derived 1-6. Both ratio expressions now coalesce to 0, which is what
took the disagreement count to zero.

## The handler

`FtyComponent` and `FtyHandler` now take a `schema` argument (default `"fty"`),
so the same code targets either schema:

```python
f = FtyComponent(db, ctx, "nba", leagues, schema="fty_dev")
f.get_matchup_box_score()
f.get_matchup_result()
```

Credentials are the one thing still read from `fty` regardless.

`get_matchup_box_score` returns long rows and takes different routes per format,
because ESPN hands them different shapes — not because the output differs:

- **category** — `cumulativeScore.scoreByStat` already holds team totals per
  category, so they are read straight off it.
- **points** — `H2HPointsBoxScore` has no `home_stats` at all, so totals are
  summed from the lineup, reading ESPN's raw response rather than `BoxPlayer`.
  `BoxPlayer.points_breakdown` cannot be used: it prefers `appliedStats`, which
  are already multiplied by the league's weights, and it keeps no copy of the raw
  `stats`. One extra request per league, not per team.

Which categories get stored comes from `platform_category` joined to
`category_label`, so a label ESPN reports with no mapping is dropped rather than
stored under a platform-native name, and ratio categories are excluded.

`get_matchup_result` reads ESPN's own outcome rather than recomputing it:
`cumulativeScore.wins/losses/ties` for a category league (score = wins +
ties/2), `home_score` for a points league. `totalPoints` is **not** usable as a
general score — ESPN reports it as 0.0 for category leagues.

### Verified on live data

Against 2025-26 Let's Get Tropical, matchup 20:

- 110 long rows, 10 competitors × 11 stored categories, ratios correctly absent
- re-running replaced rows rather than duplicating them (110 → 110)
- **derived FG% matches ESPN's own reported FG% to 5e-9 for all ten
  competitors** — the check that matters, since the stored percentage was dropped
- `matchup_result`: reciprocal pairs, complementary scores, every
  `cat_won + cat_lost + cat_tied` equal to the league's 9 scored categories

## Coverage

Backfilled across every league-season that has played games. `matchup_result`
row counts are exactly `matchups × team_count` in all seven:

| season | league | matchups | box score rows | result rows |
|---|---|---|---|---|
| 2023-24 | Let's Get Tropical | 21 | 1,848 | 168 |
| 2024-25 | Tucked | 20 | 2,640 | 240 |
| 2024-25 | Let's Get Tropical | 24 | 2,112 | 192 |
| 2025-26 | Paid In Full | 21 | 3,276 | 252 |
| 2025-26 | Tucked | 19 | 2,508 | 228 |
| 2025-26 | $100 8 Cat | 22 | 2,640 | 264 |
| 2025-26 | Let's Get Tropical | 20 | 2,200 | 200 |

17,224 box score rows, 1,544 results over 147 league-matchups. Zero score
mismatches; the 10 rows with a null `result` are exactly the 10 byes.

2024-25 Let's Get Tropical had no box scores at all before this — the wide table
never held any — so its 24 periods came from ESPN fresh.

2026-27 is empty because the season has not started.

`backfill_matchups()` drives the period list from `con.matchup_ids`, capped at
`currentMatchupPeriod`. That map comes back **empty** for 2023-24, so
`currentMatchupPeriod` is the fallback — without it that season silently
backfilled nothing.

## Still open

**The points-league path is written but unverified.** The only registered points
league is 2026-27 National Basketball Cup, and its season has not started —
`box_scores()` returns `H2HPointsBoxScore` objects with `home_score: 0`, empty
lineups and `winner: UNDECIDED`. Two things to confirm once games are played:

- that summed lineup totals agree with ESPN's own `appliedStatTotal`
- that the bench/IR filter is right. Only non-bench slots are summed, on the
  assumption bench players do not score; verify against the league's settings.

A related caveat that did **not** bite for category leagues: `BoxPlayer`
overwrites `points_breakdown` on every entry of a player's `stats` array,
keeping the last. For a category league there is only one entry
(`statSourceId=0`, `statSplitTypeId=5`), so nothing is lost. A points league may
carry projections alongside actuals, which is why the raw path filters to
`statSourceId == 0` explicitly.

Yahoo's handler was left alone beyond the schema threading; it is out of scope.

## Replacing fty

`fty_dev` is intended to replace `fty`, not sit beside it. What that needs:

**Done**

- customer objects replicated (`customer`, `customer_league`, `customer_platform`)
  with primary keys, and `FtyComponent._registered_leagues` now reads credentials
  from its own schema rather than a hardcoded `fty`
- `util.update_schedule` row added for the new method:
  `fty | matchup_result | fty.get_matchup_result`

**Outstanding**

- **Unpause that row.** It is inserted with `pause = true` on purpose. Nothing in
  this repo reads `util.update_schedule`, so an external runner drives it, and
  `fty.matchup_result` does not exist yet — an unpaused row would have that runner
  calling a method against a schema with no such table. Flip it when the schemas
  swap.

  Daily is the right cadence and needs no special handling. `get_matchup_result`
  defaults to `con.currentMatchupPeriod` and scopes its delete to that period, so
  each run refreshes the live period in place. `util.update_log` shows the existing
  job running daily at ~15:06 UTC, and under it periods 1-15 of 2025-26 came out
  exactly right — refetching closed periods on a schedule is not needed.
  `backfill_matchups()` stays a manual tool for seeding and repair.

- `league`, `league_categories`, `league_competitor` and `league_matchup_dates`
  need no `update_schedule` rows: they are derived once at the start of a season.
- **Five views were never ported**, and belong to the league dashboard (scs.nba.fty.league_dash) rather
  than here: `fty_base_vw`, `fty_free_agents_vw`, `fty_league_schedule_vw`,
  `fty_recent_activity_vw`, `fty_team_roster_schedule_vw`. None of them touch
  anything that changed — between them they read only `league`,
  `league_competitor`, `free_agents`, `league_matchup`, `league_matchup_dates`,
  `competitor_roster` and `util.conformed_player_id`, all identical in `fty_dev`.
  They port verbatim. See the contract note below for what the dashboard does
  need to know.
- **`util.table_column_order` needs nothing**, but worth knowing why: only
  `fty.competitor_roster` is registered, and `get_competitor_roster` is the sole
  method using `write_ordered`. Everything else calls `db.write`, which never
  consults the registration, and `conform()` passes an unregistered table through
  untouched. Both new writers build frames from an explicit `pl.DataFrame` schema,
  so their column order is already deterministic.

## Contract changes for the league dashboard

The five dashboard views port across unchanged. Three things do affect the
dashboard, and are worth reading before the schemas swap.

**`fty_categories_vw` changed shape.** Six of eight columns are unchanged;
two are gone:

| gone | replacement |
|---|---|
| `h2h_cat` | row presence — after the reference-data repair, `league_categories` holds exactly ESPN's scoring items, so a row existing *is* the flag |
| `fty_category` | renamed `platform_category`, since the label is now per-platform |

Added: `points`, `higher_is_better`, `numerator`, `denominator`, `is_ratio`,
`scoring_type`, `scoring_format`. A consumer filtering on `h2h_cat = true` should
just drop the filter.

**`fty_matchup_box_score_vw` keeps all 22 columns** and needs no query changes.
One behavioural difference: `fg_pct` and `ft_pct` are now derived from their
components and come back as `0` rather than `NULL` when there were no attempts,
matching what ESPN itself reports. Anything special-casing a null percentage can
stop.

**Yahoo is absent.** `fty` carries one Yahoo league-season; `fty_dev` carries
ESPN only. Any view that showed Yahoo leagues will show fewer rows. That was
deliberate, but it is a visible change to the dashboard rather than an internal
one.

Also gone: `league_schedule_RETIRED`.

There is one genuinely new view worth surfacing in the dashboard —
`matchup_category_vw`, one row per competitor per matchup per scored category,
carrying the value, the weight and the direction. It is the natural source for a
per-category matchup breakdown in either league format.

# Player identity (util.player / util.player_source_id)

Replaces the wide, name-keyed `util.conformed_player_id`. **Built and verified
against local Postgres; the old table is left in place untouched** so the five
un-ported dashboard views keep working until they are refabricated.

## Why

`conformed_player_id` held one column pair per platform (`{source}_id`,
`{source}_name`) with no primary key, no unique constraint and no index. A new
source could only arrive by matching a name, and a missed match was appended as a
new player: 1158 rows held 1128 distinct names, and **all 30 duplicates were one
player split in two** — a nba/espn/yahoo row plus a statyx-only row with a null
`nba_id` (Cameron Payne, Markelle Fultz, Seth Curry, Dalano Banton, …). Nothing
about the shape could detect it, because there was no key to conflict on.

`is_active` was worse: a stored boolean **no code in this repo ever wrote**, with
no `util.update_schedule` row. It had drifted to 124 players with a 2025-26
roster row not flagged, 29 with no crosswalk row at all, and 38 flagged active
sitting on no roster.

## Shape

```
util.player            player_key PK, conformed_name, needs_review
util.player_source_id  platform, source_id, source_name, player_key
                       PK (platform, source_id)
```

Keying the mapping on `(platform, source_id)` makes the split inexpressible, and
makes the build idempotent by virtue of the key rather than the matcher being
right. `conformed_name` is deliberately **not** unique — genuine namesakes exist,
and the matcher declines to guess rather than the constraint forbidding them.

Platform vocabulary is lowercase in `util.*` (`nba`, `espn`, `statyx`, `yahoo`);
`fty.*` stores `ESPN`/`Yahoo`, so reads of those tables `lower()` it.

## Activity is derived, not stored

Neither table has an `is_active` column. `util.active_player_vw` derives it per
season from tables the daily jobs already refresh, so it cannot go stale:

| platform | source |
|---|---|
| nba | `nba.player_box_score` ∪ `nba.team_roster` |
| statyx | `statyx.player_info` |
| espn | `fty.free_agents` ∪ `fty.competitor_roster`, current season only |

The nba union matters. `team_roster` is a **sampled snapshot** — it records who
the job saw when it ran, so a 10-day contract signed and expired between runs
never appears. `player_box_score` is a **complete event log**: nobody plays
without landing in one. For 2025-26 the box scores hold 710 players against the
roster's 645, and 72 of the 710 have no roster row at all. The roster still earns
its place, because early in a season the event log is nearly empty.

`ctx.active_ids(platform, season=None)` reads the view, cached per
(platform, season). It replaces `ctx.active_players`, and 12 call sites lost
their `.drop_nulls()` compensation with it.

Two things the view is deliberately **not**:

- It is not "currently on an NBA roster". `team_roster` cannot answer that:
  spells only close on a **trade**, never when a player is cut or leaves the
  league, so 2024-25 finished with 583 of 686 rows still open months later.
  `exit_date IS NULL` means "latest team assignment". Answering the real question
  needs a change to `get_team_roster` — close any open spell whose player is
  absent from the fresh snapshot — and nothing needs it yet.
- It is not restricted to resolved players. The join to `player_source_id` is a
  LEFT join, so an unmapped source id still drives a fetch loop and a mid-season
  arrival is fetched on the first run that sees them.

## Two source-data problems this surfaced

**Bogus nba player ids.** NBA's feed assigns ids to opposition players in
pre-season exhibitions against international clubs. 280 of them sit in a block
starting at 196,294,083 while real ids top out at 6,664,001, and the directory
view guards on that gap.

**The guard is a heuristic and leaks, deliberately.** About 600 further
pre-season-only participants carry ordinary-looking ids (1, 7020, 27001-27011,
42824 "Chris Goulding" of Melbourne United) and get through. Two tidier rules
were tested against the data; both are worse:

| rule | why it loses |
|---|---|
| `Pre Season` only → drop | loses 34 real players per season who appeared in no other game type and never hit a roster snapshot — Victor Oladipo, Delon Wright, Frank Kaminsky, Jalen McDaniels in 2025-26 |
| must exist in nba_api's static universe | loses 46 real players with real minutes; that bundled list lags, and Jahmai Mashack played 40 regular-season games and 661 minutes in 2025-26 without appearing in it |

So the residue stays and a handful of non-NBA names per season reach
`ctx.active_ids('nba')`. That is the cheaper error: excluding a real player loses
his stats silently, while including a fake one is visible in a log.

**Both nba per-player loops are now guarded for it**, and the failure modes were
not what a try/except catches. Measured against the live API with id 42824
("Chris Goulding", Melbourne United):

| method | what a non-NBA id actually does | guard |
|---|---|---|
| `get_player_season_stats` | returns a **0-row** frame, no exception; pandas types every column of it as object, so `PLAYER_ID` arrives String against a real player's Int64 and `pl.concat` raises `SchemaError`, losing the whole run | skip empty frames, log them at INFO as "not NBA players"; plus try/except → `continue` for genuine API errors, and skip the write when nothing was fetched |
| `get_player_info` | returns a **1-row** frame 33 wide — `CommonPlayerInfo` omits `SUPPLEMENTAL_STATUS` for some ids, so a plain concat raises `ShapeError`; then `height` is `""`, which splits to `[""]` and a strict Float64 cast raises | `concat(how="diagonal_relaxed")` as `get_game_schedule` already does, and `strict=False` / `null_on_oob=True` on the height casts, matching how `weight` was already handled |

`get_player_season_stats` keeps a strict concat — its frames are a consistent 27
wide, and the empty-frame guard is what that loop needs.

Note `get_player_info` does still write a row for such an id, with null
`height_cm`/`weight_kg`. That is consistent with erring toward inclusion, and
visible rather than silent.

**Names disagree across sources on accents, punctuation and whitespace.** nba's
"Egor Dëmin" is espn's "Egor Demin"; espn's "P.J. Hairston" is nba's "PJ
Hairston"; `nba.team_roster` carries "Norris  Cole" with a double space. Matching
raw strings recreated the split it was meant to end — a first pass produced 20
duplicated players. Hence `norm_name()`, which every match goes through.

> **The fold is Python now, not SQL.** `util.norm_name` (a `translate()` list
> plus `regexp_replace`) was replaced by `norm_name()` in
> `src/scs_hub/utility.py`. The SQL version silently DELETED any diacritic it
> did not name (`ć ā đ ņ Ş ū …`), so "Boban Marjanović" folded to `bobanmarjanovi`
> against espn's `bobanmarjanovic` and split the player. The Python version uses
> `unicodedata.normalize('NFKD')`, which needs no list, and folds the letters
> NFKD does not decompose (`ø æ đ`) via a small table. Matching moved with it —
> `conform_player_ids()` now folds in Polars — so the pipeline needs **no
> user-defined function and runs identically on Postgres and CockroachDB**. The
> `util.norm_name` function and its functional index `player_norm_name_ix` are
> dropped. `check_player_identity()` carries the fold-based `colliding_names`
> check the SQL view cannot; the view keeps the two DB-native checks.

## The matcher

`UtilComponent` sits on the hub alongside the other three, because util.* belongs
to no single domain and every component reads it through `ctx.active_ids`:

```python
hub.util.conform_player_ids()   # the only call; safe to repeat, ~5s either way
```

**One method, no season argument, and that is a measured decision rather than a
simplification.** An earlier version split it into a cur_season call and a
`backfill_player_ids()` for all seasons, on the assumption that scoping was
cheaper. It is not: both forms take ~5s, because the time goes on scanning the
box scores through `player_directory_vw`, not on the work. And scoping is
actively harmful — it permanently strands any id whose most recent directory
season is not the current one, which is what happens at every season rollover to
an id seen late in the old season and not yet resolved. Verified with yahoo 5642
(last seen 2024-25): unmapped, the scoped call resolved 0 and left it in the
backlog, where it would have sat forever. The unscoped call resolves it in 5.0s.


`hub.util.conform_player_ids()` — the same two moves, now done in Polars on
`norm_name()` rather than in SQL joins: mint a player for any unmatched norm
nobody owns, then attach every unmatched id to the player holding that norm.
No staging table and no upsert. Re-running is a no-op because the attach is what
empties the view the mint reads. Doing it in Python is what removed the
`util.norm_name` UDF the SQL joins depended on, and with it the accent bug.

Matching is normalised-exact, **never fuzzy** — fuzzy auto-linking is what caused
the original 30 splits. A name matching more than one player is left unmatched
rather than guessed at. Genuine platform renames ("Bub Carrington" → "Carlton
Carrington") stay in the backlog for `review_player_identities()` and a human.

## The LLM review path

The deterministic matcher handles everything it can; what it cannot — renames,
aliases, namesakes it declines to guess — goes through a local model and a
staging table, never straight into the identity tables.

```
review_player_identities(origin='backlog')   →  util.player_identity_review  →  apply_player_identity_reviews()
review_player_identities(origin='audit')     →  (pending/accepted)            (the only writer)
```

- **`origin='backlog'`** reviews `util.unmatched_player_source_vw` — the ongoing
  path, run after the daily source jobs. **`origin='audit'`** reviews ids that are
  already mapped but inconsistent (the one-time backfill); the backlog cannot see
  those.
- Every proposal is written to the staging table; `apply_player_identity_reviews`
  is the only writer to `util.player` / `util.player_source_id`. **`link` and
  `new`** auto-apply at ≥ 0.9 confidence *only when the name folds to the target's*;
  **`merge` and renames always require `status='accepted'`** — that is what keeps
  genuine namesakes (Jameer Nelson Sr/Jr) apart. Re-runs are idempotent on
  `input_hash`.
- The model sees only a **shortlist** of players sharing a folded name token with
  the batch, not all ~2.7k — sending the whole table is slow and noisy. Suffix
  tokens (jr/ii/iii) are ignored.

Configure the model with an `[ollama]` section in `credentials.ini`:

```ini
[ollama]
host    = http://<jetson-ip>:11434
model   = <chat-model-tag>
timeout = 600
think   = false
```

Env vars `OLLAMA_HOST` / `OLLAMA_MODEL` / `OLLAMA_TIMEOUT` / `OLLAMA_THINK` override
the section. Defaults are `http://localhost:11434`, `llama3.1`, 600s, false. The
model must be a **chat/instruct** model — an embedding model (e.g.
nomic-embed-text) cannot produce proposals. `think = false` matters for reasoning
models: without it a local one spent ~850 hidden tokens per three candidates at
~11 tok/s.

## Runbook

Almost all of this is one-way and automatic; the only routine human step is
clearing a merge/rename queue that is usually empty. `[auto]` is unattended, ✋
is a human.

```
DAILY (after the source jobs) — one call, fully automatic
════════════════════════════════════════════════════════════
 [auto]  source jobs write nba.* / statyx.* / fty.*
            │
 [auto]      ▼
         sync_player_identity()
            │
            ├─(1) conform_player_ids()      fold names in Python, attach exact
            │                                matches, mint genuinely new names
            │
            ├─(2) backlog empty?  ── yes ──► skip the model entirely (most days)
            │        │ no
 [auto]      │        ▼
            │     review_player_identities(origin="backlog")
            │        └─ asks the model, writes proposals to
            │           util.player_identity_review   (STAGING ONLY)
            │
            ├─(3) apply_player_identity_reviews()
            │        ├─ link / new, same-name, >=0.9  ──► applied automatically
            │        └─ merge / rename  ──► LEFT PENDING (see ✋ below)
            │
            └─(4) check_player_identity()   expect 0 / 0 / 0
```

```
THE ONLY ROUTINE HUMAN STEP  ✋   (usually empty; a few times a season)
════════════════════════════════════════════════════════════
 ✋  SELECT * FROM util.player_identity_review WHERE status='pending';
 ✋  accept: UPDATE ... SET status='accepted';   reject: SET status='rejected';
         │
         ▼
     the next sync_player_identity() applies the accepted ones.
     (This is the Jameer Nelson Sr/Jr safety: the model cannot merge two people.)
```

```
ONE-TIME / DEPLOY — not part of the daily loop
════════════════════════════════════════════════════════════
 ✅ audit pass (review_player_identities(origin="audit"))
 ✅ hand corrections applied; postgres and cockroach aligned at 2725 / 5070
 ✅ util.player_identity_review + util.player_identity_check on both DBs
 ✋ once, all infra: push scs-hub, bump scs.nba.database's uv.lock and
    rebuild the image, and set the cockroach sections' dialect=cockroachdb
    on the NUC. Until then the runner logs a warning and skips (2)/(3).
```

The mental model is three lines: **automatic** (source jobs → `sync_player_identity()`
runs itself every day); **human, rare** (clear the merge/rename queue when non-empty);
**one-time, done** (audit, cockroach alignment, deploy).

One call does the routine, after the jobs that write the source tables:

```python
hub.util.sync_player_identity()
# {'backlog': 0, 'staged': 0, 'applied': 0,
#  'checks': {'players_owning_nothing': 0, 'backlog': 0, 'colliding_names': 0}}
```

It runs `conform_player_ids()`, and **only if the backlog is non-empty** runs
`review_player_identities(origin='backlog')`, then `apply_player_identity_reviews()`
and `check_player_identity()`. Nothing destructive happens on its own: the review
only stages, and apply will not merge or rename without a human's `status='accepted'`.

**Cadence: daily, right after the source jobs.** It is a no-op most days — the
deterministic matcher is seconds, and the model is only called when
`util.unmatched_player_source_vw` is non-empty, which is a few rows a season.
There is deliberately no `util.update_schedule` row: that table has no cadence
column and the runner drives everything daily, and the daily jobs are already the
detector. With `sync_player_identity()` the same is true for the review step.

The one thing a human still does is clear the merge/rename queue when it is not
empty:

```sql
-- look
SELECT review_id, source_name, proposal, target_player_key, reason
FROM util.player_identity_review WHERE status='pending';
-- accept the ones you agree with, then
UPDATE util.player_identity_review SET status='accepted' WHERE review_id IN (...);
```

`sync_player_identity()` picks accepted rows up on its next run. Reject the rest
with `status='rejected'`.

One-time (already done on this data): `review_player_identities(origin='audit')`
to find ids that are *already* mapped but wrong — the backlog cannot see those.

Rebuild from scratch: `build_player_identity.sql` →
`build_player_identity_views.sql` → `conform_player_ids()` →
`seed_player_identity_review.sql` + `apply_player_identity_reviews()` for the
hand corrections. The retired-crosswalk seed that used to sit between the DDL and
the views has been removed, so a directory-only rebuild is incomplete (see
Outstanding).

## Verified state

Seeded from the 1158 old rows, then the full history resolved:

| | |
|---|---|
| `util.player` | 2737 (1609 `needs_review`) |
| `util.player_source_id` | 5070 |
| normalised name collisions | **0** |
| players owning no mapping | **0** |
| unmatched backlog | **0** |
| 2025-26 coverage | nba 683/683, statyx 703/703, espn 1098/1098 |

`conform_player_ids()` run twice in a row resolves 2199 then 0. Spot-checked:
Cameron Payne holds all four platform ids on one key; Egor Dëmin holds **both**
of ESPN's duplicate ids (5175643, 5243213); the bogus `nba:196294141` for Norris
Cole is gone.

The 1610 `needs_review` rows are mostly legitimate — the box scores reach back to
2009-10, so most are historical players the old crosswalk never held. They are
flagged, not trusted.

## The backlog view is one row per source id, and that matters

`unmatched_player_source_vw` collapses with `DISTINCT ON (platform, source_id)`,
preferring the most recent season's spelling. Without it the two statements of
`conform_player_ids` read different grains: one source id can reach the directory
under several names — nba id 1641756 is "Mike Miles" in the box scores and "Mike
Miles Jr." on the roster — so the minting statement created a player per spelling
while the mapping statement only ever mapped the id once, leaving a player owning
nothing. Caught by asserting `players owning no mapping = 0`, which is worth
keeping as a check after any run.

## What the retired fuzzy matcher was for

The fuzzy matcher (`name_match`, now deleted) was **never** used by
`conform_player_ids` and should not be. Run by hand over the 181 players that
hold no nba id, it returned a "close match" for 148 of them and most were
nonsense — "Aday Mara" → "Cody Martin", "Jayden Nunn"
→ "Jalen Brunson", "Cameron Boozer" → "Carlos Boozer" (his father). Auto-linking
on that output is how the original 30 splits happened.

Most of those 181 are not splits at all: statyx's directory carries college and
draft-prospect players who have no NBA id yet (AJ Dybantsa, Cameron Boozer,
Braden Smith, Aday Mara).

It did earn its keep on one systematic class — pairs differing only by a
generational suffix, which `norm_name` does not reconcile:

| | |
|---|---|
| merge | Billy Garrett / Billy Garrett Jr. · Boo Buie / Boo Buie III · Joel Berry / Joel Berry II · Kevin Knox / Kevin Knox II · Michael Frazier / Michael Frazier II |
| **do not merge** | **Jameer Nelson** (nba 2749, 2009-2018) vs **Jameer Nelson Jr.** (statyx, 2025-26) — father and son |
| unclear | Travis Trice (nba 44839, 2017-18 pre-season) vs Travis Trice II (nba 1626275, 2015-16) — 44839 looks like a non-NBA id that slipped the guard |

Jameer Nelson is exactly why stripping suffixes inside `norm_name` would be
wrong, and why this stays a human decision applied as an `UPDATE`. The five
merges are not yet applied.

## Outstanding

- **`ctx.cur_season` is hardcoded** to `"2025-26"` at `context.py:21` with the
  `nba_parameters` lines commented out, while three ESPN leagues are already
  registered for 2026-27 (with no rows fetched yet). Everything above is
  season-scoped, so this gates it. If the hardcode is flipped before those
  leagues are fetched, the espn directory goes from 1098 ids to 0 — worth a guard
  that logs and skips a platform whose directory comes back empty.
- **`nba.team_roster.player_id` is `double precision`**, cast to bigint in the
  directory view. Worth fixing at source.
- **`util.conformed_player_id` has been renamed `conformed_player_id_RETIRED`**
  (quoted, since Postgres folds unquoted identifiers to lower case). The seed
  that read it has been removed; only the five un-ported dashboard views still
  read it, so it cannot be dropped until they are refabricated.

  `player_source_id` is a strict superset: of the retired table's 2871 unpivoted
  ids, **0 are absent** from the live table, which holds 5070. Dropping it costs
  nothing beyond the rebuild path already given up.

  What genuinely cannot be regenerated is `player_source_id` itself.
  `player_directory_vw` never reports espn/yahoo ids for players outside the
  seasons fty holds data for, so the directory alone rebuilds an incomplete
  table — deleting every espn mapping and re-resolving lost 23 of them and
  minted 21 spurious players. **So back up `player_source_id`; the retired table
  is a convenience, not the system of record.**
- `Database.deduplicate_tables` is unused but not broken. It lives on the
  connection, not `UtilComponent`, because it rewrites any table with a `season`
  column whatever its schema — schema-agnostic maintenance, not a `util.*`
  concern. `season` is now required (the `Database` has no `Context`), so callers
  pass `ctx.cur_season`.

`util.*` remains owned by `UtilComponent`; the name fold (`norm_name()` and its
helpers) stays in `utility.py`, since only that module's functions use it.

## league_byes: fixed and backfilled for every ESPN league-season

`get_league_byes` now reads ESPN's own `matchupPeriodId` off the raw league
payload. A bye is a schedule entry carrying only one side — a `home` with no
`away` — and that entry's period is authoritative.

The old implementation compared each competitor's `schedule` LENGTH against a
scaffold of every period and called the shortfall a bye, which can only ever
attribute one to the TRAILING period. Every row it had produced was two periods
late:

| league-season | true bye period | had been stored as |
|---|---|---|
| 2025-26 24608 | 19 (competitors 1, 11) | 21 |
| 2025-26 95537 | 17 (competitors 5, 26) | 19 |
| 2025-26 1382487116 | 20 (competitors 4, 12) | 22 |
| 2025-26 1966813226 | 18 (competitors 4, 5) | 20 |

Cross-validated independently: for 2024-25 league 95537 the new method gives
period 18 for competitors 5 and 26, which is exactly where `league_matchup`
already carried ESPN's own null-opponent rows.

`con.teams[].schedule` is deliberately unused — espn_api omits the bye from that
list in some seasons and inserts `None` in others, which is what made a length
comparison look plausible.

State after the backfill, driven off `fty_dev.league` so it covers the
league-season missing from `customer_league`:

| season | leagues | byes |
|---|---|---|
| 2023-24 | 1 | none (8 teams, full bracket) |
| 2024-25 | 2 | 95537 at period 18; 1966813226 none |
| 2025-26 | 4 | all four, periods 19 / 17 / 20 / 18 |
| 2026-27 | 3 | none yet |

## league_matchup_dates is derived, not entered

`get_league_matchup_dates` builds it from two inputs that already exist:

1. **A season-level week grid.** Week 1 starts on the MONDAY of the week
   containing the regular-season opener (`nba.key_dates`); weeks run Monday to
   Sunday; the week containing the All-Star break absorbs the following one,
   giving 14 days.
2. **Each league's own `settings.scheduleSettings.matchupPeriods`**, mapping a
   matchup period to a list of weeks. This is where the per-league difference
   lives, and ESPN publishes it, so it needs no hand encoding: in 2025-26 league
   95537 groups its playoff rounds as `[18, 19]` and `[20, 21]` while 24608 plays
   each week as its own period.

Verified against all six hand-entered league-seasons. 2023-24 and 2024-25
reproduce **exactly**; 2025-26's four leagues differ only in period 1 starting
2025-10-20 rather than 2025-10-21 — a date with no NBA games, so nothing it
contains changes. Three of the six group several weeks into a period, so the
mapping is genuinely exercised rather than just the 1:1 case.

**On the Monday anchor.** No rule fits the openers themselves: 2023-24 and
2024-25 both began their week on the Monday *before* the opener, while 2025-26
began on the opener, a Tuesday. Anchoring on the Monday is right for two of three
outright and inert for the third, which is what makes the whole table derivable
with no hand-entered facts at all.

Generated for all seven league-seasons that have `nba.key_dates` coverage.
2024-25 league 1966813226 gained its missing 24 rows, which took
`league_schedule_vw` for that season from 240 rows to 432 — the league had been
invisible in it.

### Calling it

```python
fty.connect_leagues()                    # current season, at season start
fty.get_league_matchup_dates()

fty.connect_leagues(season="2023-24")    # or backfill an earlier one
fty.get_league_matchup_dates()
```

Works from `self.leagues` like its neighbours, so the season comes from
`connect_leagues` — which only targets a season correctly because of the connect
fix below. No `util.update_schedule` row: derived once per season, like `league`,
`league_categories` and `league_competitor`.

For the current season, `connect_leagues()` logs and skips any league whose last
matchup in `league_matchup_dates` ended more than a day ago (a one-day buffer for
late stats). The league is still registered, but no connection is made and the
daily jobs leave it alone. Explicit seasons (`connect_leagues(season=...)`) are
never filtered, so backfills are unaffected.

It dispatches BEFORE deleting, unlike the methods around it. The handler raises
when `nba.key_dates` has no opener, and deleting first would leave the league
with no dates at all — which silently removes it from `league_schedule_vw`.

One gap this path does not reach: 2024-25 league 1966813226 has a `league` row
but no `customer_league` registration, so `connect_leagues(season="2024-25")`
connects only 95537. Its 24 rows are in place already, but they were written by
borrowing the platform's credentials; registering the league is the durable fix.

**2026-27 needs one thing: an `nba.key_dates` row** (Regular Season, and All Star
for the double-week). The handler raises rather than writing an empty frame, and
`get_league_matchup_dates` dispatches BEFORE deleting, so a missing key date
leaves the existing rows alone instead of wiping a league out of the view.

## league_matchup and league_byes are now complements

`get_league_matchup` took its period from the POSITION of an entry in
`con.teams[].schedule`, and read `league_byes` in order to splice a `None` into
that list first — espn_api omits a bye from the list in some seasons and inserts
`None` in others, so the padding was load bearing and a wrong bye period shifted
every later matchup. Both writers now read ESPN's `matchupPeriodId`:

- `league_matchup` — real matchups only, `opponent_id` never null
- `league_byes` — the complement

Disjoint by construction, so `fty_dev.league_schedule_grid_vw` is a plain
`UNION ALL` (see `build_league_schedule_vw.sql`), and **matchups + byes =
competitors x periods** holds for all ten league-seasons — a cheap invariant to
assert after any rerun. Neither writer depends on the other any more, so the
`sequence = 1` on `league_byes` in `util.update_schedule` is no longer needed.

Regenerated all ten league-seasons: 1978 rows -> 2168, no null opponents left,
and 2024-25's two bye rows moved out of `league_matchup` in the process, which
also makes the representation uniform across seasons.

### The delete-scope bug this exposed

Five methods — `get_league`, `get_league_categories`, `get_free_agents`,
`get_league_competitor`, `get_league_matchup` — scoped their DELETE to
`season = ctx.cur_season AND league_id IN (...)` while writing `con.season` rows.
Harmless only while every connection was built against the current season. Once
leagues connect at their own season, running any of them for an earlier season
would **delete the current season's rows and insert the other season's in their
place**. `_delete_connected()` now clears exactly the `(season, league_id)` pairs
that are connected. This had to be fixed before the regeneration above could be
run at all.

### Superseded note

**`league_matchup` needs no regeneration** — checked cell by cell against the
corrected byes, and every bye lands on the right period. For league 24608
competitors 1 and 11 hold rows at periods 1-18 and 20-21 and are missing exactly
19, their real bye. Nothing is shifted.

Judging that by `competitors x periods` is the wrong yardstick and briefly led to
the opposite conclusion here: the expectation has to subtract the bye cells, so
league 24608's 250 rows are 12 x 21 - 2, not two short.

The two seasons do represent a bye differently, which is worth knowing when
querying rather than a defect: 2024-25 carries a row with a null `opponent_id`
(ESPN supplied the `None` natively), while 2025-26 carries no row for that
period. Both put it in the right place. Looking for null opponents in
`league_matchup` therefore finds 2024-25's byes and misses 2025-26's —
`league_byes` is the reliable source for both.

**A registration gap, separately:** 2024-25 league 1966813226 appears in
`fty_dev.league` but not in `fty_dev.customer_league`, so any season loop driven
off registrations skips it.

## Earlier seasons were NOT corrupted by the connect bug

Checked, because the bug meant every pre-fix connection returned current-season
data regardless of the season asked for. No corruption:

- Writes were stamped `con.season`, which was also the current season, so rows
  were always labelled consistently with the data they held. Nothing is
  mislabelled; the bug simply made an earlier-season backfill a silent no-op.
- `fty_dev`'s history is genuinely historical. League 1966813226's 2023-24
  competitors are 8 different people ("El Barto", "Four Quarter Fiends") from its
  2025-26 10 ("Le'GM", "3s and Ds"), and the fixed connection returns 8 teams for
  2023-24 against 10 for 2025-26. That data came from `fty`, fetched in-season.

## Files

- `build_fty_dev.sql` — the schema, idempotent
- `build_player_identity.sql` — the two identity tables (DDL only). **Not
  idempotent** — drop the tables to re-run. Populate them with
  `conform_player_ids()` after the views file; the retired-crosswalk seed has
  been removed
- `build_player_identity_views.sql` — the directory, activity and backlog views
- `build_player_identity_review.sql` — `util.player_identity_review`, the staging
  table for model-proposed corrections. Idempotent (`IF NOT EXISTS`)
- `seed_player_identity_review.sql` — the hand-audited corrections, re-runnable
  (`ON CONFLICT DO NOTHING`); reapply after a rebuild
- `build_player_identity_check.sql` — `util.player_identity_check`, the two
  DB-native integrity checks as a view (`SELECT * ... WHERE failures > 0` should
  return no rows). The third, `colliding_names`, is in Python:
  `UtilComponent.check_player_identity()`
- `player_identity_ops.sql` — manual recipes for operating the identity tables
- `one-grain-two-formats.html` — source for the design diagram
- `superseded/` — earlier drafts, kept for history. **Do not run them:** both
  target `fty` rather than `fty_dev`, and predate the pct, `is_scored` and
  `nba_category` decisions above.
