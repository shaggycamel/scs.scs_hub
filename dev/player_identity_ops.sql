-- Operating util.player / util.player_source_id. Recipes, not a script to run
-- wholesale. Each block is independent; all have been rehearsed against local
-- Postgres inside a transaction.

-- 1. IS THERE ANYTHING TO DO?
-- The daily jobs that write the source tables are the detector. Non-empty means
-- run hub.util.conform_player_ids().
SELECT platform, count(*) AS unresolved
FROM util.unmatched_player_source_vw
GROUP BY 1 ORDER BY 1;

-- 2. INTEGRITY CHECKS — every check must report 0 failures. Worth running after
-- any resolve, apply or manual edit. This SQL covers the two portable checks;
-- hub.util.check_player_identity() adds colliding_names (NFKD fold).
SELECT 'players_owning_nothing' AS check_name, count(*) AS failures
FROM util.player p
WHERE NOT EXISTS (
    SELECT 1 FROM util.player_source_id m WHERE m.player_key = p.player_key
)
UNION ALL
SELECT 'backlog', count(*)
FROM util.unmatched_player_source_vw;
-- hub.util.check_player_identity()   # all three, including colliding_names

-- 3. WHAT DID THE MATCHER INVENT? Review queue. Most are legitimate — the box
-- scores reach back to 2009-10 and statyx carries college prospects — so this is
-- for spotting splits, not for clearing to zero.
SELECT p.player_key, p.conformed_name,
       string_agg(m.platform || ':' || m.source_id, ' ' ORDER BY m.platform) AS ids
FROM util.player p
JOIN util.player_source_id m USING (player_key)
WHERE p.needs_review
GROUP BY 1, 2
HAVING NOT bool_or(m.platform = 'nba')   -- no nba id is the split signature
ORDER BY 2;

-- 4. FIND SPLITS THE MATCHER CANNOT: pairs differing only by a generational
-- suffix, which the fold deliberately does NOT strip. Read every result
-- before acting — "Jameer Nelson" and "Jameer Nelson Jr." are father and son,
-- and automating this rule would merge them. The fold itself is Python
-- (norm_name() in utility.py); this SQL approximation only needs the trailing
-- suffix, so lower + strip punctuation is enough for a human review.
WITH s AS (
    SELECT player_key, conformed_name,
           regexp_replace(lower(regexp_replace(conformed_name, '[^a-zA-Z]', '', 'g')),
                          '(jr|sr|ii|iii|iv)$', '') AS stem
    FROM util.player
)
SELECT stem, string_agg(conformed_name, '  vs  ' ORDER BY conformed_name) AS pair
FROM s
WHERE stem IN (SELECT stem FROM s GROUP BY stem HAVING count(*) > 1)
GROUP BY 1 ORDER BY 1;

-- 5. MERGE TWO PLAYERS. Move the mappings, drop the empty player, clear the
-- flag. Keyed on name rather than on player_key, because player_key is an
-- identity column and is NOT stable across a rebuild of these tables.
-- Wrap it in BEGIN/ROLLBACK and re-run check (2) before committing.
BEGIN;
    UPDATE util.player_source_id
    SET player_key = (SELECT player_key FROM util.player
                      WHERE conformed_name = 'Michael Frazier II')
    WHERE player_key = (SELECT player_key FROM util.player
                        WHERE conformed_name = 'Michael Frazier');

    DELETE FROM util.player WHERE conformed_name = 'Michael Frazier';

    UPDATE util.player SET needs_review = false
    WHERE conformed_name = 'Michael Frazier II';
ROLLBACK;  -- change to COMMIT once check (2) is clean

-- The five merges a name audit found and a human confirmed, not yet applied:
--   Billy Garrett      -> Billy Garrett Jr.
--   Boo Buie           -> Boo Buie III
--   Joel Berry         -> Joel Berry II
--   Kevin Knox         -> Kevin Knox II
--   Michael Frazier    -> Michael Frazier II
-- Explicitly NOT a merge: Jameer Nelson (nba 2749) / Jameer Nelson Jr. (statyx).

-- 6. CORRECT A DISPLAY NAME. conformed_name is the only field a human owns; no
-- job overwrites it. Matching happens on norm_name() (Python), so a cosmetic fix here
-- cannot break an existing mapping — but changing it to a DIFFERENT person's
-- normalised name would, so re-run check (2).
UPDATE util.player SET conformed_name = 'A.J. Lawson', needs_review = false
WHERE conformed_name = 'AJ Lawson';

-- 7. ATTACH AN ID BY HAND when the name will never match (a platform rename such
-- as ESPN's "Bub Carrington" -> "Carlton Carrington"). Find the id in the
-- backlog view first, then point it at the right player.
INSERT INTO util.player_source_id (platform, source_id, source_name, player_key)
SELECT 'espn', 4845374, 'Carlton Carrington',
       (SELECT player_key FROM util.player WHERE conformed_name = 'Bub Carrington')
ON CONFLICT (platform, source_id) DO UPDATE
    SET player_key  = EXCLUDED.player_key,
        source_name = EXCLUDED.source_name;

-- 8. SNAPSHOT BEFORE ANYTHING DESTRUCTIVE. player_source_id is the system of
-- record and is NOT reconstructible from util.player_directory_vw alone, since
-- the directory never reports espn/yahoo ids for players outside the seasons fty
-- holds data for. Take a copy first; player_key is an identity column, so a
-- rebuild renumbers every key and anything referencing them must be remapped.
CREATE TABLE util.player_bak            AS SELECT * FROM util.player;
CREATE TABLE util.player_source_id_bak  AS SELECT * FROM util.player_source_id;

-- 8a. RESTORE FROM A SNAPSHOT, PRESERVING KEYS. player_key is
-- GENERATED BY DEFAULT (not ALWAYS) precisely so this works — explicit values
-- are accepted on insert. The sequence does NOT learn about them, though: after
-- restoring keys up to 2737, the next auto-allocated key was still 2738 in
-- testing, so reset it or the next conform_player_ids() collides on the PK.
INSERT INTO util.player           SELECT * FROM util.player_bak;
INSERT INTO util.player_source_id SELECT * FROM util.player_source_id_bak;
SELECT setval(pg_get_serial_sequence('util.player', 'player_key'),
              (SELECT max(player_key) FROM util.player));

-- 8b. REBUILD FROM SCRATCH. dev/build_player_identity.sql (DDL), then the views
-- file, then hub.util.conform_player_ids(). Expected afterwards: 2737 players,
-- 5070 mappings, 0 orphans, 0 backlog. The old seed from
-- util."conformed_player_id_RETIRED" has been removed, so a directory-only
-- rebuild is INCOMPLETE — player_directory_vw never reports espn/yahoo ids for
-- players outside the seasons fty holds data for. Restore from a snapshot (8a)
-- instead when you can.

-- 9. ADD A PLATFORM. No DDL — the mapping table is already long. Add a UNION arm
-- to util.player_directory_vw returning (season, platform, source_id,
-- source_name) and resolve. Nothing else changes: ctx.active_ids('<platform>')
-- works immediately, and nothing needs a new column.
