-- One-off (D81): tag the tier of leagues that are in the database but not in
-- data/metadata.yaml, so Big dogs can be limited to top flights (tier 1).
-- Safe to re-run: only fills tiers that are still NULL; never overwrites a tier
-- that metadata.yaml set. Does not call the API or trigger the bootstrap DAG.
--
--   docker exec -i ballistics_postgres psql -U ballistics_user -d ballistics \
--     < etl/sql/set_missing_league_tiers.sql

BEGIN;

WITH wanted (league_name, country_name, tier) AS (VALUES
    ('Virsliga',                    'Latvia',    1),
    ('Meistriliiga',                'Estonia',   1),
    ('A Lyga',                      'Lithuania', 1),
    ('1 Lyga',                      'Lithuania', 2),
    ('Super League',                'China',     1),
    ('League One',                  'China',     2),
    ('Premier Division',            'Ireland',   1),
    ('First Division',              'Ireland',   2),
    ('Erovnuli Liga',               'Georgia',   1),
    ('1. Division',                 'Norway',    2),
    ('Division 2 - Södra Svealand', 'Sweden',    4),
    ('USL Championship',            'USA',       2),
    ('USL League One',              'USA',       3),
    ('1. Deild',                    'Iceland',   2),
    ('Primera División',            'Chile',     1),
    ('League Two',                  'China',     3),
    ('Capixaba B',                  'Brazil',    5)   -- Espírito Santo state 2nd division; state leagues sit outside/below Série A–D
)
UPDATE dim.dim_leagues dl
SET tier = w.tier
FROM wanted w, dim.dim_countries dc
WHERE dc.country_id = dl.country_id
  AND dc.country_name = w.country_name
  AND dl.league_name = w.league_name
  AND dl.tier IS NULL
RETURNING dc.country_name, dl.league_name, dl.tier AS tier_set;

-- Correction: an earlier version of this script set Capixaba B to tier 2.
UPDATE dim.dim_leagues dl
SET tier = 5
FROM dim.dim_countries dc
WHERE dc.country_id = dl.country_id AND dc.country_name = 'Brazil'
  AND dl.league_name = 'Capixaba B' AND dl.tier = 2
RETURNING dl.league_name, dl.tier AS tier_corrected;

-- Leagues with played matches that still have no tier (no Big dogs until tagged).
SELECT COALESCE(dc.country_name, '?') AS country, dl.league_name, COUNT(*) AS played
FROM dim.dim_leagues dl
LEFT JOIN dim.dim_countries dc ON dc.country_id = dl.country_id
JOIN raw.raw_fixtures rf ON rf.api_league_id = dl.api_league_id
WHERE dl.tier IS NULL
GROUP BY 1, 2
ORDER BY played DESC;

COMMIT;
