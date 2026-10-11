{{ config(materialized='table') }}

--------------------------------------------------------------------------------
-- Fact: fact_fixture_xg
-- Expected goals at fixture grain, for played fixtures with both teams' xG.
-- Carries the warehouse team ids / league season from int_fixtures so the
-- engine joins it straight onto fact_head_to_head (is_home rows) by fixture_id.
--
-- Full rebuild every run (small: covered leagues only). Not incremental on
-- purpose: xG arrives after the result without changing raw_fixtures.updated_at,
-- so an updated_at watermark would never pick it up.
--------------------------------------------------------------------------------

select
    f.fixture_id,
    f.league_season_id,
    f.league_name,
    f.season,
    f.kickoff_utc,
    f.home_team_id,
    f.away_team_id,
    x.home_xg,
    x.away_xg,
    x.captured_at as xg_captured_at
from {{ ref('stg_raw_fixture_xg') }} as x
join {{ ref('int_fixtures') }} as f
  on f.fixture_id = x.fixture_id
where f.is_played
  and x.home_xg is not null
  and x.away_xg is not null
