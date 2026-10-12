{{ config(materialized='table') }}

-- Fixture grain, every status (NS, FT, postponed, ...). The engine's only
-- fixture source: it must not read raw.raw_fixtures.
--
-- IDs are warehouse ids (team_id, league_id, league_season_id). The one
-- exception is api_league_id, kept while engine calibration maps, league
-- clustering and the Big dog state are keyed on it. Remove it once those
-- move to league_id.
--
-- LEFT joins on purpose: a fixture whose league season or team is not yet in
-- the dimensions still appears (with NULL ids) instead of silently vanishing.
-- Team names are the provider's names from staging (dim names are initcap'd).

with teams as (
    select api_team_id, team_id
    from {{ ref('stg_dim_teams') }}
)

select
    rf.fixture_id,
    lc.league_season_id,
    lc.league_id,
    rf.api_league_id,
    lc.league_name,
    lc.country_name,
    lc.tier                         as league_tier,
    rf.season,
    lc.is_current,
    rf.kickoff_utc,
    rf.fixture_status,
    rf.is_played,
    ht.team_id                      as home_team_id,
    at.team_id                      as away_team_id,
    rf.home_team_name,
    rf.away_team_name,
    rf.home_team_halftime_goal,
    rf.away_team_halftime_goal,
    rf.home_team_fulltime_goal,
    rf.away_team_fulltime_goal,
    rf.home_fulltime_result,
    rf.away_fulltime_result,
    rf.round,
    {{ fixture_stage('rf.round') }}            as stage,
    {{ fixture_stage('rf.round') }} = 'knockout' as is_knockout,
    rf.updated_at
from {{ ref('stg_raw_fixtures') }} as rf
left join {{ ref('int_league_context') }} as lc
  on lc.api_league_id = rf.api_league_id
 and lc.season = rf.season
left join teams as ht on ht.api_team_id = rf.home_api_team_id
left join teams as at on at.api_team_id = rf.away_api_team_id
