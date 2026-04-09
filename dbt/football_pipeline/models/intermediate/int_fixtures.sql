{{ config(
    materialized='table',
    schema='int'
) }}

--------------------------------------------------------------------------------
-- Core dimension model: dim_fixtures
-- Incrementally builds the fixtures dimension by upserting from staging models
--------------------------------------------------------------------------------

SELECT
    rf.fixture_id,
    lc.league_season_id,
    lc.league_name,
    lc.season,
    rf.kickoff_utc,
    home_team.team_id                   as home_team_id,
    away_team.team_id               as away_team_id,
    home_team.team_name                   as home_team_name,
    away_team.team_name               as away_team_name,
    rf.home_team_fulltime_goal    as home_ftg,
    rf.away_team_fulltime_goal    as away_ftg,
    rf.home_team_halftime_goal    as home_htg,
    rf.away_team_halftime_goal    as away_htg,
    rf.home_fulltime_result       as home_ftr,
    rf.away_fulltime_result       as away_ftr,
    rf.home_halftime_result       as home_htr,
    rf.away_halftime_result       as away_htr,
    rf.updated_at,
    rf.is_played,
    rf.fixture_status,
    lc.is_current
FROM {{ ref('stg_raw_fixtures') }} AS rf
JOIN {{ ref('int_league_context') }} AS lc
  ON rf.api_league_id = lc.api_league_id
  AND lc.season = rf.season
JOIN {{ref('dim_teams')}} AS home_team
  ON rf.home_team_id = home_team.api_team_id
JOIN {{ref('dim_teams')}} AS away_team
  ON rf.away_team_id = away_team.api_team_id
{% if is_incremental() %}
    WHERE rf.updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
