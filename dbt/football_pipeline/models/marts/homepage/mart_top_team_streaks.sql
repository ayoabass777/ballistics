{{ config(materialized='table') }}

WITH ranked AS (
  SELECT
    team_id,
    team_name,
    next_opponent_name,
    next_opponent_id,
    next_fixture_id,
    next_kickoff_utc,
    league_season_id,
    league_name,
    event_name,
    streak_type,
    context_scope,
    is_streak_eligible,
    event_flag,
    streak_length,
    current_streak_length
   
  FROM {{ ref('fact_team_streaks_latest') }}
   
),
current_streaks AS (
  SELECT
    team_id,
    team_name,
    next_opponent_name,
    next_opponent_id,
    next_fixture_id,
    next_kickoff_utc,
    league_season_id,
    league_name,
    event_name,
    streak_type,
    context_scope,
    current_streak_length
  FROM ranked
  WHERE is_streak_eligible

)

SELECT
  ROW_NUMBER() OVER (ORDER BY team_id, league_season_id, event_name, streak_type) as streak_id,
  team_id,
  league_name,
  team_name,
  next_opponent_name,
  next_opponent_id,
  next_fixture_id,
  next_kickoff_utc,
  event_name,
  streak_type,
  context_scope,
  current_streak_length
FROM current_streaks
ORDER BY
  current_streak_length DESC,
  team_name
