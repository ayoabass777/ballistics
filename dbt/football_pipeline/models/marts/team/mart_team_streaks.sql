{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Mart: mart_team_streaks
-- Projection-only mart over fact_team_streak_suppresion (no relevance scoring).
--------------------------------------------------------------------------------

with streaks as (
  select
    streak_id,
    team_id,
    team_name,
    event_name,
    streak_type,
    current_streak_length as streak_length,
    polarity,
    context_scope as scope,
    is_streak_eligible,
    league_season_id,
    league_name,
    next_opponent_id as opponent_id,
    next_opponent_name as opponent_name,
    next_fixture_id as fixture_id,
    next_kickoff_utc as kickoff_utc
  from {{ ref('fact_team_streak_suppresion') }}
  where not is_suppressed
    and is_active
)

select
  streak_id,
  team_id,
  team_name,
  event_name,
  streak_type,
  streak_length,
  polarity,
  scope,
  is_streak_eligible,
  league_season_id,
  league_name,
  opponent_id,
  opponent_name,
  fixture_id,
  kickoff_utc
from streaks
