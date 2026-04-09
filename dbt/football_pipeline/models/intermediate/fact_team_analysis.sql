{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Intermediate: fact_team_analysis
-- Combines streak relevance with team metadata; uses context averages already
-- carried on fact_team_streak_relevance_score.
--------------------------------------------------------------------------------

select
    s.team_id,
    coalesce(t.team_name, s.team_name) as team_name,
    t.logo_file_id,
    s.league_season_id,
    s.league_name,
    s.event_name,
    s.streak_type,
    s.composition,
    s.polarity,
    s.threshold,
    s.context_scope,
    s.current_streak_length,
    s.streak_length,
    s.kickoff_utc,
    s.fixture_id,
    s.opponent_id,
    s.opponent_name,
    s.is_home,
    s.next_fixture_id,
    s.next_kickoff_utc,
    s.next_opponent_id,
    s.next_opponent_name,
    s.avg_goals_for_context as avg_goals_for,
    s.avg_goals_against_context as avg_goals_against,
    s.relevance_score
from {{ ref('fact_team_streak_relevance_score') }} s
left join {{ ref('dim_teams') }} t
  on t.team_id = s.team_id
