{{ config(materialized='view') }}
-- to be deleted
-------------------------------------------------------------------------------
-- Mart: mart_teams_fixture_streaks
-- Exposes each team's latest streak metrics per event for API consumption.
-- Filtering by two team_ids in the API layer returns both clubs' current runs.
-------------------------------------------------------------------------------

with streaks as (
    select
        ts.team_id AS team_id,
        ts.team_name AS team_name, 
        os.team_id AS opponent_id,
        os.team_name AS opponent_name,
        ts.league_season_id,
        ts.league_name,
        ts.next_fixture_id,
        ts.next_kickoff_utc,
        ts.is_home,
        ts.event_name AS team_event_name,
        ts.streak_type AS team_streak_type,
        ts.event_flag AS team_event_flag,
        ts.current_streak_length AS team_streak_length,
        os.event_name AS opponent_event_name,
        os.streak_type AS opponent_streak_type,
        os.event_flag AS opponent_event_flag,
        os.current_streak_length AS opponent_streak_length
    from {{ ref('fact_team_streaks_latest') }} AS ts -- team streaks
    join {{ ref('fact_team_streaks_latest') }} AS os -- opponent_streak
      on ts.next_fixture_id = os.next_fixture_id
      AND ts.opponent_id = os.team_id
      WHERE ts.context_scope = 'all'
    
)

select
    --team_id,
    --team_name,
    --opponent_id,
    --opponent_name,
    --league_season_id,
    --league_name,
    --season,
    --fixture_id,
    --kickoff_utc,
    --is_home,
    --event_name,
    --streak_type,
    --(event_flag = 1) as is_active,
    --current_streak_length
    *
from streaks
--where fixture_rank = 1
