{{ config(materialized='table') }}

--------------------------------------------------------------------------------
-- Mart: mart_team_info
-- Simple projection of team standings/context from the INT dim_teams model.
--------------------------------------------------------------------------------

SELECT
    team_id,
    team_name,
    team_slug,
    logo_file_id,
    rank,
    points,
    games_played,
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    goal_diff,
    form,
    league_season_id,
    next_fixture_id,
    next_opponent_id,
    next_opponent_name,
    next_is_home,
    next_kickoff_utc
    --venue
FROM {{ ref('fact_team_info') }}
