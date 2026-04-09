{{ config(materialized='table') }}

--------------------------------------------------------------------------------
-- Mart: mart_team_stats
-- Projection-only mart over fact_team_stats (no additional calculations)
--------------------------------------------------------------------------------

SELECT
    kickoff_utc,
    team_name,
    team_id,
    is_home,
    league_name,
    league_id,
    league_season_id,
    country,
    season,
    result,
    opponent_name,
    opponent_id,
    games_played,
    wins,
    draws,
    losses,
    goals_scored,
    goals_conceded,
    win_rate,
    draw_rate,
    loss_rate,
    avg_goals_for,
    avg_goals_against,
    next_opponent_id,
    next_opponent_name,
    next_fixture_id,
    next_kickoff_utc,
    rank,
    recent_form
FROM {{ ref('fact_team_stats') }}
