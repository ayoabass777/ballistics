{{ config(materialized='view') }}
--- to be deleted
--------------------------------------------------------------------------------
-- Mart: mart_teams_fixture_stats
-- Per-team fixture view with opponent context and next fixture pointers
--------------------------------------------------------------------------------

{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Mart: mart_teams_fixture_stats
-- Per-team fixture view with opponent context and next fixture pointers
--------------------------------------------------------------------------------

WITH base AS (
    SELECT
        tts.next_fixture_id,
        tts.next_kickoff_utc,
        tts.league_season_id,
        tts.league_name,
        tts.team_id,
        tts.team_name,
        tts.season,
        tts.games_played,
        tts.wins,
        tts.draws,
        tts.losses,
        tts.win_rate,
        tts.draw_rate,
        tts.loss_rate,
        tts.avg_goals_for,
        tts.avg_goals_against,
        tts.recent_form,
        ots.team_id AS next_opponent_id,
        ots.team_name AS next_opponent_name,
        ots.win_rate AS opponent_win_rate,
        ots.draw_rate AS opponent_draw_rate,
        ots.loss_rate AS opponent_loss_rate,
        ots.avg_goals_for AS opponent_avg_goals_for,
        ots.avg_goals_against AS opponent_avg_goals_against,
        ots.recent_form AS opponent_recent_form
    FROM {{ ref('mart_team_stats') }} AS tts -- team to show stats for
    JOIN {{ ref('mart_team_stats') }} AS ots -- opponent team stats
      ON tts.next_fixture_id = ots.next_fixture_id
     AND tts.next_opponent_id = ots.team_id
)

SELECT * FROM base

