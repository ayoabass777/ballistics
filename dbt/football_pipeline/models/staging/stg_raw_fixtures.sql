{{ config(
    materialized='incremental',
    unique_key='api_fixture_id'
) }}

SELECT
    fixture_id,
    api_fixture_id,
    api_league_id,
    season,
    kickoff_utc,
    fixture_status,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_team_halftime_goal,
    away_team_halftime_goal,
    home_team_fulltime_goal,
    away_team_fulltime_goal,
    home_fulltime_result,
    away_fulltime_result,
    home_halftime_result,
    away_halftime_result,
    CASE WHEN fixture_status='FT' THEN TRUE
    ELSE FALSE END AS is_played,
    created_at,
    updated_at
FROM {{ source('raw', 'raw_fixtures') }}
{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
