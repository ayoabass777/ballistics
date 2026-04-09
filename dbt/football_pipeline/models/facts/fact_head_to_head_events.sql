{{ config(
    materialized='table'
) }}

--------------------------------------------------------------------------------
-- Fact: fact_head_to_head_events
-- Base head-to-head events per team/opponent/event (no streak logic).
--------------------------------------------------------------------------------

with events as (
    select
        league_season_id,
        league_name,
        season,
        fixture_id,
        team_id,
        team_name,
        opponent_id,
        opponent_name,
        kickoff_utc,
        fulltime_result,
        event_name,
        event_flag,
        streak_type,
        polarity,
        is_home,
        is_current,
        updated_at,
        context_scope as h2h_context_scope,
        team_pair_key
    from {{ ref('fact_team_events') }}
    where is_played
)

select * from events
