{{ config(
    materialized='view'
) }}

--------------------------------------------------------------------------------
-- Fact: fact_head_to_head_latest
-- One active streak per team/opponent/event (latest played fixture only).
--------------------------------------------------------------------------------

with ranked as (
    select
        -- exclude any existing rn-like columns to avoid ambiguity
        team_id,
        team_name,
        opponent_id,
        opponent_name,
        team_pair_key,
        event_name,
        event_flag,
        streak_type,
        polarity,
        streak_length,
        h2h_context_scope,
        fixture_id,
        kickoff_utc,
        is_home,
        league_season_id,
        league_name,
        season,
        total_event_count,
        total_event_flag_sum,
        updated_at,
        row_number() over (
            partition by team_id, opponent_id, event_name, h2h_context_scope
            order by kickoff_utc desc, fixture_id desc
        ) as latest_rn
    from {{ ref('fact_head_to_head_streaks') }}
    where event_flag = 1
),

final as (
    select
        team_id,
        team_name,
        opponent_id,
        opponent_name,
        team_pair_key,
        event_name,
        event_flag as latest_event_flag,
        streak_type,
        polarity,
        streak_length as latest_streak_length,
        fixture_id as latest_fixture_id,
        kickoff_utc as latest_kickoff_utc,
        is_home as latest_is_home,
        h2h_context_scope,
        league_season_id,
        league_name,
        season,
        total_event_count,
        total_event_flag_sum,
        updated_at
    from ranked
    where latest_rn = 1
)

select * from final
