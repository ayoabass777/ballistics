{{ config(materialized='table') }}

--------------------------------------------------------------------------------
-- Mart: mart_head_to_head_streaks
-- Latest head-to-head streaks grouped by team_pair_key with JSON payload.
--------------------------------------------------------------------------------

with h2h_latest as (
    select *
    from {{ ref('fact_head_to_head_latest') }}
),

labeled as (
    select
        h.team_pair_key,
        h.team_id,
        h.team_name,
        h.opponent_id,
        h.opponent_name,
        h.event_name,
        h.latest_event_flag as event_flag,
        h.streak_type,
        h.polarity,
        h.latest_streak_length as streak_length,
        h.latest_fixture_id as fixture_id,
        h.latest_kickoff_utc as kickoff_utc,
        h.latest_is_home as is_home,
        h.h2h_context_scope,
        h.league_season_id,
        h.league_name,
        h.season,
        h.total_event_count,
        h.total_event_flag_sum,
        h.updated_at,
        case
            when lower(h.event_name) in (
                'btts',
                'over_1_5',
                'over_2_5',
                'over_3_5',
                'over_4_5',
                'under_1_5',
                'under_2_5',
                'under_3_5',
                'under_4_5'
            ) then 'both'
            when h.h2h_context_scope = 'home' then 'home'
            when h.h2h_context_scope = 'away' then 'away'
            else h.h2h_context_scope
        end as teams
    from h2h_latest h
)

select
    team_pair_key,
    json_agg(
        json_build_object(
            'team_id', team_id,
            'team_name', team_name,
            'opponent_id', opponent_id,
            'opponent_name', opponent_name,
            'event_name', event_name,
            'streak_type', streak_type,
            'polarity', polarity,
            'scope', h2h_context_scope,
            'streak_length', streak_length,
            'total', total_event_flag_sum,
            'teams', teams,
            'fixture_id', fixture_id,
            'kickoff_utc', kickoff_utc,
            'is_home', is_home
        )
        order by streak_length desc, event_name
    ) as h2h_streaks_json,
    max(updated_at) as updated_at
from labeled
group by team_pair_key
