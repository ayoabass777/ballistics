{{ config(
    materialized='table'
) }}

--------------------------------------------------------------------------------
-- Fact: fact_head_to_head_streaks
-- Computes head-to-head streaks per team/opponent/event/context using run-length
-- encoding (resets when the event_flag is 0).
--------------------------------------------------------------------------------

with events as (
    select *
    from {{ ref('fact_head_to_head_events') }}
),

numbered as (
    select
        events.*,
        row_number() over (
            partition by team_id, opponent_id, event_name, h2h_context_scope
            order by kickoff_utc, fixture_id
        ) as rn,
        lag(event_flag, 1, 0) over (
            partition by team_id, opponent_id, event_name, h2h_context_scope
            order by kickoff_utc, fixture_id
        ) as prev_flag
    from events
),

streak_groups as (
    select
        *,
        sum(
            case
                when event_flag = 1 and prev_flag = 0 then 1
                else 0
            end
        ) over (
            partition by team_id, opponent_id, event_name, h2h_context_scope
            order by rn
        ) as streak_id
    from numbered
),

streaks as (
    select
        *,
        case
            when event_flag = 1 then row_number() over (
                partition by team_id, opponent_id, event_name, h2h_context_scope, streak_id
                order by rn
            )
            else 0
        end as streak_length
    from streak_groups
),

with_lengths as (
    select
        *,
        case
            when event_flag = 1 then streak_length
            else 0
        end as current_streak_length,
        case
            when event_name in ('score_1goal', 'concede_1') then 4
            else 3
        end as min_required_length
    from streaks
),

with_totals as (
    select
        *,
        count(*) over (
            partition by team_id, opponent_id, event_name, h2h_context_scope
        ) as total_event_count,
        sum(event_flag) over (
            partition by team_id, opponent_id, event_name, h2h_context_scope
        ) as total_event_flag_sum
    from with_lengths
)

select * from with_totals
