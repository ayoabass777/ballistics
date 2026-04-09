

{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Intermediate: fact_team_streaks
-- Compute running streaks for each team and event using run-length encoding
--------------------------------------------------------------------------------

with base as (
    select
        team_id,
        team_name,
        league_season_id,
        league_name,
        event_name,
        context_scope,
        fixture_id,
        kickoff_utc,
        event_flag,
        is_active,
        streak_type,
        composition,
        polarity,
        threshold,
        opponent_name,
        opponent_id,
        is_home,
        next_opponent_id,
        next_opponent_name,
        next_fixture_id,
        next_kickoff_utc,
        next_is_home,
        avg_goals_for_context,
        avg_goals_against_context,
        updated_at
    from {{ ref('fact_team_events') }}
    where is_current
      and is_played
),

numbered as (
    select
        base.*,
        row_number() over (
            partition by team_id, event_name, context_scope
            order by kickoff_utc
        ) as rn,
        lag(event_flag, 1, 0) over (
            partition by team_id, event_name, context_scope
            order by kickoff_utc
        ) as prev_flag,
        last_value(next_opponent_id) over (
            partition by team_id, league_season_id
            order by kickoff_utc
            rows between unbounded preceding and unbounded following
        ) as upcoming_opponent_id,
        last_value(next_opponent_name) over (
            partition by team_id, league_season_id
            order by kickoff_utc
            rows between unbounded preceding and unbounded following
        ) as upcoming_opponent_name,
        last_value(next_fixture_id) over (
            partition by team_id, league_season_id
            order by kickoff_utc
            rows between unbounded preceding and unbounded following
        ) as upcoming_fixture_id,
        last_value(next_kickoff_utc) over (
            partition by team_id, league_season_id
            order by kickoff_utc
            rows between unbounded preceding and unbounded following
        ) as upcoming_kickoff_utc,
        last_value(next_is_home) over (
            partition by team_id, league_season_id
            order by kickoff_utc
            rows between unbounded preceding and unbounded following
        ) as upcoming_is_home
    from base
),

grouped as (
    select
        team_id,
        team_name,
        league_season_id,
        league_name,
        event_name,
        streak_type,
        composition,
        polarity,
        threshold,
        fixture_id,
        kickoff_utc,
        event_flag,
        is_active,
        context_scope,
        avg_goals_for_context,
        avg_goals_against_context,
        opponent_name,
        opponent_id,
        is_home,
        upcoming_opponent_id as next_opponent_id,
        upcoming_opponent_name as next_opponent_name,
        upcoming_fixture_id as next_fixture_id,
        upcoming_kickoff_utc as next_kickoff_utc,
        upcoming_is_home as next_is_home,
        rn,
        sum(
            case when event_flag = 1 and prev_flag = 0 then 1 else 0 end
        ) over (
            partition by team_id, event_name, context_scope
            order by rn
        ) as streak_id,
        updated_at
    from numbered
),

streaks as (
    select
        team_id,
        team_name,
        league_season_id,
        league_name,
        event_name,
        streak_type,
        composition,
        polarity,
        threshold,
        fixture_id,
        kickoff_utc,
        event_flag,
        is_active,
        context_scope,
        avg_goals_for_context,
        avg_goals_against_context,
        opponent_name,
        opponent_id,
        is_home,
        next_opponent_id,
        next_opponent_name,
        next_fixture_id,
        next_kickoff_utc,
        next_is_home,
        row_number() over (
            partition by team_id, event_name, context_scope, streak_id
            order by rn
        ) as streak_length,
        LAST_VALUE(next_is_home) over (
            partition by team_id, event_name, context_scope, streak_id
            order by rn
            rows between unbounded preceding and unbounded following
        ) as context_checker,
        updated_at
    from grouped
),

eligible_streaks_and_current_length AS (
    SELECT *,
    CASE
            WHEN context_scope = 'home' THEN COALESCE(context_checker, false)
            WHEN context_scope = 'away' THEN COALESCE(NOT context_checker, false)
            ELSE true
        END AS is_streak_eligible,
     CASE
            WHEN is_active THEN streak_length
            ELSE 0
        END AS current_streak_length,
    CASE
            WHEN event_name IN ('score_1goal', 'concede_1') THEN 4
            ELSE 3
        END AS min_required_length
    FROM streaks
),

final as (
    select
        team_id,
        team_name,
        league_season_id,
        league_name,
        event_flag,
        event_name,
        streak_type,
        composition,
        polarity,
        threshold,
        fixture_id,
        kickoff_utc,
        avg_goals_for_context,
        avg_goals_against_context,
        opponent_name,
        opponent_id,
        context_checker,
        is_home,
        is_active,
        context_scope,
        streak_length,
        current_streak_length,
        min_required_length,
        is_streak_eligible,
        next_opponent_id,
        next_opponent_name,
        next_fixture_id,
        next_kickoff_utc,
        next_is_home,
        updated_at
    from eligible_streaks_and_current_length
)

select * from final
