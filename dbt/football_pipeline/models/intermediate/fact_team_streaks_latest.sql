{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Intermediate: fact_team_streaks_latest
-- One latest streak per team/event/context to keep downstream suppression light.
--------------------------------------------------------------------------------
with ranked as (
    select
        s.*,
        md5(concat_ws('::', team_id::text, event_name, context_scope)) as streak_id,
        row_number() over (
            partition by team_id, event_name, context_scope
            order by kickoff_utc desc
        ) as rn
    from {{ ref('fact_team_streaks') }} s
),

final as (
    select
        team_id,
        team_name,
        streak_id,
        opponent_id,
        opponent_name,
        fixture_id,
        kickoff_utc,
        is_home,
        league_season_id,
        league_name,
        event_flag,
        event_name,
        streak_type,
        is_active,
        context_scope,
        composition,
        polarity,
        threshold,
        is_streak_eligible,
        streak_length,
        current_streak_length,
        min_required_length,
        next_opponent_id,
        next_opponent_name,
        next_fixture_id,
        next_kickoff_utc,
        next_is_home,
        avg_goals_for_context,
        avg_goals_against_context,
        updated_at
    from ranked
    where rn = 1
            and is_active
            and current_streak_length >= min_required_length

)
select *
from final

