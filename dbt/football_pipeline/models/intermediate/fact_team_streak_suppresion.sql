{{ config(
    materialized='table',
    post_hook=[
        "create index if not exists idx_fact_team_streak_suppresion_ctx_run on {{ this }} (team_id, event_name, context_scope, fixture_id, current_streak_length)"
    ]
) }}

--------------------------------------------------------------------------------
-- Intermediate: fact_team_streak_suppresion
-- Mark weaker streak variants as suppressed when a stronger sibling exists
-- within the same team/context/run. Suppression is data hygiene only; ranking
-- happens downstream.
--------------------------------------------------------------------------------

with base as (
    -- fact_team_streaks already filters to active, eligible streaks
    select *
    from {{ ref('fact_team_streaks_latest') }}

),

-- Stronger → weaker mappings scoped within the same narrative axis
suppression_pairs as (
    select *
    from (values
        ('win',            'unbeaten',       true),  -- stricter result beats unbeaten
        ('loss',           'winless',        true),  -- stricter result beats winless
        ('score_2goals',   'score_1goal',    true),  -- higher scoring threshold wins
        ('concede_2',      'concede_1',      true),  -- higher conceded threshold wins
        ('clean_sheet',    'no_clean_sheet', true),  -- defensive dominance wins

        -- Over goals hierarchy: 4.5 > 3.5 > 2.5 > 1.5
        ('Over_4_5',       'Over_3_5',      true),
        ('Over_4_5',       'Over_2_5',      true),
        ('Over_4_5',       'Over_1_5',      true),
        ('Over_3_5',       'Over_2_5',      true),
        ('Over_3_5',       'Over_1_5',      true),
        ('Over_2_5',       'Over_1_5',      true),

        -- Under goals hierarchy: 1.5 > 2.5 > 3.5 > 4.5
        ('Under_1_5',      'Under_2_5',     true),
        ('Under_1_5',      'Under_3_5',     true),
        ('Under_1_5',      'Under_4_5',     true),
        ('Under_2_5',      'Under_3_5',     true),
        ('Under_2_5',      'Under_4_5',     true),
        ('Under_3_5',      'Under_4_5',     true)
    ) as sp(stronger_event, weaker_event, require_equal_length)
),

suppressed as (
    select
        b.*,
        case
            when exists (
                select 1
                from base s
                join suppression_pairs sp
                  on sp.stronger_event = s.event_name
                 and sp.weaker_event = b.event_name
                where s.team_id = b.team_id
                  and s.context_scope = b.context_scope
                  and s.fixture_id = b.fixture_id
                  and (
                    not sp.require_equal_length
                    or s.current_streak_length = b.current_streak_length
                  )
            ) then true
            else false
        end as is_suppressed,
        case
            when exists (
                select 1
                from base s
                join suppression_pairs sp
                  on sp.stronger_event = s.event_name
                 and sp.weaker_event = b.event_name
                where s.team_id = b.team_id
                  and s.context_scope = b.context_scope
                  and s.fixture_id = b.fixture_id
                  and (
                    not sp.require_equal_length
                    or s.current_streak_length = b.current_streak_length
                  )
            ) then 'stronger_variant'
            else null
        end as suppression_reason
    from base b
)

select
    team_id,
    team_name,
    league_season_id,
    league_name,
    streak_id,
    event_name,
    context_scope,
    fixture_id,
    kickoff_utc,
    is_home,
    event_flag,
    is_active,
    streak_type,
    composition,
    polarity,
    threshold,
    opponent_name,
    opponent_id,
    next_opponent_id,
    next_opponent_name,
    next_fixture_id,
    next_kickoff_utc,
    streak_length,
    current_streak_length,
    avg_goals_for_context,
    avg_goals_against_context,
    is_streak_eligible,
    is_suppressed,
    suppression_reason
from suppressed
