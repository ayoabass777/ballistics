{{ config(materialized='view') }}

-------------------------------------------------------------------------------
-- Mart: mart_homepage_top_streaks
-- Latest streak per team/event/context, filtered by minimum length rules and
-- enriched with relevance score for homepage ranking.
-------------------------------------------------------------------------------

with scored as (
    select *
    from {{ ref('fact_team_streak_relevance_score') }}
),

ranked as (
    select
        s.*,
        row_number() over (
            partition by team_id, event_name, context_scope
            order by kickoff_utc desc
        ) as rn
    from scored s
)

select
    streak_id,
    t.team_id,
    t.team_name,
    t.logo_file_id as team_logo_file_id,
    lc.league_season_id,
    lc.league_name,
    lc.league_logo_file_id,
    lc.country_name,
    lc.country_logo_file_id,
    event_name,
    streak_type,
    context_scope,
    current_streak_length as streak_length,
    next_fixture_id,
    next_kickoff_utc,
    next_opponent_id,
    next_opponent_name,
    --form
    relevance_score
from ranked
join {{ ref('dim_teams') }} t
  on ranked.team_id = t.team_id
join {{ ref('int_league_context') }} lc
  on ranked.league_season_id = lc.league_season_id
where rn = 1 and next_kickoff_utc < NOW() + INTERVAL '7 days' and next_kickoff_utc > NOW() and is_streak_eligible
ORDER BY relevance_score DESC, streak_length DESC, team_name ASC
