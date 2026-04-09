{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Mart: mart_team_fixture
-- Last 5 fixtures per team and context (overall/home/away) with JSON payload.
--------------------------------------------------------------------------------

with base as (
    select
        team_id,
        team_name,
        fixture_id,
        kickoff_utc,
        opponent_name,
        goals_for,
        goals_against,
        fulltime_result,
        is_home
    from {{ ref('fact_team_fixtures') }}
    where is_played
),

scoped as (
    select
        b.*,
        ctx.context_scope
    from base b
    cross join lateral (
        select context_scope
        from (values
            ('overall'::text),
            (case when b.is_home then 'home'::text end),
            (case when not b.is_home then 'away'::text end)
        ) as ctx(context_scope)
        where ctx.context_scope is not null
    ) ctx
),

ranked as (
    select
        *,
        row_number() over (
            partition by team_id, context_scope
            order by kickoff_utc desc, fixture_id desc
        ) as rn
    from scoped
)

select
    team_id,
    team_name,
    context_scope,
    json_agg(
        json_build_object(
            'date', kickoff_utc::date,
            'opponent', opponent_name,
            'score', concat_ws('-', goals_for, goals_against),
            'result', case
                when fulltime_result = 'win' then 'W'
                when fulltime_result = 'draw' then 'D'
                when fulltime_result = 'loss' then 'L'
                else null
            end,
            'isHome', is_home
        )
        order by kickoff_utc desc, fixture_id desc
    ) as fixtures_json
from ranked
where rn <= 5
group by team_id, team_name, context_scope
