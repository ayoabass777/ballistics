{{ config(materialized='table') }}

--------------------------------------------------------------------------------
-- Mart: mart_head_to_head_matches
-- Head-to-head match list per current fixture (team_pair_key).
--------------------------------------------------------------------------------

with current_fixtures as (
    select
        fixture_id as current_fixture_id,
        team_pair_key,
        home_team_id as current_home_team_id
    from {{ ref('mart_fixture_info') }}
),

matches as (
    select
        h.fixture_id,
        h.team_pair_key,
        max(h.kickoff_utc)::date as kickoff_date,
        max(case when h.is_home then h.team_id end) as home_team_id,
        max(case when not h.is_home then h.team_id end) as away_team_id,
        max(case when h.is_home then h.goals_for end) as home_goals_for,
        max(case when not h.is_home then h.goals_for end) as away_goals_for
    from {{ ref('fact_head_to_head') }} h
    group by h.fixture_id, h.team_pair_key
),

matches_with_result as (
    select
        m.*,
        case
            when m.home_goals_for > m.away_goals_for then 'H'
            when m.home_goals_for = m.away_goals_for then 'D'
            else 'A'
        end as result
    from matches m
),

ranked as (
    select
        c.team_pair_key,
        c.current_fixture_id,
        m.fixture_id as h2h_fixture_id,
        m.kickoff_date,
        m.home_goals_for,
        m.away_goals_for,
        m.result,
        (m.home_team_id = c.current_home_team_id) as is_home_team_home,
        h.h2h_context_scope,
        row_number() over (
            partition by c.current_fixture_id
            order by m.kickoff_date desc, m.fixture_id desc
        ) as rn
    from matches_with_result m
    join current_fixtures c
      on c.team_pair_key = m.team_pair_key
    left join {{ ref('fact_head_to_head') }} h
      on h.fixture_id = m.fixture_id
     and h.team_pair_key = m.team_pair_key
     and h.team_id = c.current_home_team_id
)

select
    team_pair_key,
    current_fixture_id,
    json_agg(
        json_build_object(
            'date', kickoff_date,
            'home', home_goals_for,
            'away', away_goals_for,
            'result', result,
            'isHomeTeamHome', is_home_team_home
        )
        order by kickoff_date desc, h2h_fixture_id desc
    ) as h2h_matches_json,
    json_agg(h2h_context_scope order by kickoff_date desc, h2h_fixture_id desc) as h2h_context_scopes_json
from ranked
where rn <= 5
group by team_pair_key, current_fixture_id
