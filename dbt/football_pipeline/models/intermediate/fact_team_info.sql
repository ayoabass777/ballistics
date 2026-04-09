{{ config(
    materialized='table',
    schema='int'
) }}

--------------------------------------------------------------------------------
-- Fact: fact_team_info
-- One row per team (latest fixture snapshot) enriched with standings and next
-- fixture pointers.
--------------------------------------------------------------------------------

with next_fixture as (
    select
        team_id,
        league_season_id,
        next_fixture_id,
        next_opponent_id,
        next_opponent_name,
        next_is_home,
        next_kickoff_utc,
        row_number() over (partition by team_id order by kickoff_utc desc, fixture_id desc) as rn
    from {{ ref('fact_team_fixtures') }}
    where is_played
),

latest_next as (
    select *
    from next_fixture
    where rn = 1
)

select
    dt.team_id,
    dt.team_name,
    dt.team_slug,
    dt.logo_file_id,
    dt.rank,
    dt.points,
    dt.games_played,
    dt.wins,
    dt.draws,
    dt.losses,
    dt.goals_for,
    dt.goals_against,
    dt.goal_diff,
    dt.form,
    dt.league_season_id,
    ln.next_fixture_id,
    ln.next_opponent_id,
    ln.next_opponent_name,
    ln.next_is_home,
    ln.next_kickoff_utc
from {{ ref('dim_teams') }} dt
left join latest_next ln
  on dt.team_id = ln.team_id
  and dt.league_season_id = ln.league_season_id
