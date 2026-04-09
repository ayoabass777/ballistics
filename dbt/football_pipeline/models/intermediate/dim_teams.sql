{{ config (materialized='table', schema='int')}}

with teams as (
    select 
        team_id,
        api_team_id,
        initcap(team_name) as team_name,
        logo_file_id
    from {{ ref('stg_dim_teams') }}
),

standings as (
    select
        api_team_id,
        rank as standings_rank,
        points,
        games_played,
        wins,
        draws,
        losses,
        goals_for,
        goals_against,
        goal_diff,
        form,
        league_season_id
    from {{ ref('stg_raw_league_standings') }}
)

select
    t.team_id,
    t.api_team_id,
    t.team_name,
    concat_ws(
        '-',
        lower(regexp_replace(t.team_name, '\\s+', '-', 'g')),
        t.team_id
    ) as team_slug,
    t.logo_file_id,
    s.standings_rank as rank,
    s.points,
    s.games_played,
    s.wins,
    s.draws,
    s.losses,
    s.goals_for,
    s.goals_against,
    s.goal_diff,
    s.form,
    s.league_season_id
from teams t
join standings s
  on t.api_team_id = s.api_team_id
