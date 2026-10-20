{{ config (materialized='table', schema='int')}}

with teams as (
    select 
        team_id,
        api_team_id,
        team_name,                 -- as the provider spells it (no initcap: "FC", "KV", accents)
        logo_file_id
    from {{ ref('stg_dim_teams') }}
),

standings as (
    select
        s.api_team_id,
        s.rank as standings_rank,
        s.points,
        s.games_played,
        s.wins,
        s.draws,
        s.losses,
        s.goals_for,
        s.goals_against,
        s.goal_diff,
        s.form,
        s.league_season_id,
        -- One row per team. A team has standings in several league seasons
        -- (past seasons, league + another competition) and, in split
        -- seasons, in two groups. Pick: current season, latest season,
        -- lowest tier, latest refresh, then the last table in the API
        -- response (the ETL's old last-table-wins behaviour).
        row_number() over (
            partition by s.api_team_id
            order by lc.is_current desc nulls last,
                     lc.season desc nulls last,
                     lc.tier asc nulls last,
                     s.updated_at desc,
                     s.group_order desc nulls last,
                     s.league_season_id desc
        ) as rn
    from {{ ref('stg_raw_league_standings') }} s
    left join {{ ref('int_league_context') }} lc
      on lc.league_season_id = s.league_season_id
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
 and s.rn = 1
