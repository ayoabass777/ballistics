{{ config(materialized='table') }}

-- League standings for the match page and league table page
-- (docs/product/match-detail-contract.md §3.2, D69).
--
-- Grain: league season x group x team, latest refresh only. One standings
-- refresh writes a league season in a single transaction, so its rows share
-- updated_at; rows from older refreshes (teams the API no longer returns, or
-- pre-2026-10-11 rows without a group) are left out.
--
-- Warehouse ids only. group_name is the API table label (or "Table N").
-- standings_format is derived here, per league season, from the standings
-- themselves (no league config), so a league that changes format between
-- seasons is handled:
--   single       one group
--   split        several groups and some team appears in more than one
--                (regular season + championship / relegation rounds)
--   conferences  several groups, each team in exactly one (MLS, USL)

with standings as (
    select * from {{ ref('stg_raw_league_standings') }}
),

latest as (
    select league_season_id, max(updated_at) as standings_updated_at
    from standings
    group by league_season_id
),

current_rows as (
    select s.*, l.standings_updated_at
    from standings s
    join latest l
      on l.league_season_id = s.league_season_id
     and s.updated_at = l.standings_updated_at
),

group_stats as (
    select
        league_season_id,
        group_name,
        count(*) <> count(distinct rank) as ranks_repeat
    from current_rows
    group by league_season_id, group_name
),

season_stats as (
    select
        league_season_id,
        count(distinct group_name) as n_groups,
        case
            when count(distinct group_name) = 1 then 'single'
            when count(*) > count(distinct api_team_id) then 'split'
            else 'conferences'
        end as standings_format
    from current_rows
    group by league_season_id
),

teams as (
    select api_team_id, team_id
    from {{ ref('stg_dim_teams') }}
),

with_context as (
    select
        c.*,
        lc.league_id,
        ss.standings_format,
        ss.n_groups,
        gs.ranks_repeat,
        t.team_id,
        max(c.group_order) over (
            partition by c.league_season_id, c.api_team_id
        ) as team_last_group_order
    from current_rows c
    join season_stats ss on ss.league_season_id = c.league_season_id
    join group_stats gs
      on gs.league_season_id = c.league_season_id
     and gs.group_name = c.group_name
    left join {{ ref('int_league_context') }} lc
      on lc.league_season_id = c.league_season_id
    left join teams t on t.api_team_id = c.api_team_id
)

select
    league_season_id,
    league_id,
    season,
    group_name,
    group_order,
    team_id,
    team_name,
    rank,
    games_played,
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    goal_diff,
    points,
    form,
    description,
    standings_updated_at,
    standings_format,
    n_groups,
    ranks_repeat,
    -- The table to show for this team: its only group, or in a split season
    -- the latest round it appears in (last table in the API response).
    (group_order = team_last_group_order
        or team_last_group_order is null) as is_team_current_group
from with_context
