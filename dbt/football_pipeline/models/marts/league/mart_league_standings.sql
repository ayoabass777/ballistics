{{ config(materialized='table') }}

-- League standings for the match page and league table page
-- (docs/product/match-detail-contract.md §3.2, D69).
--
-- Grain: league season x group x team, latest refresh only. One standings
-- refresh writes a league season in a single transaction, so its rows share
-- updated_at; rows from older refreshes (teams the API no longer returns, or
-- pre-2026-10-11 rows without a group) are left out.
--
-- Warehouse ids only. group_name is the API table label: one group for most
-- leagues, parallel conferences (MLS, USL), or split rounds (regular season,
-- championship / relegation round). standings_format comes from the league
-- definition; format_check flags leagues whose data disagrees with it.

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
    select league_season_id, count(distinct group_name) as n_groups
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
        lc.standings_format,
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
    case
        when standings_format is null and n_groups > 1 then 'undeclared_groups'
        when standings_format = 'single' and n_groups > 1 then 'single_has_groups'
        when standings_format in ('conferences', 'split') and n_groups = 1
            then 'declared_groups_has_one'
        else 'ok'
    end as format_check,
    -- The table to show for this team: its only group, or in a split season
    -- the latest round it appears in (last table in the API response).
    (group_order = team_last_group_order
        or team_last_group_order is null) as is_team_current_group
from with_context
