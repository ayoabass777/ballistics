{{ config(
    materialized='view',
    schema='int'
) }}

with teams as (
    select
        'team'::text as entity_type,
        t.team_id as entity_id,
        t.team_name as name,
        lower(t.team_name) as name_lc,
        t.logo_file_id as logo_file_id
        --null::boolean as is_current
    from {{ ref('dim_teams') }} t
),

leagues as (
    select
        'league'::text as entity_type,
        lc.league_season_id as entity_id,
        lc.league_name as name,
        lower(lc.league_name) as name_lc,
        lc.league_logo_file_id as logo_file_id
        --lc.is_current
    from {{ ref('int_league_context') }} lc
)

select * from teams
union all
select * from leagues
