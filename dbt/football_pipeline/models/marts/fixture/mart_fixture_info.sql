{{ config(materialized='table') }}

--------------------------------------------------------------------------------
-- Mart: mart_fixture_info
-- Fixture-level payload with team info JSON (joined via fixture_id + team_id)
--------------------------------------------------------------------------------

with fixture_teams as (
    select
        f.fixture_id,
        f.is_home,
        f.team_pair_key,
        t.team_id,
        t.team_name,
        t.team_slug,
        t.logo_file_id,
        t.form,
        json_build_object(
            'team_id', t.team_id,
            'team_name', t.team_name,
            'shortname', null,
            'team_slug', t.team_slug,
            'logo_file_id', t.logo_file_id,
            'form', t.form
        ) as team_info
    from {{ ref('fact_team_fixtures') }} f
    join {{ ref('mart_team_info') }} t
      on t.team_id = f.team_id
     and t.next_fixture_id = f.fixture_id
),

fixture_rollup as (
    select
        fixture_id,
        max(team_pair_key) as team_pair_key,
        max(case when is_home then team_id end) as home_team_id,
        max(case when not is_home then team_id end) as away_team_id,
        max(case when is_home then team_slug end) as home_team_slug,
        max(case when not is_home then team_slug end) as away_team_slug,
        (array_agg(team_info) filter (where is_home))[1] as home_team_info,
        (array_agg(team_info) filter (where not is_home))[1] as away_team_info
    from fixture_teams
    group by fixture_id
)

select
    fixture_id,
    team_pair_key,
    home_team_id,
    away_team_id,
    concat_ws('-', home_team_slug, away_team_slug, fixture_id) as fixture_slug,
    json_build_object(
        'fixture_id', fixture_id,
        'fixture_slug', concat_ws('-', home_team_slug, away_team_slug, fixture_id),
        'team_info', json_build_object(
            'home', home_team_info,
            'away', away_team_info
        )
    ) as fixture_info_json
from fixture_rollup
