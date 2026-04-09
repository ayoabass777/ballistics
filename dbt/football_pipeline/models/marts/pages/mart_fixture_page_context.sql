{{ config(
    materialized='table',
    enabled=var('fixture_id', none) is not none
) }}

--------------------------------------------------------------------------------
-- Mart: mart_fixture_page_context
-- Context bundle for a single fixture page (parameterized by {{ var('fixture_id') }}).
--------------------------------------------------------------------------------

with fixture as (
    select
        fixture_id,
        fixture_slug,
        team_pair_key,
        home_team_id,
        away_team_id,
        fixture_info_json
    from {{ ref('mart_fixture_info') }}
    where fixture_id = {{ var('fixture_id', none) }}
),

fixture_streaks as (
    select
        fs.fixture_id,
        json_build_object(
            'home', json_agg(fs.fixture_streak_json order by fs.streak_length desc)
                filter (where fs.team_id = fs.home_team_id),
            'away', json_agg(fs.fixture_streak_json order by fs.streak_length desc)
                filter (where fs.team_id = fs.away_team_id)
        ) as fixture_streaks_json
    from {{ ref('mart_fixture_streaks') }} fs
    where fs.fixture_id = {{ var('fixture_id', none) }}
    group by fs.fixture_id
),

h2h_info as (
    select
        team_pair_key,
        current_fixture_id,
        h2h_info_json,
        h2h_streaks_json,
        h2h_matches_json,
        h2h_context_scopes_json
    from {{ ref('mart_head_to_head_info') }}
    where current_fixture_id = {{ var('fixture_id', none) }}
),

team_fixtures as (
    select
        f.home_team_id,
        f.away_team_id,
        tf.team_id,
        tf.team_name,
        tf.context_scope,
        tf.fixtures_json
    from {{ ref('mart_team_fixture') }} tf
    join fixture f
      on tf.team_id in (f.home_team_id, f.away_team_id)
),

team_fixtures_pivot as (
    select
        max(case when tf.team_id = tf.home_team_id then tf.team_id end) as home_team_id,
        max(case when tf.team_id = tf.home_team_id then tf.team_name end) as home_team_name,
        max(case when tf.team_id = tf.home_team_id and tf.context_scope = 'overall' then tf.fixtures_json::text end) as home_overall_fixtures,
        max(case when tf.team_id = tf.home_team_id and tf.context_scope = 'home' then tf.fixtures_json::text end) as home_home_fixtures,
        max(case when tf.team_id = tf.away_team_id then tf.team_id end) as away_team_id,
        max(case when tf.team_id = tf.away_team_id then tf.team_name end) as away_team_name,
        max(case when tf.team_id = tf.away_team_id and tf.context_scope = 'overall' then tf.fixtures_json::text end) as away_overall_fixtures,
        max(case when tf.team_id = tf.away_team_id and tf.context_scope = 'away' then tf.fixtures_json::text end) as away_away_fixtures
    from team_fixtures tf
),

team_fixtures_json as (
    select
        json_build_object(
            'home', json_build_object(
                'team_id', home_team_id,
                'team_name', home_team_name,
                'overall', home_overall_fixtures::json,
                'home', home_home_fixtures::json
            ),
            'away', json_build_object(
                'team_id', away_team_id,
                'team_name', away_team_name,
                'overall', away_overall_fixtures::json,
                'away', away_away_fixtures::json
            )
        ) as team_fixtures_json
    from team_fixtures_pivot
)

select
    f.fixture_id,
    f.fixture_slug,
    f.team_pair_key,
    f.home_team_id,
    f.away_team_id,
    f.fixture_info_json,
    s.fixture_streaks_json,
    h.h2h_info_json,
    h.h2h_streaks_json,
    h.h2h_matches_json,
    h.h2h_context_scopes_json,
    tf.team_fixtures_json
from fixture f
left join fixture_streaks s
  on s.fixture_id = f.fixture_id
left join h2h_info h
  on h.team_pair_key = f.team_pair_key
 and h.current_fixture_id = f.fixture_id
left join team_fixtures_json tf
  on true
