{{ config(
    materialized='incremental',
    unique_key='team_fixture_key',
    on_schema_change='append_new_columns'
) }}

with src as (
    select
        league_season_id,
        league_name,
        season,
        fixture_id,
        team_id,
        team_name,
        opponent_id,
        opponent_name,
        kickoff_utc,
        goals_for,
        goals_against,
        halftime_result,
        fulltime_result,
        is_home,
        case
            when is_home then 'home'
            else 'away'
        end as h2h_context_scope,
        is_current,
        is_played,
        updated_at,
        team_pair_key
    from {{ ref('fact_team_fixtures') }}
    where is_played
    {% if is_incremental() %}
      and updated_at > (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
    {% endif %}
),

with_keys as (
    select
        *,
        concat_ws('_', team_id, fixture_id) as team_fixture_key
    from src
)

select * from with_keys
