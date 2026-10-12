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
    from {{ ref('fact_team_fixtures') }} tf
    where is_played
    {% if is_incremental() %}
      -- New or changed rows, plus any played row not yet here. The second
      -- branch catches rows that arrive upstream late with an old updated_at
      -- (e.g. after a backfill), which the watermark alone skips forever.
      and (
        updated_at > (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
        or not exists (
          select 1 from {{ this }} h
          where h.fixture_id = tf.fixture_id and h.team_id = tf.team_id
        )
      )
    {% endif %}
),

with_keys as (
    select
        *,
        concat_ws('_', team_id, fixture_id) as team_fixture_key
    from src
)

select * from with_keys
