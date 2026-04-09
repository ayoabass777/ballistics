{{ config(
    materialized='table',
    enabled=var('team_id', none) is not none
) }}

--------------------------------------------------------------------------------
-- Mart: mart_team_page_context
-- Context bundle for a single team page (parameterized by {{ var('team_id') }}).
--------------------------------------------------------------------------------

with league_peers as (
    select
        league_season_id,
        json_agg(
            json_build_object(
                'teamId', team_id,
                'name', team_name,
                'teamHref', team_slug,
                'logoFileId', logo_file_id
            )
            order by team_rank
        ) as league_teams_json
    from {{ ref('mart_league_teams') }}
    where team_id != {{ var('team_id') }}
    group by league_season_id
),

team_streaks as (
    select
        team_id,
        json_agg(
            json_build_object(
                'teamId', team_id,
                'streakId', streak_id,
                'eventName', event_name,
                'streakType', streak_type,
                'streakLength', streak_length,
                'polarity', polarity,
                'scope', scope
            )
            order by streak_length desc, event_name
        ) as streaks_json
    from {{ ref('mart_team_streaks') }}
    where team_id = {{ var('team_id') }}
    group by team_id
)

select
  t.team_id,
  t.team_name,
  t.team_slug,
  t.rank,
  t.form,
  l.league_name,
  l.season_label,
  l.country_name,
  teams.league_teams_json,
  streaks.streaks_json,
  -- upcoming from t.next_* fields (already present on mart_team_info)
  t.next_fixture_id,
  t.next_opponent_id,
  t.next_opponent_name,
  t.next_is_home,
  t.next_kickoff_utc
from {{ ref('mart_team_info') }} t
left join {{ ref('mart_league_info') }} l using (league_season_id)
left join league_peers teams
  on teams.league_season_id = t.league_season_id
left join team_streaks streaks
  on streaks.team_id = t.team_id
where t.team_id = {{ var('team_id') }}
