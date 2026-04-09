{{ config(materialized='table') }}

--------------------------------------------------------------------------------
-- Mart: mart_head_to_head_info
-- H2H summary info per current fixture (team_pair_key) with JSON payloads.
--------------------------------------------------------------------------------

with current_fixtures as (
    select
        fixture_id as current_fixture_id,
        team_pair_key,
        home_team_id,
        away_team_id
    from {{ ref('mart_fixture_info') }}
),

h2h_latest as (
    select *
    from {{ ref('fact_head_to_head_latest') }}
),

h2h_totals as (
    select
        c.team_pair_key,
        c.current_fixture_id,
        max(
            case
                when h.h2h_context_scope = 'all'
                 and h.event_name = 'win'
                 and h.team_id = c.home_team_id
                then h.total_event_count
            end
        ) as total_matches,
        max(
            case
                when h.h2h_context_scope = 'all'
                 and h.event_name = 'win'
                 and h.team_id = c.home_team_id
                then h.total_event_flag_sum
            end
        ) as home_wins,
        max(
            case
                when h.h2h_context_scope = 'all'
                 and h.event_name = 'draw'
                 and h.team_id = c.home_team_id
                then h.total_event_flag_sum
            end
        ) as draws,
        max(
            case
                when h.h2h_context_scope = 'all'
                 and h.event_name = 'win'
                 and h.team_id = c.away_team_id
                then h.total_event_flag_sum
            end
        ) as away_wins
    from current_fixtures c
    join h2h_latest h
      on h.team_pair_key = c.team_pair_key
    group by c.team_pair_key, c.current_fixture_id
)

select
    t.team_pair_key,
    t.current_fixture_id,
    json_build_object(
        'totalMatches', t.total_matches,
        'homeWins', t.home_wins,
        'draws', t.draws,
        'awayWins', t.away_wins
    ) as h2h_info_json,
    s.h2h_streaks_json,
    m.h2h_matches_json,
    m.h2h_context_scopes_json
from h2h_totals t
left join {{ ref('mart_head_to_head_streaks') }} s
  on s.team_pair_key = t.team_pair_key
left join {{ ref('mart_head_to_head_matches') }} m
  on m.team_pair_key = t.team_pair_key
 and m.current_fixture_id = t.current_fixture_id
