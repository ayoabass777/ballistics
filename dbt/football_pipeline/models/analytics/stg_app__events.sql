-- One row per usage event with the allow-listed properties as typed columns.
SELECT
    event_id,
    user_id,
    session_id,
    event_name,
    occurred_at,
    received_at,
    (props->>'fixture_id')::bigint AS fixture_id,
    (props->>'after_kickoff')::boolean AS after_kickoff,
    props->>'outcome' AS onboarding_outcome,
    (props->>'league_count')::int AS league_count,
    props->>'page' AS page,
    (props->>'followed_league_count')::int AS followed_league_count,
    props->>'tutorial_id' AS tutorial_id,
    props->>'action' AS tutorial_action,
    props->>'what' AS replayed,
    props->>'from_section' AS from_section,
    props->>'entry_route' AS entry_route,
    props->>'device' AS device
FROM {{ source('app', 'app_events') }}
