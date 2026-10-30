-- One row per member: the beta-recruitment funnel (docs/product/beta-analytics.md).
-- Email is used only to join invitations and is never selected.
WITH events AS (
    SELECT * FROM {{ ref('stg_app__events') }}
),

invites AS (
    SELECT lower(email) AS email, min(created_at) AS invited_at, min(claimed_at) AS accepted_at
    FROM {{ source('app', 'beta_invite_codes') }}
    GROUP BY 1
),

onboarding AS (
    SELECT DISTINCT ON (user_id) user_id, occurred_at AS onboarded_at, onboarding_outcome
    FROM events
    WHERE event_name = 'onboarding_finished'
    ORDER BY user_id, occurred_at
),

activity AS (
    SELECT
        user_id,
        count(DISTINCT session_id) AS sessions,
        min(occurred_at) FILTER (WHERE event_name = 'match_opened') AS first_match_opened_at,
        count(*) FILTER (WHERE event_name = 'match_opened') AS matches_opened,
        count(*) FILTER (WHERE event_name = 'fixture_unsaved') AS fixtures_unsaved,
        count(*) FILTER (WHERE event_name = 'tutorial_dismissed' AND tutorial_action = 'skip') AS tutorials_skipped,
        count(*) FILTER (WHERE event_name = 'methodology_opened') AS methodology_opens,
        bool_or(device = 'mobile') AS used_mobile,
        count(*) FILTER (WHERE event_name = 'request_failed') AS requests_failed,
        count(*) FILTER (WHERE event_name = 'client_error') AS client_errors,
        count(*) FILTER (WHERE event_name = 'retry_clicked') AS retries,
        count(*) FILTER (WHERE event_name = 'search_used') AS searches,
        count(*) FILTER (WHERE event_name = 'search_used' AND result_count = 0) AS empty_searches,
        count(*) FILTER (WHERE event_name = 'filter_changed') AS filter_changes,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY speed_ms) FILTER (WHERE speed_metric = 'lcp') AS lcp_p75_ms,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY speed_ms) FILTER (WHERE speed_metric = 'inp') AS inp_p75_ms,
        count(*) FILTER (WHERE event_name = 'page_speed' AND speed_rating = 'poor') AS poor_speed_readings,
        max(occurred_at) AS last_seen_at
    FROM events
    GROUP BY user_id
),

leagues AS (
    SELECT user_id, min(created_at) AS leagues_saved_at, count(*) AS leagues_followed
    FROM {{ source('app', 'user_followed_leagues') }}
    GROUP BY user_id
),

saves AS (
    SELECT user_id, min(saved_at) AS first_saved_at, count(*) AS fixtures_saved
    FROM {{ source('app', 'saved_fixtures') }}
    GROUP BY user_id
),

-- Fixtures a member looked at or saved before kickoff.
before_kickoff AS (
    SELECT user_id, fixture_id FROM events WHERE event_name = 'match_opened' AND NOT after_kickoff
    UNION
    SELECT user_id, fixture_id FROM {{ source('app', 'saved_fixtures') }}
),

returned AS (
    SELECT e.user_id, min(e.occurred_at) AS returned_after_kickoff_at
    FROM events e
    JOIN before_kickoff b ON b.user_id = e.user_id AND b.fixture_id = e.fixture_id
    WHERE e.event_name = 'match_opened' AND e.after_kickoff
    GROUP BY e.user_id
)

SELECT
    u.user_id,
    i.invited_at,
    i.accepted_at,
    u.created_at AS account_created_at,
    o.onboarded_at,
    o.onboarding_outcome,
    l.leagues_saved_at,
    coalesce(l.leagues_followed, 0) AS leagues_followed,
    a.first_match_opened_at,
    coalesce(a.matches_opened, 0) AS matches_opened,
    s.first_saved_at,
    coalesce(s.fixtures_saved, 0) AS fixtures_saved,
    coalesce(a.fixtures_unsaved, 0) AS fixtures_unsaved,
    r.returned_after_kickoff_at,
    coalesce(a.sessions, 0) AS sessions,
    coalesce(a.tutorials_skipped, 0) AS tutorials_skipped,
    coalesce(a.methodology_opens, 0) AS methodology_opens,
    coalesce(a.used_mobile, false) AS used_mobile,
    coalesce(a.requests_failed, 0) AS requests_failed,
    coalesce(a.client_errors, 0) AS client_errors,
    coalesce(a.retries, 0) AS retries,
    coalesce(a.searches, 0) AS searches,
    coalesce(a.empty_searches, 0) AS empty_searches,
    coalesce(a.filter_changes, 0) AS filter_changes,
    a.lcp_p75_ms,
    a.inp_p75_ms,
    coalesce(a.poor_speed_readings, 0) AS poor_speed_readings,
    a.last_seen_at,
    coalesce(p.usage_events_enabled, true) AS usage_events_enabled
FROM {{ source('app', 'app_users') }} u
LEFT JOIN invites i ON i.email = lower(u.email)
LEFT JOIN onboarding o ON o.user_id = u.user_id
LEFT JOIN activity a ON a.user_id = u.user_id
LEFT JOIN leagues l ON l.user_id = u.user_id
LEFT JOIN saves s ON s.user_id = u.user_id
LEFT JOIN returned r ON r.user_id = u.user_id
LEFT JOIN {{ source('app', 'user_preferences') }} p ON p.user_id = u.user_id
