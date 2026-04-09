{{ config(
    materialized='view'
) }}

--------------------------------------------------------------------------------
-- Intermediate: fact_team_events
-- Explodes each team_fixture_stats row into event flags per fixture
--------------------------------------------------------------------------------

WITH fixtures AS (
    SELECT
        league_season_id,
        league_name,
        season,
        fixture_id,
        team_id,
        team_name,
        opponent_id,
        opponent_name,
        team_pair_key,
        kickoff_utc,
        goals_for,
        goals_against,
        halftime_goals_for,
        halftime_goals_against,
        fulltime_result,
        halftime_result,
        is_home,
        is_current,
        is_played,
        next_opponent_id,
        next_opponent_name,
        next_fixture_id,
        next_kickoff_utc,
        next_is_home,
        updated_at
    FROM {{ ref('fact_team_fixtures') }}
    WHERE is_played
    {% if is_incremental() %}
      AND updated_at > (SELECT max(updated_at) FROM {{ this }})
    {% endif %}
),

base AS (
    SELECT
        league_season_id,
        league_name,
        season,
        fixture_id,
        team_id,
        team_name,
        opponent_id,
        opponent_name,
        team_pair_key,
        kickoff_utc,
        goals_for,
        goals_against,
        halftime_goals_for,
        halftime_goals_against,
        fulltime_result,
        halftime_result,
        is_home,
        is_current,
        is_played,
        next_opponent_name,
        next_opponent_id,
        next_fixture_id,
        next_kickoff_utc,
        next_is_home,
        updated_at
    FROM fixtures
),

event_ind AS (
    SELECT
        b.league_season_id,
        b.league_name,
        b.season,
        b.fixture_id,
        b.team_id,
        b.team_name,
        b.opponent_id,
        b.opponent_name,
        b.team_pair_key,
        b.kickoff_utc,
        b.goals_for,
        b.goals_against,
        b.fulltime_result,
        ev.event_name,
        ev.event_flag,
        (ev.event_flag = 1) as is_active,
        ev.streak_type,
        ev.polarity,
        ev.composition,
        ev.threshold,
        b.is_home,
        b.is_current,
        b.is_played,
        ctx.context_scope,
        avg(b.goals_for) over (
            partition by b.team_id, ctx.context_scope
        ) as avg_goals_for_context,
        avg(b.goals_against) over (
            partition by b.team_id, ctx.context_scope
        ) as avg_goals_against_context,
        b.next_opponent_name,
        b.next_opponent_id,
        b.next_fixture_id,
        b.next_kickoff_utc,
        b.next_is_home,
        b.updated_at
    FROM base b
    CROSS JOIN LATERAL (
        SELECT *
        FROM (VALUES
            ('score_1goal',        (b.goals_for >= 1)::int,   'goal',  'positive', 'single', 1),
            ('score_2goals',       (b.goals_for >= 2)::int,   'goal',  'positive', 'single', 2),
            ('concede_1',          (b.goals_against >= 1)::int,   'goal',  'negative', 'single', 1),
            ('concede_2',          (b.goals_against >= 2)::int,   'goal',  'negative', 'single', 2),
            ('goalless',           (b.goals_for = 0)::int,        'goal', 'negative', 'single', null),
            ('Over_1_5',           ((b.goals_for + b.goals_against) > 1.5)::int,  'goal', null, null, null),
            ('Over_2_5',           ((b.goals_for + b.goals_against) > 2.5)::int,  'goal', null, null, null),
            ('Over_3_5',           ((b.goals_for + b.goals_against) > 3.5)::int,  'goal', null, null, null),
            ('Over_4_5',           ((b.goals_for + b.goals_against) > 4.5)::int,  'goal', null, null, null),
            ('Under_1_5',          ((b.goals_for + b.goals_against) < 1.5)::int,  'goal', null, null, null),
            ('Under_2_5',          ((b.goals_for + b.goals_against) < 2.5)::int,  'goal', null, null, null),
            ('Under_3_5',          ((b.goals_for + b.goals_against) < 3.5)::int,  'goal', null, null, null),
            ('Under_4_5',          ((b.goals_for + b.goals_against) < 4.5)::int,  'goal', null, null, null),
            ('clean_sheet',        (b.goals_against = 0)::int,    'goal', 'positive', 'single', null),
            --('no_scoring',         (b.goals_for = 0)::int,         'goal',  'negative', 'single', null),
            ('win',                (b.fulltime_result = 'win')::int,           'result',   'positive', 'single', null),
            ('draw',               (b.fulltime_result = 'draw')::int,          'result',   'neutral', 'single', null),
            ('loss',               (b.fulltime_result = 'loss')::int,          'result',    'negative', 'single', null),
            ('unbeaten',           (b.fulltime_result in ('win','draw'))::int,  'result',   'positive', 'single', null),
            ('winless',            (b.fulltime_result in ('draw','loss'))::int, 'result',   'negative', 'single', null),
            ('halftime_win',       (b.halftime_result = 'win')::int,            'result',   'positive',       null,    null),
            ('BTTS', ((b.goals_for > 0) AND (b.goals_against > 0))::int, 'goal', null, null, null)
        ) AS ev(event_name, event_flag, streak_type, polarity, composition, threshold)
    ) ev
    CROSS JOIN LATERAL (
        SELECT ctx.context_scope
        FROM (VALUES
            ('all'::text),
            (CASE WHEN b.is_home THEN 'home'::text END),
            (CASE WHEN NOT b.is_home THEN 'away'::text END)
        ) AS ctx(context_scope)
        WHERE ctx.context_scope IS NOT NULL
    ) ctx
)

SELECT * FROM event_ind
