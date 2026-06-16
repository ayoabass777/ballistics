WITH ranked_snapshots AS (
    SELECT
        table_snapshot_id,
        pipeline_run_id,
        run_id,
        dag_id,
        table_schema,
        table_name,
        row_count,
        max_created_at,
        max_updated_at,
        max_kickoff_utc,
        details,
        snapshot_at,
        ROW_NUMBER() OVER (
            PARTITION BY table_schema, table_name
            ORDER BY snapshot_at DESC, table_snapshot_id DESC
        ) AS rn
    FROM {{ ref('stg_data_detector_table_snapshots') }}
),

latest_snapshots AS (
    SELECT *
    FROM ranked_snapshots
    WHERE rn = 1
),

open_stale_events AS (
    SELECT
        table_schema,
        table_name,
        COUNT(*) AS stale_event_count,
        jsonb_agg(
            jsonb_build_object(
                'check_name', check_name,
                'severity', severity,
                'detected_at', detected_at,
                'error_message', error_message,
                'details', details
            )
            ORDER BY detected_at DESC
        ) AS stale_event_details
    FROM {{ ref('stg_data_detector_data_quality_events') }}
    WHERE status = 'open'
      AND check_name IN ('raw_fixture_stale', 'standings_stale')
    GROUP BY table_schema, table_name
)

SELECT
    latest_snapshots.table_snapshot_id,
    latest_snapshots.pipeline_run_id,
    latest_snapshots.run_id,
    latest_snapshots.dag_id,
    latest_snapshots.table_schema,
    latest_snapshots.table_name,
    latest_snapshots.row_count,
    latest_snapshots.max_created_at,
    latest_snapshots.max_updated_at,
    latest_snapshots.max_kickoff_utc,
    latest_snapshots.snapshot_at,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - latest_snapshots.snapshot_at)) / 3600.0 AS snapshot_age_hours,
    CASE
        WHEN latest_snapshots.max_updated_at IS NOT NULL
            THEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - latest_snapshots.max_updated_at)) / 3600.0
    END AS data_freshness_age_hours,
    CASE
        WHEN latest_snapshots.table_schema = 'raw'
            AND latest_snapshots.table_name = 'raw_fixtures'
            AND latest_snapshots.max_updated_at < CURRENT_TIMESTAMP - INTERVAL '26 hours'
            THEN TRUE
        WHEN latest_snapshots.table_schema = 'raw'
            AND latest_snapshots.table_name = 'raw_league_standings'
            AND latest_snapshots.max_updated_at < CURRENT_TIMESTAMP - INTERVAL '7 days'
            THEN TRUE
        ELSE FALSE
    END AS freshness_threshold_exceeded,
    COALESCE(open_stale_events.stale_event_count, 0) > 0 AS is_stale,
    COALESCE(open_stale_events.stale_event_count, 0) AS stale_event_count,
    COALESCE(open_stale_events.stale_event_details, '[]'::jsonb) AS stale_event_details,
    CASE
        WHEN latest_snapshots.table_schema = 'raw' AND latest_snapshots.table_name = 'raw_fixtures' THEN '26 hours'
        WHEN latest_snapshots.table_schema = 'raw' AND latest_snapshots.table_name = 'raw_league_standings' THEN '7 days'
    END AS stale_threshold,
    latest_snapshots.details
FROM latest_snapshots
LEFT JOIN open_stale_events
    ON latest_snapshots.table_schema = open_stale_events.table_schema
    AND latest_snapshots.table_name = open_stale_events.table_name
