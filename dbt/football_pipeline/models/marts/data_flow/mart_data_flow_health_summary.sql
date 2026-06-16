WITH latest_task_runs AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY dag_id, task_id
                ORDER BY COALESCE(finished_at, started_at, updated_at) DESC, pipeline_run_id DESC
            ) AS rn
        FROM {{ ref('mart_data_flow_runs') }}
    ) ranked
    WHERE rn = 1
),

failed_tasks AS (
    SELECT *
    FROM latest_task_runs
    WHERE status = 'failed'
),

stale_tables AS (
    SELECT *
    FROM {{ ref('mart_data_flow_table_freshness') }}
    WHERE is_stale
),

open_quality_events AS (
    SELECT *
    FROM {{ ref('mart_data_flow_quality_events') }}
),

latest_movement AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                ORDER BY latest_detected_at DESC, movement_event_count DESC
            ) AS rn
        FROM {{ ref('mart_data_flow_movements') }}
    ) ranked
    WHERE rn = 1
),

failed_task_summary AS (
    SELECT
        COUNT(*) AS failed_task_count,
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'dag_id', dag_id,
                    'task_id', task_id,
                    'run_id', run_id,
                    'finished_at', finished_at,
                    'error_message', error_message
                )
                ORDER BY finished_at DESC NULLS LAST
            ),
            '[]'::jsonb
        ) AS failed_tasks
    FROM failed_tasks
),

stale_table_summary AS (
    SELECT
        COUNT(*) AS stale_table_count,
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'table_schema', table_schema,
                    'table_name', table_name,
                    'row_count', row_count,
                    'max_updated_at', max_updated_at,
                    'snapshot_at', snapshot_at,
                    'stale_threshold', stale_threshold
                )
                ORDER BY table_schema, table_name
            ),
            '[]'::jsonb
        ) AS stale_tables
    FROM stale_tables
),

quality_event_summary AS (
    SELECT
        COUNT(*) AS open_quality_event_count,
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'severity', severity,
                    'check_name', check_name,
                    'dag_id', dag_id,
                    'task_id', task_id,
                    'table_schema', table_schema,
                    'table_name', table_name,
                    'source_s3_key', source_s3_key,
                    'detected_at', detected_at,
                    'error_message', error_message
                )
                ORDER BY detected_at DESC
            ),
            '[]'::jsonb
        ) AS open_quality_events
    FROM open_quality_events
)

SELECT
    CURRENT_TIMESTAMP AS checked_at,
    (
        failed_task_summary.failed_task_count = 0
        AND stale_table_summary.stale_table_count = 0
        AND quality_event_summary.open_quality_event_count = 0
    ) AS is_pipeline_healthy,
    failed_task_summary.failed_task_count,
    failed_task_summary.failed_tasks,
    stale_table_summary.stale_table_count,
    stale_table_summary.stale_tables,
    quality_event_summary.open_quality_event_count,
    quality_event_summary.open_quality_events,
    latest_movement.latest_detected_at AS most_recent_movement_at,
    latest_movement.movement_type AS most_recent_movement_type,
    latest_movement.status AS most_recent_movement_status,
    latest_movement.source_system AS most_recent_source_system,
    latest_movement.source_name AS most_recent_source_name,
    latest_movement.source_s3_key AS most_recent_source_s3_key,
    latest_movement.target_schema AS most_recent_target_schema,
    latest_movement.target_table AS most_recent_target_table,
    latest_movement.rows_moved AS most_recent_rows_moved,
    latest_movement.failed_units AS most_recent_failed_units
FROM failed_task_summary
CROSS JOIN stale_table_summary
CROSS JOIN quality_event_summary
LEFT JOIN latest_movement ON TRUE
