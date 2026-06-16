SELECT
    data_quality_event_id,
    pipeline_run_id,
    run_id,
    dag_id,
    task_id,
    severity,
    check_name,
    table_schema,
    table_name,
    source_s3_key,
    status,
    details,
    error_message,
    detected_at
FROM {{ source('data_detector', 'data_quality_events') }}
