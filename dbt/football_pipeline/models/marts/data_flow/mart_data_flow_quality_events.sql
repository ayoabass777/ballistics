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
FROM {{ ref('stg_data_detector_data_quality_events') }}
WHERE severity IN ('warning', 'error', 'critical')
  AND status IN ('open', 'failed')
