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
    snapshot_at
FROM {{ source('data_detector', 'table_snapshots') }}
