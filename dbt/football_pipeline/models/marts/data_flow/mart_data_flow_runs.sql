WITH ranked_runs AS (
    SELECT
        pipeline_run_id,
        run_id,
        dag_id,
        task_id,
        run_type,
        status,
        started_at,
        finished_at,
        kafka_started_at,
        kafka_finished_at,
        kafka_topic,
        kafka_partition,
        kafka_start_offset,
        kafka_finish_offset,
        error_message,
        metadata,
        (metadata ->> 'data_interval_start')::timestamptz AS data_interval_start,
        (metadata ->> 'data_interval_end')::timestamptz AS data_interval_end,
        created_at,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY dag_id, run_id, task_id
            ORDER BY updated_at DESC, pipeline_run_id DESC
        ) AS rn
    FROM {{ ref('stg_data_detector_pipeline_runs') }}
)

SELECT
    pipeline_run_id,
    run_id,
    dag_id,
    task_id,
    run_type,
    status,
    started_at,
    finished_at,
    data_interval_start,
    data_interval_end,
    kafka_started_at,
    kafka_finished_at,
    kafka_topic,
    kafka_partition,
    kafka_start_offset,
    kafka_finish_offset,
    CASE
        WHEN started_at IS NOT NULL AND finished_at IS NOT NULL
            THEN EXTRACT(EPOCH FROM (finished_at - started_at))
    END AS task_duration_seconds,
    error_message,
    metadata,
    created_at,
    updated_at
FROM ranked_runs
WHERE rn = 1
