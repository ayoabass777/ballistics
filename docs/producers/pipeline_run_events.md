# Producer Contract: Pipeline Run Events

This contract defines the task lifecycle events emitted by Airflow for the `data_detector.pipeline_runs` table.

## Producer

- Name: `airflow_task_lifecycle`
- Current implementation: `etl/src/data_detector/pipeline_runs.py`
- Producer hooks:
  - `pipeline_run_started`
  - `pipeline_run_succeeded`
  - `pipeline_run_failed`
- Future Kafka topic: `ballistics.pipeline.events`

## Event Types

- `pipeline_task_started`
- `pipeline_task_succeeded`
- `pipeline_task_failed`

## Event Payload

```json
{
  "event_type": "pipeline_task_started",
  "event_time": "2026-05-21T07:01:13Z",
  "run_id": "scheduled__2026-05-21",
  "dag_id": "fixture_update_dag",
  "task_id": "load_updates",
  "run_type": "scheduled",
  "status": "running",
  "error_message": null,
  "metadata": {
    "try_number": 1,
    "map_index": -1,
    "logical_date": "2026-05-21T00:00:00Z",
    "data_interval_start": "2026-05-20T00:00:00Z",
    "data_interval_end": "2026-05-21T00:00:00Z"
  }
}
```

## Field Rules

| Field | Required | Description |
|---|---|---|
| `event_type` | yes | One of `pipeline_task_started`, `pipeline_task_succeeded`, `pipeline_task_failed`. |
| `event_time` | yes | Producer timestamp computed when the Airflow callback creates the event. |
| `run_id` | yes | Airflow DAG run identifier. |
| `dag_id` | yes | Airflow DAG identifier. |
| `task_id` | yes | Airflow task identifier. |
| `run_type` | no | Airflow run type, such as `scheduled`, `manual`, or `backfill`. |
| `status` | yes | Normalized task state: `running`, `success`, or `failed`. |
| `error_message` | no | Failure reason for failed events. |
| `metadata` | yes | JSON object for Airflow context fields that should not become first-class columns yet. |

## Table Mapping

| Event field | `data_detector.pipeline_runs` column |
|---|---|
| `event_time` on `pipeline_task_started` | `started_at` |
| `event_time` on `pipeline_task_succeeded` | `finished_at` |
| `event_time` on `pipeline_task_failed` | `finished_at` |
| `run_id` | `run_id` |
| `dag_id` | `dag_id` |
| `task_id` | `task_id` |
| `run_type` | `run_type` |
| `status` | `status` |
| `error_message` | `error_message` |
| `metadata` | `metadata` |

## Kafka Metadata Mapping

When events are transported through Kafka, the detector consumer should preserve Kafka broker metadata separately:

| Kafka metadata | `data_detector.pipeline_runs` column |
|---|---|
| start event broker timestamp | `kafka_started_at` |
| finish event broker timestamp | `kafka_finished_at` |
| topic | `kafka_topic` |
| partition | `kafka_partition` |
| start event offset | `kafka_start_offset` |
| finish event offset | `kafka_finish_offset` |

Kafka time must not replace `started_at` or `finished_at`; those are producer event times.
