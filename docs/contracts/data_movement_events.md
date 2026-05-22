# Contract: Data Movement Events

Data movement events describe data crossing a storage or processing boundary.

## Producer

- Name: `airflow_data_movement`
- Current implementation: `etl/src/data_detector/data_movements.py`
- Table: `data_detector.data_movements`

## Event Types

The `movement_type` field is the event type.

- `api_to_s3`
- `s3_to_raw`
- `dlq_replay_to_s3`
- `raw_correction`

## Payload Fields

| Field | Required | Description |
|---|---|---|
| `movement_type` | yes | Boundary crossed by the data. |
| `source_system` | yes | Producer system, such as `api_sports` or `s3`. |
| `source_name` | no | Logical source name, such as `fixtures:39:2025`. |
| `source_s3_key` | no | S3 key when a landing object is involved. |
| `target_schema` | no | Destination schema when loading to Postgres. |
| `target_table` | no | Destination table when loading to Postgres. |
| `row_count` | no | Number of records represented by the movement. |
| `inserted_count` | no | Number of inserted records when known. |
| `updated_count` | no | Number of updated records when known. |
| `deleted_count` | no | Number of deleted records when known. |
| `failed_count` | yes | Number of failed movement units. Defaults to `0`. |
| `watermark_column` | no | Data-time column used for coverage. Fixture movements use `kickoff_utc`. |
| `watermark_min` | no | Earliest data-time covered by the movement. |
| `watermark_max` | no | Latest data-time covered by the movement. |
| `status` | yes | `success` or `failed`. |
| `details` | yes | JSON object for source-specific context. |

## Current Producers

- Bootstrap fixture extraction records `api_to_s3`.
- Bootstrap fixture load records `s3_to_raw`.
- Incremental fixture extraction records `api_to_s3`.
- Incremental fixture load records `s3_to_raw`.
- Fixture correction records `raw_correction`.
- DLQ replay records `dlq_replay_to_s3` and `s3_to_raw`.
