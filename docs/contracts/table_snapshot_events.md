# Contract: Table Snapshot Events

Table snapshot events describe the state of selected raw tables after pipeline work completes.

## Producer

- Name: `airflow_table_snapshot`
- Current implementation: `etl/src/data_detector/table_snapshots.py`
- Table: `data_detector.table_snapshots`

## Snapshot Scope

Only these tables are currently in scope:

- `raw.raw_fixtures`
- `raw.raw_league_standings`

## Payload Fields

| Field | Required | Description |
|---|---|---|
| `table_schema` | yes | Schema being measured. |
| `table_name` | yes | Table being measured. |
| `row_count` | yes | Current row count. |
| `max_created_at` | no | Max `created_at` when present. |
| `max_updated_at` | no | Max `updated_at` when present. |
| `max_kickoff_utc` | no | Max `kickoff_utc` when present. |
| `details` | yes | JSON object for the pipeline context that triggered the snapshot. |

## Current Producers

- Bootstrap fixture loads snapshot `raw.raw_fixtures`.
- Incremental fixture loads snapshot `raw.raw_fixtures`.
- DLQ replay loads snapshot `raw.raw_fixtures`.
- Standings refresh snapshots `raw.raw_league_standings` with `leagues_updated` in `details`.
