# Contract: Data Quality Events

Data quality events describe detected data health issues.

## Producer

- Name: `airflow_data_quality`
- Current implementation: `etl/src/data_detector/quality_events.py`
- Table: `data_detector.data_quality_events`

## Payload Fields

| Field | Required | Description |
|---|---|---|
| `severity` | yes | `info`, `warning`, or `error`. |
| `check_name` | yes | Stable detector check name. |
| `table_schema` | no | Affected schema when table-specific. |
| `table_name` | no | Affected table when table-specific. |
| `source_s3_key` | no | Related S3 key when applicable. |
| `status` | yes | Event lifecycle state, currently `open`. |
| `details` | yes | JSON object with check-specific evidence. |
| `error_message` | no | Human-readable failure or warning message. |

## Current Checks

- `s3_load_failed`: error when a fixture landing key fails to load and is sent to DLQ.
- `dlq_not_empty`: warning when DLQ entries remain after replay.
- `raw_fixture_stale`: warning only when current seasons exist, past unresolved fixtures exist, and `raw.raw_fixtures.max(updated_at)` is older than 26 hours.
- `standings_stale`: warning when any current league has no standings update within 7 days. Details include `stale_days`, `stale_league_count`, and `stale_leagues`.
