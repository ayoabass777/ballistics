# Pipeline Stabilization Context

This note records the operational reasoning behind the stabilization work so future changes do not have to rely on chat history.

## Bootstrap metadata mtime

`mtime` is the filesystem modified timestamp for `metadata.yaml`. The bootstrap DAG uses it to decide whether the league configuration changed since the last successful bootstrap.

The sensor should only detect a newer mtime and trigger the DAG. It should not persist `metadata_last_mtime`. The terminal `mark_metadata_processed` task records that Airflow Variable only after schema bootstrap, metadata extraction, fixture extraction, and fixture loading have succeeded. If any downstream task fails, the stored mtime remains old and the next scheduled run can retry without requiring a manual touch of `metadata.yaml`.

Fixture extraction failures and fixture load failures both write DLQ entries. Load failures include the failed landing-zone `source_s3_key` and `failure_stage="load"` in the DLQ payload before the bootstrap task fails, so the replay DAG can still re-fetch and reload that league-season later.

## Portable PROJECT_ROOT

Airflow's `DockerOperator` launches a separate dbt container and bind-mounts dbt project files from the host. That requires a host-visible repository path, so `PROJECT_ROOT` must come from the environment instead of a developer-specific absolute path.

Set `PROJECT_ROOT` in `.env` to the repository root path, for example the output of `pwd` from this repo. The fixture update DAG fails during parse with a clear error if `PROJECT_ROOT` is missing, because otherwise the dbt container would run with invalid mounts.

## API-Sports configuration

The pipeline now assumes API-Sports direct access only. Both metadata and fixture extraction use the centralized `API_HEADERS` value from `etl/src/config.py`:

```python
{"x-apisports-key": API_KEY}
```

`API_HOST` and RapidAPI headers are intentionally not part of the supported config. Keeping one provider avoids credentials that work for metadata extraction but fail for fixture extraction, or the reverse.

## dbt seed before dbt run

The dbt project has CSV seeds for streak scoring weights and multipliers. Models reference those seed relations, so fresh databases need `dbt seed` before `dbt run`.

The incremental DAG runs `dbt_seed` as a separate DockerOperator before `run_dbt`. Keeping seed and run as separate Airflow tasks makes failures easier to diagnose: missing or invalid seed data fails in `dbt_seed`, while model SQL failures stay isolated to `run_dbt`.

## Data detector schema

The `data_detector` schema is the audit layer for data flow. It is created by the raw bootstrap DDL and contains:

- `pipeline_runs`: one row per DAG/task execution that should be tracked.
- `data_movements`: one row per movement across boundaries such as API to S3, S3 to raw, raw to dbt, or replay to raw.
- `table_snapshots`: row counts and freshness watermarks for important tables after loads and dbt runs.
- `data_quality_events`: durable anomaly records, including DLQ activity, stale tables, failed loads, and unexpected row deltas.

This schema is intentionally separate from `raw`, `dim`, and dbt marts so operational telemetry can evolve without changing analytics table contracts.

For table examples and operating rules, see `docs/data_flow_detector.md`.
