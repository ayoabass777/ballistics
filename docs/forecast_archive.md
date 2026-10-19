# Full and incremental forecast archive

Implemented 2026-10-19. Copies append-only history to private S3; never deletes or
updates source snapshots, entries or settlements. The existing Forecast record
API continues to read PostgreSQL. This is archival replication, not a hot-storage
retention migration or a replacement for backups.

## Run and resume

Set `FORECAST_ARCHIVE_BUCKET` in the gitignored project environment. Current private
bucket: `ballistics-archive-438542302919-ca-central-1`.

```sh
python -m engine.eval.forecast_archive --mode full --batch-size 500
python -m engine.eval.forecast_archive --mode incremental --batch-size 500
```

The first run starts each table at zero. Both modes resume verified checkpoints;
`full` is an operator label, not permission to reset existing checkpoints. A new
bucket has independent checkpoints. Sources: `forecast.snapshots`, `entries`,
`settlements`; all rows are copied, including ineligible entries, misses and every
correction observation. No historical records are fabricated or reclassified.

A short SHARE lock across source tables waits for in-flight inserts before taking
stable upper IDs. Thus a sequence ID reserved by an uncommitted insert cannot be
skipped by the watermark. Locks are released before network uploads. A session
advisory lock serializes archive jobs. Appends after the upper bounds are handled
on the next run. Source append-only guards remain intact.

Snapshots upload one per batch; smaller entries/settlements use bounded ID-ordered
batches (default 100, scheduled 500, maximum 1000). Objects contain complete rows,
including stable IDs, original capture/observation timestamps and provenance.
Canonical JSON is deterministically gzip-compressed. S3 keys use
`forecast-history/v1/<table>/<export-year>/<month>/<day>/<first>-<last>-<sha256>.json.gz`.
Export-date partitioning does not replace original capture dates.

Each payload is encrypted, uploaded and downloaded to verify SHA-256 and decoded
row count. A manifest records schema version, table, IDs, count, checksum, object
key and run upper bound; its upload is also read back. Only then does one database
transaction record the batch and advance its checkpoint. Failure leaves the old
checkpoint; retries may leave unreferenced objects if interrupted before commit,
but cannot skip data. Bucket versioning preserves overwritten retry versions.

`forecast_archive.checkpoints` is keyed by bucket/table.
`forecast_archive.batches` records verified durable object and manifest locations.
For restoration/audit, read the batch catalog, verify object SHA-256, decompress,
and use the original IDs. There is no API fallback to these objects yet because
all source rows remain hot and existing queries are unchanged.

## Daily Airflow execution

`forecast_history_archive` runs daily at **05:45 UTC** in the local Airflow default
UTC timezone, with two retries, five-minute retry delay, one active run and no
catch-up. `forecast_archive_bucket` Airflow Variable selects the bucket; worker
uses its existing AWS and DB credentials. Initial creation is paused so bucket
access and initial load can be verified before unpausing.

The task uses the existing engine volume mount and installed boto3/psycopg2.
No scheduler/container rebuild is needed for source-only changes. Production
requires separately deployed code, restricted runtime IAM and worker configuration.

## Verification and limits

Opt-in disposable PostgreSQL integration test:
`TEST_FORECAST_ARCHIVE=1 python -m pytest engine/tests/test_forecast_archive.py -q`.
It exercises initial copying, failed verification without checkpoint advancement,
retry, no-op incremental run, appended correction and intact originals. Its S3
transport is simulated; live run evidence is recorded in implementation status.

No expiry, lifecycle deletion, partition removal, performance-cohort changes or
forecast-history API migration is enabled by this job. Those are separate changes
requiring final retention and restore-access decisions.

## Live verification — 2026-10-19

Full load verified 18 snapshots, 57,718 entries and 43,197 settlements. Source and
archive counts matched. Airflow worker incremental task succeeded with zero new
rows. Local DAG unpaused for 05:45 UTC daily execution. Original rows retained.
