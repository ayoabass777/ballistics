# Shared snapshots and retention for Saved and Forecast record

2026-10-19 — Private archive bucket `ballistics-archive-438542302919-ca-central-1` provisioned in ca-central-1 at owner request. Public access blocking, bucket-owner-enforced ownership, AES256 default encryption, versioning and HTTPS-only policy verified. Account gitignored environment now sets SAVED_ARCHIVE_BUCKET; API restarted. Actual S3 synthetic write/read through the account archive resolver passed checksum/identity verification; test object version removed. No user snapshots archived, no lifecycle expiry set, no retention activation. Existing AWS credential chain used; dedicated deployment IAM roles and worker configuration remain deployment work. Reprovision via original repo `python -m scripts.configure_saved_archive_bucket`. Local SDK installed at boto3/botocore 1.39.8 to preserve existing aiobotocore compatibility.

Date: 2026-10-19. Status: **Saved shared storage implemented locally; forecast payload migration and retention activation pending**.

The owner approved documenting shared immutable snapshots, unique fixture/event forecast identifiers, date partitioning, and the proposed Postgres/S3 retention approach. The initial request authorized documentation only. A subsequent explicit “implement it” request authorized the local implementation recorded below. Numerical retention periods below remain proposed defaults, not an active policy.

## Direction

Store each immutable forecast/evidence version once. User saves reference that version rather than copying the full forecast/card JSON for every user. Use PostgreSQL for current, queryable data and S3 for older detailed evidence. Retain stable identifiers and an archive lookup path so a saved reference remains resolvable after its payload leaves Postgres. Add date partitioning to large historical payload tables as part of the eventual migration; keep small account/reference tables indexed normally unless measurements justify partitioning them.

Separate the UI display window, the hot database retention window and the archive retention period. Pagination and bounded queries address expensive requests; retention addresses accumulated storage. Neither alone solves every form of database load.

## Existing implementation to build on

Inspected 2026-10-19:

- `engine/eval/forecast_history.py`: `forecast.snapshots.id` identifies a captured engine payload; `content_hash` deduplicates identical captures. It records `run_ds`, database-generated `recorded_at`, payload and provenance.
- `forecast.entries.id` already identifies a particular fixture/event forecast within a snapshot. `(snapshot_id, fixture_id, event_key)` is unique. Entries retain probability, kickoff, policy, eligibility and detail.
- `forecast.settlements.id` identifies an append-only outcome observation/correction linked by `entry_id`; it records grader and evidence.
- `engine/api/forecast_records.py` already exposes entry IDs and reads historical observations as of a requested time. Current selection includes the latest eligible pre-kickoff forecast policy.
- The original API's local `app.saved_fixtures` implementation stores a full JSON card per user/fixture. Shared snapshot references are a future migration from that shape, not existing functionality.
- Forecast tables currently reject UPDATE, DELETE and TRUNCATE through append-only triggers. No routine cleanup may bypass those protections. A retention migration must explicitly preserve the audit contract and introduce controlled archive maintenance.

Reconcile with [forecast history documentation](../forecast_history.md) before implementation. Do not introduce a second competing forecast ID system where the existing IDs meet the requirement.

## Identity and reference contract

| Identifier | Meaning | Version behaviour |
| --- | --- | --- |
| `fixture_id` | Stable internal fixture identity; provider IDs remain separately namespaced | Same fixture across rescheduling and forecast revisions |
| `event_key` | Canonical event/market definition, including subject, period and threshold where relevant | Human-readable labels are not identity; changed semantics require a new definition/version |
| `forecast_id` | Proposed API terminology for the existing `forecast.entries.id` where compatible | Identifies one exact forecast version, not every prediction ever made for a fixture/event |
| `snapshot_id` | Existing engine capture identity | One capture may contain many fixtures and events; identical capture retries reuse its identity |
| `saved_snapshot_id` | Proposed shared immutable presentation/evidence snapshot identity | Pins the exact card/evidence the user saw; links to fixture and zero or more forecast entry IDs |
| `settlement_id` | Existing outcome observation identity | Corrections append new observations; never rewrite the original forecast |

A logical fixture/event pair is not sufficient to identify a forecast version: the same match and market can be forecast again after a new run or model change. Preserve the capture/run identity, recorded-at timestamp, model/calibration provenance, policy and schema version alongside the immutable forecast ID. Use database-generated capture time for prospective eligibility; do not replace it with Airflow's logical date.

A saved card may represent Strength or a historical Pattern rather than a forecast. Such cards still need an immutable evidence/presentation snapshot; do not manufacture forecast IDs for observed statistics. A whole-match save can reference several event forecasts through an explicit mapping, not an ambiguous single event ID.

Deduplicate only truly identical, canonicalised snapshots under the same schema/provenance contract. Same fixture, same displayed percentage or same event label alone is insufficient. Preserve enough presentation data to reproduce historical labels after future UI/model changes.

## Proposed logical storage shape

Names and physical keys below are design candidates, not migration-ready DDL.

- **Shared snapshot identity/locator:** stable ID, fixture ID, capture timestamp, schema version, content hash, provenance reference and current storage location. Lightweight identity remains in Postgres after archival.
- **Shared snapshot payload:** immutable card/evidence details, owned once per version, in date-partitioned hot storage or a verified archive object.
- **Snapshot-to-forecast mapping:** links a saved snapshot to its exact forecast entry IDs where applicable.
- **User saved reference:** `user_id`, `fixture_id`, `saved_snapshot_id`, `saved_at`, and optional archive-state metadata. Preserve current one-save-per-user/fixture semantics: duplicate save retains the first snapshot until removed. Unsave deletes only that user's reference, never shared forecast evidence.
- **Archive manifest/locator:** object key, format/schema version, partition bounds, row counts, integrity checksum, export watermark, verification status and archival time. Avoid storing expiring signed URLs as permanent references.

Separating a small permanent identity/locator from large historical payloads allows Saved and settlement references to remain valid when payload partitions are removed. A foreign key directly to an expiring payload row would prevent removal or break the intended reference contract. Resolve this before selecting the physical schema.

Account references remain private and require authorization. Shared football evidence contains no user identities; do not include user save lists in a public asset bucket. Account deletion removes personal references under its own lifecycle, independently of shared football forecast history.

## Date partitioning

Agreed direction: date-based partitioning for high-volume historical snapshot/forecast payloads. Proposed initial granularity: monthly UTC ranges by immutable capture/recorded time. Settlement observations can use their own observed-at ranges. Retention eligibility is separate from partition placement.

Do not partition by a mutable kickoff time: postponed fixtures must not move historical capture identity. `run_ds` is processing provenance, not necessarily the timestamp at which a forecast became eligible. Keep pending/postponed fixtures and records under correction holds accessible even if their capture partition is old.

PostgreSQL uniqueness constraints on a partitioned table must include its partition key. Therefore a globally unique ID alone cannot simply be declared the primary key of a date-partitioned parent. Candidate approach: a nonpartitioned identity registry for global IDs and references, with composite `(capture_date, id)` keys on partitioned payloads. An alternative is carrying the partition date in every reference. Choose and validate one before migration. [PostgreSQL partitioning limitations](https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE-LIMITATIONS)

Queries need explicit date predicates to support partition pruning. Use cursor pagination with stable date/ID ordering, bounded page sizes and account/date indexes. Pre-create future partitions and monitor missing-partition errors. Do not partition small user preference/follow tables just for consistency.

## Proposed windows

| Data | Candidate hot/display policy | Archive policy |
| --- | --- | --- |
| Upcoming or unresolved saved fixtures | Keep active until terminal settlement or explicit unsave | Never expire solely because kickoff or save date is old |
| Settled saved fixtures | Display in recent Saved for 30 days after settlement | Keep an accessible archive/reference rather than silently deleting |
| Detailed forecast/evidence payloads | Target 90 days in hot storage | Archive older eligible details; preserve IDs, provenance and outcomes |
| Performance summaries | Keep long-term in queryable form | Preserve period, cohort, policy/model version and reconciliation metadata |

The 30/90-day values are proposals. Confirm archive access UX, final retention duration, late-correction grace period and the clock used for each policy before activation. For forecast payloads, a capture-age threshold must also require terminal settlement and no active correction/legal/operational hold. A partition can leave hot storage only when all its required records are safely resolvable elsewhere; unresolved records need an explicit exception strategy or the partition remains hot.

Forecast performance must include all eligible forecasts under the published policy, including misses. User unsaves and hot-data expiration must not change the evaluation cohort. Retain reconstructible archive evidence in addition to summaries; summaries alone cannot reproduce a forecast audit. New correction observations must remain linked to archived forecast IDs, and affected summaries must be recalculated/versioned without rewriting original predictions.

## Archive lifecycle using existing Airflow and S3

1. Select a stable, bounded export set under a declared policy/version and watermark; exclude unresolved or held records.
2. Export immutable detailed records and related evidence/provenance to versioned, date-partitioned S3 keys. Avoid replacing an earlier archive silently.
3. Write a manifest with counts, checksums and schema/version information. Verify the uploaded objects and exercise retrieval before marking the export complete.
4. Publish durable ID-to-archive locators and ensure Saved/history APIs can resolve the archived records. Reconcile long-term performance summaries against the exported cohort.
5. Only after all verification/reference gates pass, detach/remove eligible hot payload partitions through a reviewed maintenance path. Do not cascade-delete user references or forecast identity registries.
6. Retry idempotently from the manifest state. On failure, retain hot data and alert. Test recovery at export, upload, locator publication and partition-maintenance boundaries.

Archiving is not a backup substitute. Define archive access controls, restoration tests and S3 lifecycle transitions separately. Colder storage may add retrieval latency; the archive UI must make that clear.

Partition detach/drop can be more efficient than row-by-row expiry, but locking and foreign-key constraints still require maintenance planning. [PostgreSQL partition maintenance](https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-MAINTENANCE)

## Kafka position

Kafka is deferred. The current proposal is a daily Airflow archive/maintenance flow with Postgres and S3. Reconsider Kafka if several independent consumers need reliable streams of forecast-issued, fixture-settled or user-save events, with explicit replay/idempotency requirements.

Kafka topic retention governs the event log, not records consumers have already persisted into Postgres. It would not replace database archival, reference integrity or S3 lifecycle rules. [Apache Kafka topic retention configuration](https://kafka.apache.org/42/configuration/topic-configs/)

## Migration and verification gates

Before implementation:

- Measure table/index sizes, per-user JSON duplication, growth, query latency and likely archive retrieval volume. Record baseline and expected benefit; keep the identity registry's own growth visible.
- Finalise event identity/version rules and the identity-versus-payload partition model; prove FKs and global uniqueness remain valid.
- Map existing Saved JSON to exact shared versions only when provenance is sufficient. Preserve unmatched legacy cards as immutable legacy snapshots with explicit provenance gaps; never attach them to today's forecast or invent historical IDs.
- Backfill and reconcile in batches; preserve original save timestamps and first-save semantics. Validate new reads before switching, retaining a rollback path and originals until verified.
- Test shared deduplication across users, authorization, independent unsave, revised forecasts, postponed fixtures, late corrections, archive retrieval, mixed hot/archive pagination, duplicate job retries and crash recovery.
- Verify forecast counts/results and historical as-of responses before and after archival. Do not infer eligibility from a later snapshot during migration.
- Extend bootstrap/migration documentation with dependencies and explicit maintenance permissions. Existing append-only safeguards remain until an approved migration provides an equivalent audit-safe design.

No schema, API, Airflow, S3 lifecycle, Kafka or Figma changes have been made as part of this documentation request.


## Implementation update — 2026-10-19

Saved now uses `app.saved_fixture_refs` → `app.saved_snapshots` → monthly
`app.saved_snapshot_payloads`. Registry identity and user references outlive hot
payloads. SHA-256 includes schema version, provenance category, run date and the
canonical card. New client cards and legacy cards are separate provenance classes;
neither is certified as an engine forecast. No guessed forecast-entry mapping was
introduced. Existing engine snapshot/entry/settlement identities remain unchanged.

The three local legacy saves (2,587 total card bytes) were migrated and reconciled
with identical card content, run dates and saved timestamps. Original rows remain
as a rollback checkpoint; current reads and writes use the shared tables. This
small baseline does not demonstrate a storage saving today: deduplication benefits
arrive when users save the same exact version, and legacy originals are retained.

Implemented archive tooling uses a private configured S3 bucket, content-addressed
objects, SHA-256 validation, read-back through the account resolver, a count/checksum
manifest, transactional locator publication and optional whole-partition retirement.
All rows must be explicitly settled, released from hold and old enough; new rows
start held. Dry-run is default. Retention age has no implicit value. No S3 bucket,
IAM policy, upload or production partition deletion was performed.

A paused-on-creation Airflow DAG invokes account maintenance once its repository
mount/dependencies/environment are configured. Current month plus three future
months are provisioned locally. The live API was restarted and the browser shows
all three original Saved cards. Thirty-eight API/PostgreSQL tests pass; archive
transport is simulated, PostgreSQL storage/DDL is real and transaction-rolled-back.

Remaining scope: forecast payload repartitioning/archival and summary reconciliation;
exact forecast provenance propagation into submitted cards; bounded Saved cursor
pagination and archive display UX; final retention/correction windows and automated
settlement/hold release; private S3 deployment and end-to-end archive verification.
The 30/90-day proposals are not active, and forecast append-only guards are unchanged.
See the original repository `API/sql/README.md` for bootstrap, migration, retry,
rollback and maintenance commands. Kafka remains deferred.


2026-10-19 — Owner authorized initial full forecast-history load followed by
incremental archival. Implemented `engine.eval.forecast_archive` and daily
`forecast_history_archive` Airflow DAG. See [full/incremental archive runbook](../forecast_archive.md).
This copies complete history to private S3 with verified manifests and per-table
checkpoints. It does not repartition or delete source forecast tables, activate
30/90-day expiry, or change existing Forecast record queries. Earlier notes saying
forecast export tooling is missing are superseded; hot-storage retirement and API
archive fallback remain separate pending work.
