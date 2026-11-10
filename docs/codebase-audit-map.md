# Ballistics codebase audit map

Started: 2026-10-20. Scope: this repository's Python, Airflow, dbt, API, tests, scripts, configuration, and documentation. The sibling `football_stats_project` may be inspected where an integration crosses repositories; changes there should be tracked separately.

## Daily method

Review one bounded unit per day (about 20–40 minutes). Read the relevant code and tests, run a focused check when practical, record concrete findings with file/line evidence, and make only small, safe fixes that can be verified that day. Keep larger changes as follow-up items. Preserve unrelated work. A unit is complete only when its review and verification are recorded below.

## Work map

| Section | Small unit | Review target |
|---|---|---|
| 1. Orientation | 1.1 Repository inventory | Identify modules, entry points, test and quality commands |
|  | 1.2 Runtime and configuration | Environment variables, defaults, secrets handling, Docker mounts |
|  | 1.3 Dependency and build setup | Requirements, pinned versions, Dockerfiles, reproducibility |
| 2. Ingestion | 2.1 Bootstrap metadata | Sensor, schema bootstrap, extraction, idempotency |
|  | 2.2 Fixture full load | API pagination, S3 writes, load validation, DLQ |
|  | 2.3 Incremental updates | Existing and newly created fixtures, upserts, retries |
|  | 2.4 Standings and xG | Deduplication, group rules, missing-data behavior |
|  | 2.5 Replay and corrections | Retry safety, cleanup order, correction edge cases |
| 3. Orchestration and observability | 3.1 Airflow DAG dependencies | Scheduling, task order, failure propagation |
|  | 3.2 Data detector | Event writes, freshness checks, alert lifecycle |
|  | 3.3 Daily flow registry | Dataset lineage, blocked/root-cause logic |
| 4. Transformations | 4.1 dbt sources and staging | Source contracts, freshness, null/key tests |
|  | 4.2 dbt intermediate and facts | Joins, row cardinality, fixture classification |
|  | 4.3 dbt marts | API payload shape, duplicate/empty result handling |
| 5. Engine and API | 5.1 Elo and model inputs | Leakage guards, fixture eligibility, reproducibility |
|  | 5.2 Model runs and consensus | Run versions, cache invalidation, failure behavior |
|  | 5.3 Forecast and archive | Write idempotency, settlement updates, archive checkpoints |
|  | 5.4 API endpoints | Input validation, errors, query bounds, response contracts |
|  | 5.5 Cross-repo account integration | Auth boundary, Saved, billing hooks in sibling API |
| 6. Test and release quality | 6.1 Test coverage map | Critical paths lacking meaningful tests |
|  | 6.2 Quality gates | Focused tests, dbt checks, lint/type/compile checks |
|  | 6.3 Documentation and runbooks | Commands and diagrams compared with current code |
|  | 6.4 Final issue triage | Severity, owner, fix order, retest evidence |

Priority can change when a serious finding appears. Each daily update should say what was checked, what was found or fixed, what passed, and the next unit.

## Progress log

### 2026-11-09 — Unit 6.2 reviewed: quality gates

- **Evidence:** `.github/workflows/ci.yml:174-220` runs `dbt compile` against an empty Postgres service, so model SQL is compiled but source, relationship, uniqueness, and singular data tests are not executed in CI. `dbt/football_pipeline/README.md:7` and root `README.md:138` document `dbt test` as a separate manual command. **Severity: medium.** Follow-up: provide a small seeded dbt integration database or a controlled staging gate for `dbt build/test`, especially the fixture cardinality and freshness checks found in units 4.1–4.3.
- **Evidence:** `scripts/quality_check.sh:7-23` uses unittest discovery and a fixed compile list, whereas CI uses pytest over `tests` and `engine/tests` (`.github/workflows/ci.yml:62-66`). The local script can omit pytest-only tests and newer DAGs. **Severity: low.** Follow-up: align the local quality command with the current CI test and syntax scope, then update the README.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-11-09 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_fact_fixtures_source.py tests/test_data_detector.py` passed (22 tests). No runtime code changed. Next unit: 6.3 documentation and runbooks.

### 2026-11-08 — Unit 6.1 reviewed: test coverage map

- **Evidence:** `.github/workflows/ci.yml:62-66` compiles DAG Python syntax and runs Python tests, but no test asserts the `engine_predict` task graph or sensor trigger rule. The high-severity quorum failure recorded in unit 5.2 (`airflow/dags/engine_predict_dag.py:135-144,178`) therefore passes current CI. **Severity: high.** Follow-up: add a DAG parse/dependency test that fails when a model failure prevents the quorum sensor from running.
- **Evidence:** `tests/test_elo_unprocessed_fixtures.py:6-12` only checks the SQL excludes played fixtures without scores; it does not cover late historical fixtures or corrected results found in unit 5.1. **Severity: medium.** Follow-up: add a database or stateful service test for chronological backfill and correction replay.
- **Evidence:** `.github/workflows/ci.yml:37-41,56-66` provisions isolated databases for flow and forecast integration suites, while the local run skips them without `*_TEST_DSN`. **Severity: info.** This is a local verification limit, not a CI coverage omission.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-11-08 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_ingestion_failure_alerts.py tests/test_elo_unprocessed_fixtures.py` passed (16 tests). No runtime code changed. Next unit: 6.2 quality gates.

### 2026-11-07 — Unit 5.5 reviewed: cross-repo account integration

- **Scope evidence:** `docs/product/implementation-status.md:58-60` records the account API and frontend source move into Ballistics, with the sibling `football_stats_project` retained for rollback. Current `frontend_dev/lib/server/account-proxy.ts:15-53` obtains a Clerk session token server-side, validates the configured upstream origin, forwards a fixed route path, and validates the response. `API/app.py:257-274,1580-1642` verifies bearer claims and scopes Saved operations to the token subject. **Severity: info.** The active integration is now local to Ballistics; sibling files were inspected for location only and not changed.
- **Evidence:** `API/account_data.py:71-73` calls `read_saved`, which returns only `read_saved_page(... )['items']` (`API/saved_snapshots.py:63-67`) using the 12-item default. Thus `/account/saved` and its write responses return at most 12 cards, although `frontend_dev/components/research/shared.tsx:115-118,164-171` parses them as a complete list. The dedicated Saved browser uses `/account/saved/page` and membership uses `/account/saved/ids`, reducing current impact, but any full-list consumer of the legacy route can silently omit older saves. **Severity: low.** Follow-up: document the legacy route as a preview or migrate callers to pagination/IDs and test over 12 saves.
- **Evidence:** `API/sql/billing.sql:1-37` defines billing mirror tables; no active billing route was found in `API/app.py` or the account proxy routes. **Severity: info.** Billing is schema/proposal scope in this review, not a live hook.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-11-07 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider API/tests/test_account_data.py API/tests/test_app.py` passed (16 tests). Next unit: 6.1 test coverage map.

### 2026-11-06 — Unit 5.4 reviewed: API endpoints

- **Evidence:** `engine/api/app.py:82-97` validates that a Discover date range is ordered but sets no maximum span or result limit. `engine/api/discover.py:45-86,170-195` assembles every matching fixture and list entry into one JSON response; the response cache may hold up to 256 distinct parameter combinations. A client can request a very wide range and produce large response and cache entries. **Severity: medium.** Follow-up: define a maximum range and response size or pagination for Discover, then add boundary tests. No runtime code changed.
- **Evidence:** `engine/api/app.py:30-56` bounds forecast pagination, checks positive entry IDs, and maps invalid input to 422; lines 17-27 return 503 for database failures without exposing connection details. **Severity: info.** The focused API tests cover validation and error mapping.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-11-06 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_forecast_api.py tests/test_match_context.py tests/test_discover_api.py` passed (38 passed, 7 database integration tests skipped). Next unit: 5.5 cross-repo account integration.

### 2026-11-05 — Unit 5.3 reviewed: forecast and archive

- **Evidence:** `engine/eval/forecast_history.py:257-285` settles only entries returned by `forecast.latest_forecasts` (view at lines 76-81). When a later eligible snapshot replaces an earlier one for the same fixture and event, the earlier entry never receives a settlement observation unless it had already been settled before replacement. **Severity: medium.** Follow-up: decide whether every eligible forecast entry needs an outcome for audit history; if so, settle all eligible entries and test a replacement before kickoff followed by a final score. No runtime code changed.
- **Evidence:** `engine/eval/forecast_archive.py:34-41,67-99` writes a bounded S3 batch, reads it back to verify a checksum, then commits the manifest and checkpoint together. **Severity: info.** This protects the checkpoint from advancing after a failed upload or verification.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-11-05 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_forecast_history.py engine/tests/test_forecast_archive.py tests/test_forecast_api.py` passed (8 passed, 11 database integration tests skipped). Next unit: 5.4 API endpoints.

### 2026-11-04 — Unit 5.2 reviewed: model runs and consensus

- **Evidence:** `airflow/dags/engine_predict_dag.py:125-144,178` makes all three model tasks direct upstream dependencies of `wait_for_quorum`, but the sensor has the default `all_success` trigger rule. If a model fails, Airflow will not run the sensor, so the code's `>=2` ready-model quorum, all-finished, and grace-window branches cannot keep consensus running after that failure. **Severity: high.** Follow-up: set an explicit trigger rule consistent with the intended failure policy and add a DAG dependency test; also ensure the gate can start while a slow model is still running if the grace behavior is required. No runtime change in this bounded review.
- **Evidence:** `engine/eval/model_cache.py:75-96` hashes inputs and implementation and records state only after successful computation; `engine/eval/picks_store.py:106-136,143-159` replaces picks and writes readiness markers by run date. **Severity: info.** Focused cache and consensus tests cover these paths.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-11-04 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider engine/tests/test_model_cache.py tests/test_consensus_slips.py` passed (18 passed, 2 skipped). Next unit: 5.3 forecast and archive.

### 2026-11-03 — Unit 5.1 reviewed: Elo and model inputs

- **Evidence:** `engine/elo/elo_service.py:97-127` selects any scored played fixture absent from Elo history, while lines 239-263 seed its teams from their latest recorded ratings. If an older fixture arrives late or its score is corrected after an Elo row exists, an incremental run cannot replay subsequent matches chronologically; the late fixture is applied to current ratings, and an existing fixture is skipped altogether. **Severity: medium.** Follow-up: detect backfills and result changes, then rebuild affected Elo histories from the earliest changed kickoff (or use the existing full `--reset` path) and add a chronological backfill test. No runtime code changed.
- **Evidence:** `engine/elo/elo_service.py:115-127` requires played, home-perspective rows with both goals and orders them by kickoff and fixture ID; `dbt/football_pipeline/models/facts/fact_fixtures.sql:93-98` retains fixtures when dimension joins are missing. **Severity: info.** The source and ordering guards were reviewed.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-11-03 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_elo_unprocessed_fixtures.py tests/test_elo_goals.py tests/test_engine_fixture_guards.py tests/test_fact_fixtures_source.py` passed in two focused runs (24 tests total). Next unit: 5.2 model runs and consensus.

### 2026-11-02 — Unit 4.3 reviewed: dbt marts

- **Evidence:** `dbt/football_pipeline/models/marts/fixture/mart_fixture_info.sql:26-29` inner-joins fixture-team rows to `mart_team_info` and requires that team's `next_fixture_id` match. If only one side has a current team-info row, the rollup at lines 32-43 still emits the fixture with a null home or away team payload; if neither side matches, it emits no fixture. `models/marts/schema.yml:260-274` checks fixture IDs for null but has no uniqueness, completeness, or expected-coverage test. **Severity: medium.** Follow-up: define expected coverage for upcoming fixture pages, preserve both sides or explicitly exclude incomplete fixtures, and test null team payloads and fixture grain before changing the join.
- **Evidence:** `models/marts/pages/mart_fixture_page_context.sql:1-4` and `mart_team_page_context.sql:1-4` are parameterized and disabled unless their dbt vars are supplied. **Severity: info.** These are on-demand bundles, so a default dbt run does not materialize them; verify any caller supplies the vars when using them.
- **Verification:** `dbt parse --no-partial-parse --project-dir dbt/football_pipeline --profiles-dir .dbt --target-path /private/tmp/ballistics-dbt-parse-2026-11-02 --log-path /private/tmp/ballistics-dbt-log-2026-11-02` passed with the previously noted generic-test deprecation warnings. No data-dependent dbt tests or runtime code changes. Next unit: 5.1 Elo and model inputs.

### 2026-11-01 — Unit 4.2 reviewed: dbt intermediate and facts

- **Evidence:** `dbt/football_pipeline/models/intermediate/int_fixtures.sql:41-48` inner-joins league context and both teams. A fixture with a missing dimension row disappears from this intermediate model and downstream `fact_team_fixtures` (`:38-42`); the `int_fixtures` fixture-id uniqueness test in `intermediate/schema.yml:34-36` cannot detect missing rows. **Severity: medium.** Follow-up: add a reconciliation test comparing staging fixture IDs with intermediate IDs, and decide whether these downstream analyses should retain incomplete fixtures. The separate engine `fact_fixtures.sql:93-98` uses left joins to retain them.
- **Evidence:** `dbt/football_pipeline/models/facts/fact_fixtures.sql:23-54,82-91` derives competition phase and stage from round labels and phase game counts. `dbt/football_pipeline/tests/assert_fixture_stage_classification.sql:4-28` covers known label cases; `facts/schema.yml:64-72` checks allowed stage values. **Severity: info.** The model has classification checks, though this session did not run data-dependent dbt tests.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-11-01 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_fact_fixtures_source.py tests/test_engine_excludes_knockout.py` passed (11 tests). No runtime code changed. Next unit: 4.3 dbt marts.

### 2026-10-31 — Unit 4.1 reviewed: dbt sources and staging

- **Evidence:** `dbt/football_pipeline/models/staging/stg_raw_fixtures.sql:32-34` selects only raw rows with `updated_at` strictly greater than the current staging maximum. A late-arriving fixture with the same timestamp as that maximum, or a correction whose timestamp is older, will not be staged. **Severity: medium.** Follow-up: use a safe overlap or key-based reconciliation and verify late-arrival and equal-timestamp cases before changing the incremental strategy. No runtime code changed.
- **Evidence:** `dbt/football_pipeline/models/staging/raw__sources.yml:6-9` configures source freshness at 24-hour warning and 48-hour error; `schema.yml:5-30,56-75` covers staging fixture keys and league-season relationships. **Severity: info.** These contracts are declared, though this review did not run data-dependent tests.
- **Verification:** `dbt parse --no-partial-parse --project-dir dbt/football_pipeline --profiles-dir .dbt --target-path /private/tmp/ballistics-dbt-parse-2026-10-31 --log-path /private/tmp/ballistics-dbt-log-2026-10-31` passed with dbt 1.11.5. It emitted generic-test argument deprecation warnings (10 occurrences, including `schema.yml:65-67`), so a syntax migration is a low-severity follow-up. Initial parse attempt failed only because the default log path was read-only; the retry used temporary paths. Next unit: 4.2 dbt intermediate and facts.

### 2026-10-30 — Unit 3.3 reviewed: daily flow registry

- **Evidence:** `etl/src/data_detector/daily_flow.py:100-108` treats a movement as `ok` when any matching record for the logical day is successful, even if a later movement failed. `MOVEMENT_SQL` at lines 57-65 orders records but does not select a latest run or exclude earlier attempts. A successful earlier run can therefore mask a failed later producer run. **Severity: medium.** Follow-up: define whether the day should reflect the latest scheduled run or aggregate successful output, then align the query and add a mixed success/failure test. No runtime code changed.
- **Evidence:** `etl/src/data_detector/datasets.py:106-149` validates known dependencies and cycles; `etl/src/data_detector/daily_flow.py:344-377` evaluates upstream first and pushes blame to the furthest upstream root. `tests/test_daily_flow.py:48-80` checks the shipped registry, lineage, and root/blocked classification. **Severity: info.** The registry and blame paths have focused coverage.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-10-30 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_daily_flow.py` passed (16 passed, 11 integration tests skipped). Next unit: 4.1 dbt sources and staging.

### 2026-10-29 — Unit 3.2 reviewed: data detector

- **Evidence:** `etl/src/data_detector/monitoring.py:105,112-122` opens `unprocessed_rows` events for a specific league-season key but resolves them with only check name and target table. `etl/src/data_detector/quality_events.py:117-127,138-146` applies no league-season filter to that update. A clean load for one league can therefore resolve another league's still-open row-quality event. **Severity: medium.** Follow-up: persist a stable league-season identifier in a filterable column (or scoped key) and test that clean loads resolve only their own event. No runtime change in this bounded review.
- **Evidence:** `etl/src/data_detector/quality_events.py:220-248,296-322` inserts a new open freshness event each time a stale check runs, then resolves all matching open events when fresh. This preserves history but can create multiple simultaneous open rows for one condition. **Severity: low.** Follow-up: decide whether one active event per check/table is desired, then upsert or deduplicate if so.
- **Evidence:** `etl/src/data_detector/failure_alerts.py:11-25` records and sends the immediate task-failure alert; `etl/src/data_detector/notify.py:61-76` attempts each configured channel independently. **Severity: info.** The committed email path was reviewed; live SMTP delivery was not exercised.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-10-29 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_data_detector.py tests/test_dlq_fixtures_monitoring.py tests/test_ingestion_failure_alerts.py` passed (56 passed, 6 skipped). Next unit: 3.3 daily flow registry.

### 2026-10-28 — Fix for issue 1: silent ingestion failures (local code)

- Fixture bootstrap now uses strict API fetch and records a failed league-season in DLQ. An empty season is a failure only while the season is in progress by its `dim_league_seasons` dates; outside them it is logged and skipped (staging had three such seasons). Seasons that extracted still load; `load_fixtures` then fails the run (no task retry when only extraction failed), so the metadata revision is not marked complete and the failure callback fires.
- Incremental fixture selection now propagates database errors. ID-batch fetches use strict API failure handling; any failed batch or an empty result for requested IDs fails extraction rather than reporting no work.
- Standings fetch uses strict API failure handling. A league whose fetch fails is logged and skipped so the other leagues still refresh; the task fails at the end naming the failed leagues, instead of an empty standings table.
- Both ingestion DAGs now attempt an immediate alert through every configured channel (Slack webhook, SMTP email to `ALERT_EMAIL_TO`) when a task reaches failed state after retries. The callback also records the detector failure. A failed channel is logged and does not stop the others. The daily flow report remains in place.
- **Verification:** focused ingestion tests passed locally (32 passed, 3 database integration tests skipped). No live API, SMTP delivery, container DAG parse, or deployed pipeline run was performed. Deployment and live alert verification remain open.

### 2026-10-20 — Unit 1.1 complete: repository inventory

- **Evidence:** repository directories include `airflow/dags`, `etl/src`, `dbt/football_pipeline`, `engine/eval`, `engine/elo`, `engine/api`, `tests`, `scripts`, and `docs`. The README points to `scripts/quality_check.sh` for local unit/compile checks and `docker compose run --rm dbt dbt test` for dbt tests. There are focused Python tests under both `tests/` and `engine/tests/`.
- **Result:** entry-point and test locations mapped. No code defect asserted by this inventory alone; implementation review begins with unit 1.2.
- **Verification:** file listing and README search on 2026-10-20. No tests run for this documentation-only unit.

## Finding record

### 2026-10-21 — Unit 1.2 reviewed: runtime and configuration

- **Evidence:** `etl/src/config.py:9` calls `load_dotenv(override=True)`, so values from `.env` replace variables already supplied by the process. This can select unexpected credentials or endpoints when the same module runs in a container or test process. **Severity: medium.** Follow-up: decide precedence, then add a focused configuration test before changing behavior.
- **Evidence:** `etl/src/config.py:41-49` derives data directories from `os.getcwd()` and creates them at import time. Running a command from another directory can create files in the wrong location. **Severity: low.** Follow-up: use an explicit project/data root and defer directory creation to the command that needs it.
- **Evidence:** `docker-compose.yml:3` uses `postgres:latest`; `:61` and `:84` mount the Docker socket into Airflow containers. The version choice can change between rebuilds, and the mounts give those containers control of the host Docker daemon. **Severity: medium.** Follow-up: pin the database image and review whether DockerOperator requires both socket mounts.
- **Verification:** read `.env.example`, Compose, config, and DockerOperator mounts; AST syntax parse passed for `etl/src/config.py` and `airflow/dags/fixture_update.py`. No runtime behavior was changed. Next unit: 1.3 dependency and build setup.


### 2026-10-22 — Unit 1.3 reviewed: dependency and build setup

- **Evidence:** `airflow/requirements.txt` (0/2 pinned), `etl/requirements.txt` (0/8 pinned), and `engine/requirements.txt` (0/10 pinned) leave all listed direct packages open to version changes at rebuild. By contrast, `engine/api/requirements.txt` pins 3/3 and `dbt/requirements.txt` pins 1/1. `airflow/Dockerfile:1` pins Airflow/Python, while `engine/Dockerfile:9` and `dbt/Dockerfile:1` use Python minor tags. **Severity: medium.** Follow-up: lock compatible dependency versions per image, including the Docker provider, then verify image builds and focused tests before switching deployment.
- **Evidence:** `engine/Dockerfile:20-22` copies the entire `data/` tree into the engine image as fallback. This can make builds large and package local data unintentionally. **Severity: low.** Follow-up: check the image's actual runtime file needs and narrow the copy or build ignore rules.
- **Scope note:** `deploy/compose.staging.yml:66` pins Postgres major version 17. The `postgres:latest` finding from unit 1.2 applies to the local Compose file, not this staging file.
- **Verification:** reviewed five requirement files and four Dockerfiles; a read-only script counted exact pins. No image build or dependency install was run, and no runtime code changed. Next unit: 2.1 bootstrap metadata.

### 2026-10-23 — Unit 2.1 reviewed: bootstrap metadata

- **Evidence:** `etl/src/extract_metadata.py:467-470` logs an empty YAML document and returns successfully. `airflow/dags/bootstrap_dag.py:281-294,498-501` can then mark that file modification time processed after the downstream no-op fixture path. **Severity: medium.** Follow-up: fail the extraction task on empty/invalid metadata, with a focused test, so the changed file remains retryable.
- **Evidence:** `etl/src/extract_metadata.py:119-125,501-503` records unresolved league names but only logs them after committing other rows; the DAG can still mark the metadata revision processed. **Severity: medium.** Follow-up: define whether unresolved configured leagues should fail the bootstrap or enter a durable retry queue, then test that behavior.
- **Evidence:** `airflow/dags/bootstrap_dag.py:58-72` compares file modification times, not contents. A deployment that preserves timestamps can leave changed metadata unseen. **Severity: low.** Follow-up: consider a content hash or explicit bootstrap trigger if deployment copies preserve mtime.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-10-23 python -m pytest -q tests/test_extract_metadata_tiers.py` passed (3 tests). The first run without a temporary data directory failed at collection because logger import could not write under `data/logs` in this read-only scheduled workspace. No code was changed. Next unit: 2.2 fixture full load.

### 2026-10-24 — Unit 2.2 reviewed: fixture full load

- **Evidence:** `airflow/dags/bootstrap_dag.py:203-213` calls `fetch_fixtures` without `raise_on_failure=True`. `etl/src/extract_fixtures.py:172-182,188-193` returns `[]` after exhausted transport retries by default. The DAG treats that as an empty season, writes no DLQ entry, and may still run `mark_metadata_processed` (`bootstrap_dag.py:281-294,498-501`). A transient API outage can therefore leave pending league-seasons without an automatic bootstrap retry. **Severity: high.** Follow-up: enable failure propagation for bootstrap fetches, distinguish valid empty seasons from failures, and add a DAG-level test before changing the retry/marking behavior.
- **Evidence:** `etl/src/extract_fixtures.py:125-142` checks remaining rate-limit headers before consuming a successful response. If a valid response uses the last request in the allowance, it sleeps and retries rather than returning that payload. **Severity: medium.** Follow-up: process the current successful response first; apply the remaining quota to subsequent requests, with a mocked last-request test.
- **Evidence:** `etl/src/s3_landing.py:70-77,91-98` writes fixed object keys within a day. A retry on the same day replaces the previous object at that key; the landing zone is not immutable at retry granularity. **Severity: low.** Follow-up: either document date-level replacement or add unique run/object version keys if exact replay is required.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-10-24 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_dlq_remaining_paths.py tests/test_new_fixtures.py` passed (13 passed, 3 integration tests skipped). No code was changed. Next unit: 2.3 incremental updates.

### 2026-10-26 — Unit 2.4 reviewed: standings and xG

- **Evidence:** `etl/src/update_standings.py:53-59` uses `_rate_limited_get` without strict failure mode. On exhausted transport retries the helper returns `[]`; `etl/src/standings_dlq.py:209-218` then records zero loaded rows and no failure. **Severity: high.** Follow-up: use strict fetch for standings and add a test that an API outage fails or quarantines the league rather than reporting an empty table.
- **Evidence:** `etl/src/extract_xg.py:282-289` stores an xG row and marks the attempt `available` if either side has a value. The `only_missing` selector at lines 246-253 excludes every fixture with an xG row, so a one-sided response is never retried automatically. **Severity: medium.** Follow-up: define whether partial xG is acceptable; if both teams are required, retain a partial attempt state and retry until complete.
- **Evidence:** `etl/src/update_standings.py:215-231,258-317` deduplicates by table group and team before upsert and prunes stale keys for refreshed seasons. Existing tests cover split-season groups and missing team IDs. **Severity: info.** No change needed in this unit.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-10-26 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_update_standings.py tests/test_extract_xg.py` passed (16 tests). No runtime code changed. Next unit: 2.5 replay and corrections.

### 2026-10-27 — Unit 2.5 reviewed: replay and corrections

- **Evidence:** `etl/src/replay_dlq.py:97-110` records the raw fixture table snapshot before `load_fixtures_from_s3` runs. The snapshot can therefore report pre-replay row counts even when the load succeeds. **Severity: medium.** Follow-up: record the snapshot after the load and commit, and test its ordering.
- **Evidence:** `etl/src/replay_dlq.py:51-55` uses non-strict fetch, so exhausted API retries become an empty response and count as a failed replay attempt through `airflow/dags/replay_dag.py:110-120`. The DLQ entry remains, but the recorded reason loses the transport error and can consume the 24-attempt budget during an outage. **Severity: medium.** Follow-up: use strict fetch and preserve the underlying error in retry state.
- **Evidence:** replay cleanup is registered after load (`etl/src/replay_dlq.py:159-160`) and executed only after database commit (`airflow/dags/replay_dag.py:106-107`); rollback discards it (`:113-115`). The fixture reconciliation path also requires a strict direct-ID lookup before marking an apparent orphan (`etl/src/fixture_correction/fixture_correction.py:340-358`). **Severity: info.** These safeguards were reviewed; no change needed in this unit.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-10-27 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_dlq.py tests/test_fixture_reconciliation.py` passed (30 passed, 3 integration tests skipped). No runtime code changed. Next unit: 3.1 Airflow DAG dependencies.

### 2026-10-28 — Unit 3.1 reviewed: Airflow DAG dependencies

- **Evidence:** `airflow/dags/fixture_update.py:350-358,389` gates dbt only on `extract_updates` and `load_updates`. The new-fixture, xG, standings, correction, and quality tasks are upstream of that gate but use `ALL_DONE` for parts of the chain (`:263,291,316,323`), so dbt can run after one of these side tasks fails. `airflow/dags/engine_predict_dag.py:108-117` waits for `run_dbt` success and can then consume stale side-input data. **Severity: medium.** Follow-up: decide which side-task failures should block dbt/engine and which should be explicitly marked degraded; add DAG dependency tests for those policies.
- **Evidence:** `engine_predict_dag.py:108-117,172-178` waits on the same-day `fixture_update_dag.run_dbt` before models and forecast history, addressing the prior previous-night facts race. **Severity: info.** This dependency is present in code.
- **Verification:** AST parsing succeeded for all 8 DAG Python files. A local Airflow 2.10.5 DagBag attempt could not parse DAGs because its installed SQLAlchemy raises `MappedAnnotationError` before project DAG loading; this is a local environment compatibility issue, not evidence that the deployed DAGs fail. No runtime code changed. Next unit: 3.2 data detector.

`Date | Unit | File/line or command | Observed behavior | Severity | Fix/follow-up | Verification`

### 2026-10-25 — Unit 2.3 reviewed: incremental fixture updates

- **Evidence:** `etl/src/update_fixtures.py:75-78` returns `[]` on a selector database error. `airflow/dags/fixture_update.py:128-136` treats `[]` as no work and succeeds. **Severity: high.** Follow-up: propagate selector failures and test the DAG's failure path so a broken staging query cannot look like a quiet day.
- **Evidence:** `etl/src/update_fixtures.py:248-253` uses the non-strict fixture fetch. As reviewed in unit 2.2, exhausted transport retries return `[]`, so failed batches are counted as successful empty responses. The all-batches-failed guard at lines 273-278 is bypassed in this case. **Severity: high.** Follow-up: use `raise_on_failure=True` and test mixed and all-failed batches.
- **Evidence:** `etl/src/new_fixtures.py:97-100` uses strict fetch and the daily DAG wires the new-fixture discovery task at `airflow/dags/fixture_update.py:383-389`. This closes the older known gap for current league seasons; per-league failures remain in the returned summary when at least one league succeeds. **Severity: info.** Keep partial failure visibility in the daily report and verify coverage separately.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-10-25 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_update_fixtures.py tests/test_new_fixtures.py` passed (10 passed, 3 integration tests skipped). No runtime code changed. Next unit: 2.4 standings and xG.
