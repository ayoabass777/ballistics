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

`Date | Unit | File/line or command | Observed behavior | Severity | Fix/follow-up | Verification`

### 2026-10-25 — Unit 2.3 reviewed: incremental fixture updates

- **Evidence:** `etl/src/update_fixtures.py:75-78` returns `[]` on a selector database error. `airflow/dags/fixture_update.py:128-136` treats `[]` as no work and succeeds. **Severity: high.** Follow-up: propagate selector failures and test the DAG's failure path so a broken staging query cannot look like a quiet day.
- **Evidence:** `etl/src/update_fixtures.py:248-253` uses the non-strict fixture fetch. As reviewed in unit 2.2, exhausted transport retries return `[]`, so failed batches are counted as successful empty responses. The all-batches-failed guard at lines 273-278 is bypassed in this case. **Severity: high.** Follow-up: use `raise_on_failure=True` and test mixed and all-failed batches.
- **Evidence:** `etl/src/new_fixtures.py:97-100` uses strict fetch and the daily DAG wires the new-fixture discovery task at `airflow/dags/fixture_update.py:383-389`. This closes the older known gap for current league seasons; per-league failures remain in the returned summary when at least one league succeeds. **Severity: info.** Keep partial failure visibility in the daily report and verify coverage separately.
- **Verification:** `BASE_DATA_DIR=/private/tmp/ballistics-code-audit-2026-10-25 PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider tests/test_update_fixtures.py tests/test_new_fixtures.py` passed (10 passed, 3 integration tests skipped). No runtime code changed. Next unit: 2.4 standings and xG.
