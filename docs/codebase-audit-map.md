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


`Date | Unit | File/line or command | Observed behavior | Severity | Fix/follow-up | Verification`
