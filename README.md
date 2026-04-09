# Ballistics

A football analytics pipeline that extracts fixture data from an external API, lands raw JSON in S3, loads into Postgres, and transforms with dbt into streak analysis, head-to-head breakdowns, and team performance metrics.

Built as a portfolio project to demonstrate data engineering patterns — not a production system.

## Architecture

```
                         ┌──────────────┐
                         │ Football API │
                         │  (RapidAPI)  │
                         └──────┬───────┘
                                │
                      ┌─────────▼──────────┐
                      │  Apache Airflow    │
                      │  (Dockerized)      │
                      └──┬─────────────┬───┘
                         │             │
              ┌──────────▼──┐   ┌──────▼──────────┐
              │  S3 Landing │   │   Postgres       │
              │  Zone (raw) │──►│   raw schema     │
              └──────┬──────┘   └──────┬───────────┘
                     │                 │
              ┌──────▼──────┐   ┌──────▼───────┐
              │  S3 DLQ     │   │     dbt      │
              │  (failures) │   │  (Docker)    │
              └──────┬──────┘   └──────┬───────┘
                     │                 │
              ┌──────▼──────┐    ┌─────┼─────────────┐
              │ Replay DAG  │    │     │             │
              │ (re-extract)│    ┌────▼───┐ ┌─────▼────┐ ┌────▼────┐
              └─────────────┘    │  stg   │ │   int    │ │  mart   │
                                 │ views  │ │  tables  │ │ tables  │
                                 └────────┘ └──────────┘ └─────────┘
```

## Pipeline Layers

**Bootstrap (sensor-gated, daily schedule):** A custom `MetadataChangeSensor` compares `metadata.yaml`'s modification time against the last-processed timestamp (stored as an Airflow Variable). Most days: no change detected → all downstream tasks soft-fail and skip. When you edit `metadata.yaml` (e.g. add a new league), the sensor triggers the full chain: create schemas, extract metadata, full-load fixtures into S3 and Postgres. This means adding a league is a config change, not a code change.

The bootstrap DAG uses parallel branches — metadata schema and raw fixtures schema run concurrently before converging at the extract step. S3 keys are passed between extract and load tasks via XCom (lightweight URI strings, not full payloads).

**Incremental (daily):** Picks up played fixtures missing fulltime scores, fetches updates by fixture ID, writes to S3 (`incremental/{ds}/fixtures.json`), upserts into Postgres, corrects any rescheduled kickoff times, refreshes league standings, then triggers dbt.

**Replay (daily, independent):** Scans the S3 dead letter queue (`dlq/`) for failed extractions, re-fetches from the API, loads into Postgres, and cleans up successful entries. Remaining failures are logged — in production this would trigger a Slack or email alert.

**dbt transforms:** Staging views clean raw data. Intermediate tables enrich fixtures with league context and team dimensions, compute streaks (win runs, clean sheet runs, scoring runs) and relevance scores. Mart tables assemble API-ready payloads — team pages, fixture pages, head-to-head breakdowns, homepage streak rankings.

## Tech Stack

- **Orchestration:** Apache Airflow 2.9.3 (Docker)
- **Storage:** AWS S3 (raw landing zone), PostgreSQL (structured data)
- **Transformation:** dbt 1.9.4 (Docker, via Airflow DockerOperator)
- **Language:** Python 3.10
- **Infrastructure:** Docker Compose (local), AWS IAM for S3 access

## Project Structure

```
ballistics/
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       ├── bootstrap_dag.py          # One-time schema + data setup
│       ├── fixture_update.py         # Daily incremental pipeline
│       └── replay_dag.py             # DLQ replay for failed extractions
├── etl/
│   └── src/
│       ├── config.py                 # Environment config
│       ├── extract_metadata.py       # League/team/country extraction
│       ├── extract_fixtures.py       # Fixture extraction + S3 writes
│       ├── s3_landing.py             # S3 read/write/load + DLQ
│       ├── update_fixtures.py        # Incremental fixture updates
│       ├── update_fixtures_main.py   # Upsert logic for raw.raw_fixtures
│       ├── update_standings.py       # League standings refresh
│       └── fixture_correction/       # Kickoff time mismatch corrections
├── dbt/football_pipeline/
│   ├── models/
│   │   ├── staging/                  # Source declarations + staging views
│   │   ├── intermediate/             # Enriched fixtures, streaks, team stats
│   │   └── marts/                    # API-ready payloads (team, fixture, h2h)
│   ├── seeds/                        # Streak scoring weights
│   └── tests/                        # Singular data quality tests
├── docker-compose.yml
├── .env.example
└── data/metadata.yaml                # League config (countries + tiers)
```

## How to Run

### Prerequisites
- Docker & Docker Compose
- AWS account with S3 bucket + IAM credentials
- RapidAPI key for [API-Football](https://www.api-football.com/)

### Setup

```bash
# Clone and configure
cp .env.example .env
# Edit .env with your credentials

# Start the stack
docker compose up -d

# Airflow UI at http://localhost:8080
# Default login: admin / (your AIRFLOW__WEBSERVER__DEFAULT_USER_PASSWORD)
```

### First Run

1. Trigger `bootstrap_dag` in the Airflow UI — this creates schemas, loads metadata, and full-loads all fixtures.
2. Once bootstrap completes, `fixture_update_dag` runs daily on schedule.

### dbt

dbt runs automatically as the final task in the daily DAG via `DockerOperator`. To run manually:

```bash
docker compose run --rm dbt dbt run
docker compose run --rm dbt dbt test
```

## Data Quality

Tests are defined at three levels:

**Source tests** (`raw__sources.yml`): Not-null, unique, and accepted-value constraints on raw tables. Source freshness monitoring warns at 24h stale, errors at 48h.

**Staging model tests** (`staging/schema.yml`): Key uniqueness, not-null constraints, and referential integrity (e.g. league_seasons → leagues FK).

**Singular tests** (`tests/`): Custom SQL assertions — e.g. no fixture should be marked as finished with a kickoff time in the future.

## Design Decisions

This project is a working demo. Some choices were made for simplicity that wouldn't hold in production:

| Aspect | Demo (current) | Production equivalent |
|---|---|---|
| Airflow | Local Docker Compose | EC2/ECS with IAM role |
| Credentials | Access keys in `.env` | Role-based, no keys in code |
| S3 | Single bucket, prefix-partitioned | Separate buckets per environment |
| Versioning | Off | On for critical data |
| Encryption | SSE-S3 (default) | SSE-KMS for sensitive data |
| IAM | Admin user + access keys | Least-privilege roles per service |

The S3 landing zone uses date-partitioned prefixes (`full_load/{league_id}/{season}/{ds}/`) so raw data is replayable without re-hitting the API. Incremental loads write to new prefixes — never overwriting. Failed extractions land in a dead letter queue (`dlq/`) with enough context to retry automatically via the replay DAG.

## Known Gaps

- **Playoff fixture ingestion:** The incremental path only updates existing fixtures. Newly created fixtures (e.g. Belgian championship playoffs) that appear after bootstrap are skipped. Fix: add an insert-new-fixtures path to the incremental flow, or re-bootstrap the affected league-season.
- **`stg_raw__completed_fixtures`** and **`goals_streak/`** have been removed — they referenced deprecated sources.

## Leagues Tracked

Belgium (Jupiler Pro League), Denmark (Superliga), Egypt (Premier League), England (Premier League, Championship), France (Ligue 1), Germany (Bundesliga, 2. Bundesliga), Italy (Serie A, Serie B), Netherlands (Eredivisie), Norway (Eliteserien), Portugal (Primeira Liga), Russia (Premier League), Scotland (Premiership), Spain (La Liga, Segunda División), Sweden (Allsvenskan), Turkey (Süper Lig).
