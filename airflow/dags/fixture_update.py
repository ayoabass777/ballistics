"""
Fixture Update DAG — daily incremental updates for played fixtures.

Fetches fixtures that have been played since last run (null fulltime goals,
past kickoff), writes raw updates to S3 landing zone, then applies to Postgres.
Also corrects rescheduled kickoff times and updates league standings.

Task graph:

  extract_updates ──► load_updates ──► fixture_corrections ──► update_standings ──► dbt_run
"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


default_args = {
    "owner": "Ayomide Abass",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    "/Users/ayomideabass/Documents/Projects/ballistics",
)


@dag(
    dag_id="fixture_update_dag",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["fixture_update"],
    doc_md=__doc__,
)
def fixture_update_dag():

    # ------------------------------------------------------------------
    # Extract: fetch updates from API → write to S3
    # ------------------------------------------------------------------

    @task
    def extract_updates(**context) -> str:
        """
        Fetch fixture updates (by ID) from the API.
        Write raw updates to S3 incremental landing zone.
        Returns S3 key via XCom.
        """
        from etl.src.update_fixtures import to_update_fixture_ids, update_by_ids
        from etl.src.s3_landing import write_incremental_to_s3
        from etl.src.logger import get_logger
        from etl.src.config import UPDATE_FIXTURES_LOG

        logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)

        ids = to_update_fixture_ids()
        if not ids:
            logger.info("No fixtures need updating.")
            return ""

        updates = update_by_ids(ids)
        if not updates:
            logger.info("No updates returned from API.")
            return ""

        ds = context["ds"]
        s3_key = write_incremental_to_s3(updates, ds)
        logger.info("Extracted %d fixture updates → s3://%s", len(updates), s3_key)
        return s3_key

    # ------------------------------------------------------------------
    # Load: read from S3 → upsert to Postgres
    # ------------------------------------------------------------------

    @task
    def load_updates(s3_key: str):
        """
        Read fixture updates from S3 and apply to raw.raw_fixtures
        via batched upserts.
        """
        from etl.src.update_fixtures_main import apply_updates_to_db
        from etl.src.s3_landing import read_fixtures_from_s3
        from etl.src.logger import get_logger
        from etl.src.config import UPDATE_FIXTURES_LOG

        logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)

        if not s3_key:
            logger.info("No S3 key provided. Skipping load.")
            return

        updates = read_fixtures_from_s3(s3_key)
        if not updates:
            logger.info("Empty updates file. Skipping.")
            return

        count = apply_updates_to_db(updates)
        logger.info("Applied %d fixture updates from %s", count, s3_key)

    # ------------------------------------------------------------------
    # Corrections: fix rescheduled kickoff times
    # ------------------------------------------------------------------

    @task
    def fixture_corrections():
        """Correct missed and changed fixture kickoff times."""
        from etl.src.fixture_correction.main import run as run_correction
        from etl.src.logger import get_logger
        from etl.src.config import UPDATE_FIXTURES_LOG

        logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)
        count = run_correction(mode="both", count=11)
        logger.info("Fixture corrections applied: %d updates", count)

    # ------------------------------------------------------------------
    # Standings: refresh league standings from API
    # ------------------------------------------------------------------

    @task
    def update_standings_task():
        """Fetch and upsert current league standings."""
        from etl.src.update_standings import update_standings
        from etl.src.logger import get_logger
        from etl.src.config import UPDATE_FIXTURES_LOG

        logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)
        count = update_standings()
        logger.info("Standings updated: %d rows", count)

    # ------------------------------------------------------------------
    # dbt: run transformations
    # ------------------------------------------------------------------

    dbt_run = DockerOperator(
        task_id="run_dbt",
        image="ballistics-dbt:latest",
        api_version="auto",
        auto_remove=True,
        mount_tmp_dir=False,
        network_mode="ballistics_network",
        mounts=[
            Mount(
                source=f"{PROJECT_ROOT}/dbt/football_pipeline",
                target="/opt/dbt",
                type="bind",
                read_only=False,
            ),
            Mount(
                source=f"{PROJECT_ROOT}/.dbt/profiles.yml",
                target="/root/.dbt/profiles.yml",
                type="bind",
                read_only=True,
            ),
            Mount(
                source=f"{PROJECT_ROOT}/dbt/football_pipeline/logs",
                target="/opt/dbt/logs",
                type="bind",
                read_only=False,
            ),
        ],
        environment={"DBT_PROFILES_DIR": "/root/.dbt"},
        working_dir="/opt/dbt",
        command=["dbt", "run", "--profiles-dir", "/root/.dbt"],
    )

    # ------------------------------------------------------------------
    # Wiring
    # ------------------------------------------------------------------

    s3_key = extract_updates()
    loaded = load_updates(s3_key)
    corrections = fixture_corrections()
    standings = update_standings_task()

    loaded >> corrections >> standings >> dbt_run


fixture_update_dag()
