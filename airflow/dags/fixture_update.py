"""
Fixture Update DAG — daily incremental updates for played fixtures.

Fetches fixtures that have been played since last run (null fulltime goals,
past kickoff), writes raw updates to S3 landing zone, then applies to Postgres.
Also corrects rescheduled kickoff times and updates league standings.

Task graph:

  extract_updates ──► load_updates ──► fixture_corrections ──► update_standings ──► dbt_seed ──► dbt_run
"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from etl.src.data_detector import (
    pipeline_run_failed,
    pipeline_run_started,
    pipeline_run_succeeded,
)


default_args = {
    "owner": "Ayomide Abass",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_execute_callback": pipeline_run_started,
    "on_success_callback": pipeline_run_succeeded,
    "on_failure_callback": pipeline_run_failed,
}

PROJECT_ROOT = os.getenv("PROJECT_ROOT")
if not PROJECT_ROOT:
    raise RuntimeError(
        "PROJECT_ROOT must be set to the host path of this repository so Airflow's "
        "DockerOperator can bind-mount dbt files. Set PROJECT_ROOT in .env to "
        "the output of `pwd` from the repository root."
    )

DBT_MOUNTS = [
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
]

DBT_OPERATOR_KWARGS = {
    "image": "ballistics-dbt:latest",
    "api_version": "auto",
    "auto_remove": True,
    "mount_tmp_dir": False,
    "network_mode": "ballistics_network",
    "mounts": DBT_MOUNTS,
    "environment": {"DBT_PROFILES_DIR": "/root/.dbt"},
    "working_dir": "/opt/dbt",
}


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
        from etl.src.data_detector import (
            fixture_kickoff_watermark,
            record_data_movement_from_context,
        )

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
        watermark_column, watermark_min, watermark_max = fixture_kickoff_watermark(updates)
        record_data_movement_from_context(
            context,
            movement_type="api_to_s3",
            source_system="api_sports",
            source_name="fixtures:incremental",
            source_s3_key=s3_key,
            row_count=len(updates),
            inserted_count=len(updates),
            failed_count=0,
            watermark_column=watermark_column,
            watermark_min=watermark_min,
            watermark_max=watermark_max,
            status="success",
            details={
                "fixture_ids_requested": len(ids),
                "ds": ds,
            },
        )
        logger.info("Extracted %d fixture updates → s3://%s", len(updates), s3_key)
        return s3_key

    # ------------------------------------------------------------------
    # Load: read from S3 → upsert to Postgres
    # ------------------------------------------------------------------

    @task
    def load_updates(s3_key: str, **context):
        """
        Read fixture updates from S3 and apply to raw.raw_fixtures
        via batched upserts.
        """
        from etl.src.update_fixtures_main import apply_updates_to_db
        from etl.src.s3_landing import read_fixtures_from_s3
        from etl.src.logger import get_logger
        from etl.src.config import UPDATE_FIXTURES_LOG
        from etl.src.data_detector import (
            fixture_kickoff_watermark,
            record_data_movement_from_context,
            record_table_snapshot_from_context,
        )

        logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)

        if not s3_key:
            logger.info("No S3 key provided. Skipping load.")
            return

        updates = read_fixtures_from_s3(s3_key)
        if not updates:
            logger.info("Empty updates file. Skipping.")
            return

        count = apply_updates_to_db(updates)
        watermark_column, watermark_min, watermark_max = fixture_kickoff_watermark(updates)
        record_data_movement_from_context(
            context,
            movement_type="s3_to_raw",
            source_system="s3",
            source_s3_key=s3_key,
            target_schema="raw",
            target_table="raw_fixtures",
            row_count=len(updates),
            updated_count=count,
            failed_count=0,
            watermark_column=watermark_column,
            watermark_min=watermark_min,
            watermark_max=watermark_max,
            status="success",
            details={
                "payload_rows": len(updates),
                "applied_rows": count,
            },
        )
        record_table_snapshot_from_context(
            context,
            table_schema="raw",
            table_name="raw_fixtures",
            details={
                "movement_type": "s3_to_raw",
                "source_s3_key": s3_key,
                "payload_rows": len(updates),
                "applied_rows": count,
            },
        )
        logger.info("Applied %d fixture updates from %s", count, s3_key)

    # ------------------------------------------------------------------
    # Corrections: fix rescheduled kickoff times
    # ------------------------------------------------------------------

    @task
    def fixture_corrections(**context):
        """Correct missed and changed fixture kickoff times."""
        from etl.src.fixture_correction.main import run as run_correction
        from etl.src.logger import get_logger
        from etl.src.config import UPDATE_FIXTURES_LOG
        from etl.src.data_detector import record_data_movement_from_context

        logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)
        count_of_corrected_fixtures = run_correction(mode="both")
        record_data_movement_from_context(
            context,
            movement_type="raw_correction",
            source_system="raw",
            source_name="fixture_corrections",
            target_schema="raw",
            target_table="raw_fixtures",
            row_count=count_of_corrected_fixtures,
            updated_count=count_of_corrected_fixtures,
            failed_count=0,
            status="success",
            details={
                "correction_mode": "both",
                "count_of_corrected_fixtures": count_of_corrected_fixtures,
            },
        )
        logger.info("Fixture corrections applied: %d updates", count_of_corrected_fixtures)

    # ------------------------------------------------------------------
    # Standings: refresh league standings from API
    # ------------------------------------------------------------------

    @task
    def update_standings_task(**context):
        """Fetch and upsert current league standings."""
        from etl.src.update_standings import update_standings
        from etl.src.logger import get_logger
        from etl.src.config import UPDATE_FIXTURES_LOG
        from etl.src.data_detector import check_standings_stale_from_context, record_table_snapshot_from_context

        logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)
        standings_summary = update_standings(return_summary=True)
        count_of_standing_rows = standings_summary["total_rows"]
        record_table_snapshot_from_context(
            context,
            table_schema="raw",
            table_name="raw_league_standings",
            details={
                "refresh_rows": count_of_standing_rows,
                "league_count": standings_summary["league_count"],
                "leagues_updated": standings_summary["leagues_updated"],
            },
        )
        check_standings_stale_from_context(context)
        logger.info("Standings updated: %d rows", count_of_standing_rows)

    @task
    def run_data_quality_checks(**context):
        """Run detector checks that evaluate raw data state after daily refresh."""
        from etl.src.data_detector import check_raw_fixture_stale_from_context

        check_raw_fixture_stale_from_context(context)

    # ------------------------------------------------------------------
    # dbt: load seeds, then run transformations
    # ------------------------------------------------------------------

    dbt_seed = DockerOperator(
        task_id="dbt_seed",
        command=["dbt", "seed", "--profiles-dir", "/root/.dbt"],
        **DBT_OPERATOR_KWARGS,
    )

    dbt_run = DockerOperator(
        task_id="run_dbt",
        command=["dbt", "run", "--profiles-dir", "/root/.dbt"],
        **DBT_OPERATOR_KWARGS,
    )

    # ------------------------------------------------------------------
    # Wiring
    # ------------------------------------------------------------------

    s3_key = extract_updates()
    loaded = load_updates(s3_key)
    corrections = fixture_corrections()
    standings = update_standings_task()
    quality_checks = run_data_quality_checks()

    loaded >> corrections >> standings >> quality_checks >> dbt_seed >> dbt_run


fixture_update_dag()
