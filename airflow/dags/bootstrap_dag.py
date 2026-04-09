"""
Bootstrap DAG — automated pipeline setup triggered by metadata changes.

Runs on a daily schedule. A custom MetadataChangeSensor checks whether
metadata.yaml has been modified since the last successful bootstrap.
Most days: sensor detects no change → all downstream tasks are skipped (soft_fail).
When you edit metadata.yaml (e.g. add a new league): sensor detects
the mtime change → full bootstrap chain executes automatically.

Task graph:

  sensor ──► metadata_schema ──► extract_metadata ──┐
                                                     ├──► extract_fixtures ──► load_fixtures
  sensor ──► raw_fixtures_schema ───────────────────┘

Failed extractions are written to a DLQ prefix in S3 (dlq/{league_id}/{season}/{ds}/).
A separate replay_dag handles DLQ retry and monitoring independently.
"""

import os
from datetime import datetime, timedelta
from typing import List

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sensors.base import BaseSensorOperator


# ---------------------------------------------------------------------------
# Custom sensor
# ---------------------------------------------------------------------------

class MetadataChangeSensor(BaseSensorOperator):
    """
    Compares metadata.yaml's modification time against the last-processed
    timestamp stored in an Airflow Variable. Triggers only when the file
    has been modified since the last successful bootstrap.
    """

    def __init__(self, filepath: str, **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath

    def poke(self, context) -> bool:
        if not os.path.isfile(self.filepath):
            self.log.info("File not found: %s", self.filepath)
            return False

        current_mtime = os.path.getmtime(self.filepath)
        last_mtime = float(Variable.get("metadata_last_mtime", default_var=0))

        if current_mtime > last_mtime:
            self.log.info(
                "metadata.yaml changed (mtime %.0f > stored %.0f). Triggering bootstrap.",
                current_mtime,
                last_mtime,
            )
            Variable.set("metadata_last_mtime", str(current_mtime))
            return True

        self.log.info("metadata.yaml unchanged (mtime %.0f). Skipping.", current_mtime)
        return False


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

METADATA_PATH = os.getenv(
    "METADATA_FILE",
    os.path.join(os.getcwd(), "data", "metadata.yaml"),
)

default_args = {
    "owner": "Ayomide Abass",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="bootstrap_dag",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["bootstrap"],
    doc_md=__doc__,
)
def bootstrap_dag():

    wait_for_metadata_change = MetadataChangeSensor(
        task_id="wait_for_metadata_change",
        filepath=METADATA_PATH,
        poke_interval=60,
        timeout=60,
        mode="poke",
        soft_fail=True,
    )

    # ------------------------------------------------------------------
    # Branch 1: Metadata schema + extract
    # ------------------------------------------------------------------

    @task
    def bootstrap_metadata_schema():
        """Create dim schema and dimension tables if they don't exist."""
        from etl.src.bootstrap_metadata_schema import bootstrap_metadata_schema as _bootstrap
        _bootstrap()

    @task
    def extract_metadata():
        """Read metadata.yaml, hit API for league/team info, upsert into dim tables."""
        from etl.src.extract_metadata import extract_metadata as _extract
        _extract(include_logos=False)

    # ------------------------------------------------------------------
    # Branch 2: Raw fixtures schema
    # ------------------------------------------------------------------

    @task
    def bootstrap_raw_fixtures_schema():
        """Create raw schema and raw_fixtures / raw_league_standings tables."""
        from etl.src.bootstrap_raw_fixtures_schema import bootstrap_raw_fixtures_schema as _bootstrap
        _bootstrap()

    # ------------------------------------------------------------------
    # Extract → S3 (with DLQ on failure)
    # ------------------------------------------------------------------

    @task
    def extract_fixtures(**context) -> List[str]:
        """
        Fetch fixtures for all league-seasons with fixtures_bootstrap_done=FALSE.
        Writes raw JSON to S3 landing zone per league-season.
        Failures are written to the DLQ prefix in S3.
        Returns list of successful S3 keys via XCom.
        """
        from etl.src.extract_fixtures import (
            FIXTURES_ENDPOINT,
            extract_fixtures_field,
            fetch_fixtures,
            get_season_rows,
        )
        from etl.src.extract_metadata import get_db_connection
        from etl.src.schema_checks import ensure_raw_fixtures_bootstrap_ready
        from etl.src.s3_landing import write_fixtures_to_s3, write_to_dlq
        from etl.src.logger import get_logger
        from etl.src.config import EXTRACT_FIXTURES_LOG

        logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)

        conn = get_db_connection()
        cur = conn.cursor()
        s3_keys = []

        try:
            ensure_raw_fixtures_bootstrap_ready(cur)
            season_rows = get_season_rows(cur)

            if not season_rows:
                logger.info("No league seasons pending fixture extraction.")
                return s3_keys

            ds = context["ds"]

            for country_name, league_name, api_league_id, season_year, league_season_id in season_rows:
                logger.info("Extracting %s (%s)", league_name, season_year)
                try:
                    fixtures = fetch_fixtures(
                        FIXTURES_ENDPOINT,
                        params={"league": api_league_id, "season": season_year},
                    )
                    extracted = extract_fixtures_field(fixtures)

                    if not extracted:
                        logger.warning("No fixtures extracted for %s (%s)", league_name, season_year)
                        continue

                    s3_key = write_fixtures_to_s3(extracted, api_league_id, season_year, ds)
                    s3_keys.append(s3_key)

                    logger.info(
                        "Extracted %d fixtures for %s (%s) → s3://%s",
                        len(extracted), league_name, season_year, s3_key,
                    )

                except Exception as exc:
                    logger.error(
                        "Failed extracting %s (%s): %s",
                        league_name, season_year, exc, exc_info=True,
                    )
                    write_to_dlq(
                        api_league_id=api_league_id,
                        season_year=season_year,
                        ds=ds,
                        error=exc,
                        league_name=league_name,
                    )
                    continue

            return s3_keys

        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # Load from S3 → Postgres
    # ------------------------------------------------------------------

    @task
    def load_fixtures(s3_keys: List[str]):
        """
        Read extracted fixtures from S3 and COPY into raw.raw_fixtures.
        Marks league-seasons as bootstrapped after successful load.
        """
        from etl.src.extract_fixtures import mark_fixtures_bootstrap_done
        from etl.src.extract_metadata import get_db_connection
        from etl.src.schema_checks import ensure_raw_fixtures_bootstrap_ready
        from etl.src.s3_landing import load_fixtures_from_s3
        from etl.src.logger import get_logger
        from etl.src.config import EXTRACT_FIXTURES_LOG

        logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)

        if not s3_keys:
            logger.info("No S3 keys to load. Skipping.")
            return

        conn = get_db_connection()
        cur = conn.cursor()
        total_loaded = 0

        try:
            ensure_raw_fixtures_bootstrap_ready(cur)

            for s3_key in s3_keys:
                try:
                    count = load_fixtures_from_s3(s3_key, conn)

                    parts = s3_key.split("/")
                    api_league_id = int(parts[1])
                    season_year = int(parts[2])

                    cur.execute(
                        """
                        SELECT ls.league_season_id
                        FROM dim.dim_league_seasons ls
                        JOIN dim.dim_leagues l ON ls.league_id = l.league_id
                        WHERE l.api_league_id = %s AND ls.season = %s
                        """,
                        (api_league_id, season_year),
                    )
                    row = cur.fetchone()
                    if row:
                        mark_fixtures_bootstrap_done(cur, row[0])

                    conn.commit()
                    total_loaded += count
                    logger.info("Loaded %d fixtures from %s", count, s3_key)

                except Exception as exc:
                    conn.rollback()
                    logger.error("Failed loading from %s: %s", s3_key, exc, exc_info=True)
                    continue

            logger.info("Total fixtures loaded: %d from %d S3 keys", total_loaded, len(s3_keys))

        finally:
            cur.close()
            conn.close()

    # ------------------------------------------------------------------
    # Wiring
    # ------------------------------------------------------------------

    metadata_schema = bootstrap_metadata_schema()
    metadata_extract = extract_metadata()
    raw_schema = bootstrap_raw_fixtures_schema()
    fixtures_extracted = extract_fixtures()
    fixtures_loaded = load_fixtures(fixtures_extracted)

    #   sensor ──► metadata_schema ──► extract_metadata ──┐
    #                                                      ├──► extract ──► load
    #   sensor ──► raw_fixtures_schema ───────────────────┘

    wait_for_metadata_change >> metadata_schema >> metadata_extract >> fixtures_extracted
    wait_for_metadata_change >> raw_schema >> fixtures_extracted


bootstrap_dag()
