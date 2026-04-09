"""
Replay DAG — retries failed extractions from the S3 dead letter queue.

Runs daily, no sensor gating. Scans the dlq/ prefix in S3 for failed
league-season extractions, re-extracts from the API, loads to Postgres,
and cleans up successful entries. Logs remaining failures.

Task graph:

  replay_dlq ──► check_dlq

Independent from bootstrap_dag — runs regardless of metadata changes.
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task


default_args = {
    "owner": "Ayomide Abass",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 20),
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="replay_dag",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["replay", "dlq"],
    doc_md=__doc__,
)
def replay_dag():

    @task
    def replay_dlq():
        """
        Scan S3 DLQ for failed extractions.
        Re-extract from API, write to landing zone, load to Postgres.
        Clean up DLQ entries on success.
        """
        from etl.src.extract_fixtures import mark_fixtures_bootstrap_done
        from etl.src.extract_metadata import get_db_connection
        from etl.src.s3_landing import list_dlq_entries, replay_from_dlq
        from etl.src.logger import get_logger
        from etl.src.config import EXTRACT_FIXTURES_LOG

        logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)

        entries = list_dlq_entries()
        if not entries:
            logger.info("DLQ is empty. Nothing to replay.")
            return

        logger.info("Found %d DLQ entries. Attempting replay.", len(entries))
        conn = get_db_connection()
        replayed = 0
        failed = 0

        try:
            for entry in entries:
                try:
                    success = replay_from_dlq(entry, conn)
                    if success:
                        cur = conn.cursor()
                        cur.execute(
                            """
                            SELECT ls.league_season_id
                            FROM dim.dim_league_seasons ls
                            JOIN dim.dim_leagues l ON ls.league_id = l.league_id
                            WHERE l.api_league_id = %s AND ls.season = %s
                            """,
                            (entry["api_league_id"], entry["season_year"]),
                        )
                        row = cur.fetchone()
                        if row:
                            mark_fixtures_bootstrap_done(cur, row[0])
                        conn.commit()
                        cur.close()
                        replayed += 1
                    else:
                        failed += 1
                except Exception as exc:
                    conn.rollback()
                    logger.error(
                        "Replay failed for league %s season %s: %s",
                        entry["api_league_id"], entry["season_year"], exc, exc_info=True,
                    )
                    failed += 1

            logger.info("DLQ replay complete: %d replayed, %d still failed.", replayed, failed)

        finally:
            conn.close()

    @task
    def check_dlq():
        """
        Check if any DLQ entries remain after replay.
        Logs a warning with details. Production: this would trigger Slack/email.
        """
        from etl.src.s3_landing import list_dlq_entries
        from etl.src.logger import get_logger
        from etl.src.config import EXTRACT_FIXTURES_LOG

        logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)

        entries = list_dlq_entries()
        if not entries:
            logger.info("DLQ is clear. All league-seasons processed.")
            return

        logger.warning(
            "%d league-season(s) remain in DLQ after replay:",
            len(entries),
        )
        for entry in entries:
            logger.warning(
                "  - League %s, Season %s (failed on %s)",
                entry["api_league_id"], entry["season_year"], entry["ds"],
            )

    # Wiring
    replay_dlq() >> check_dlq()


replay_dag()
