"""
Replay DAG — hourly retries for the S3 dead letter queue (D147).

Runs every hour, no sensor gating. Each DLQ entry carries its own retry
state (etl/src/dlq.py): an entry is retried when its next_retry_at has
passed (one hour after the last attempt), and after 24 failed attempts it is
marked dead and alerted. Dead fixture entries (extractions and rows) are
still rechecked once a day, since no nightly run reloads them; dead
standings clear on the next clean nightly refresh.

  replay_dlq            failed fixture extractions: dlq/{league}/{season}/{ds}/error.json
  replay_fixture_rows_dlq  unprocessable fixture rows: dlq/fixtures/rows/{league}/{season}/open.json
  replay_standings_dlq  unprocessable standings rows: dlq/standings/{league}/{season}/open.json

Task graph:

  replay_dlq ──► replay_fixture_rows_dlq ──► replay_standings_dlq ──► check_dlq

Monitoring and alerting live in the daily data check (datasets dlq.fixtures,
dlq.fixture_rows and dlq.standings); check_dlq here only logs what is left.

Independent from bootstrap_dag — runs regardless of metadata changes.
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from etl.src.data_detector import (
    pipeline_run_failed,
    pipeline_run_skipped,
    pipeline_run_started,
    pipeline_run_succeeded,
)


default_args = {
    "owner": "Ayomide Abass",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_execute_callback": pipeline_run_started,
    "on_success_callback": pipeline_run_succeeded,
    "on_failure_callback": pipeline_run_failed,
    "on_skipped_callback": pipeline_run_skipped,
}


@dag(
    dag_id="replay_dag",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    tags=["replay", "dlq"],
    doc_md=__doc__,
)
def replay_dag():

    @task
    def replay_dlq(**context):
        """
        Retry due fixture DLQ entries: re-extract from API, write to landing
        zone, load to Postgres, move the entry to dlq/resolved/ on success.
        A failure counts an attempt (next try in an hour; dead after 24).
        """
        from etl.src import dlq
        from etl.src.extract_fixtures import mark_fixtures_bootstrap_done
        from etl.src.extract_metadata import get_db_connection
        from etl.src.replay_dlq import due_fixture_entries, fixture_attempt_failed, replay_from_dlq
        from etl.src.s3_landing import list_dlq_entries
        from etl.src.logger import get_logger
        from etl.src.config import EXTRACT_FIXTURES_LOG

        logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)

        listed = list_dlq_entries()
        entries = due_fixture_entries(listed)
        if not entries:
            logger.info("No fixture DLQ entries due (%d listed). Nothing to replay.", len(listed))
            return

        logger.info("%d of %d fixture DLQ entries due. Attempting replay.", len(entries), len(listed))
        conn = get_db_connection()
        replayed = 0
        failed = 0

        try:
            for entry in entries:
                try:
                    success = replay_from_dlq(entry, conn, context=context)
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
                        dlq.run_after_commit(conn)
                        cur.close()
                        replayed += 1
                    else:
                        fixture_attempt_failed(entry, "replay returned no rows")
                        failed += 1
                except Exception as exc:
                    conn.rollback()
                    dlq.discard_after_commit(conn)
                    logger.error(
                        "Replay failed for league %s season %s: %s",
                        entry["api_league_id"], entry["season_year"], exc, exc_info=True,
                    )
                    fixture_attempt_failed(entry, f"{type(exc).__name__}: {exc}")
                    failed += 1

            logger.info("DLQ replay complete: %d replayed, %d still failed.", replayed, failed)

        finally:
            conn.close()

    @task
    def replay_standings_dlq(**context):
        """Retry due standings DLQ entries (one API call per league season)."""
        from etl.src.standings_dlq import replay_standings_dlq as replay
        return replay(ds=context["ds"])

    @task
    def replay_fixture_rows_dlq(**context):
        """Retry due fixture-row DLQ entries (one API call per league season)."""
        from etl.src.fixtures_dlq import replay_fixture_rows_dlq as replay
        return replay(ds=context["ds"])

    @task
    def check_dlq(**context):
        """
        Log what is left in the DLQ. Hourly, so it writes no quality events:
        the daily data check owns monitoring (dlq.fixtures, dlq.standings),
        and dead entries were alerted when they gave up.
        """
        from etl.src import dlq
        from etl.src.s3_landing import list_dlq_entries
        from etl.src.standings_dlq import PREFIX
        from etl.src.fixtures_dlq import PREFIX as ROW_PREFIX
        from etl.src.logger import get_logger
        from etl.src.config import EXTRACT_FIXTURES_LOG

        logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)
        fixtures = list_dlq_entries()
        standings = [k for k in dlq.list_keys(PREFIX) if k.endswith("/open.json")]
        rows = [k for k in dlq.list_keys(ROW_PREFIX) if k.endswith("/open.json")]
        if not fixtures and not standings and not rows:
            logger.info("DLQ is clear.")
            return {"fixtures": 0, "standings": 0, "fixture_rows": 0}
        for entry in fixtures:
            logger.warning("  fixtures: league %s, season %s (failed on %s)",
                           entry["api_league_id"], entry["season_year"], entry["ds"])
        for key in standings:
            logger.warning("  standings: %s", key)
        for key in rows:
            logger.warning("  fixture rows: %s", key)
        return {"fixtures": len(fixtures), "standings": len(standings), "fixture_rows": len(rows)}

    # Wiring
    replay_dlq() >> replay_fixture_rows_dlq() >> replay_standings_dlq() >> check_dlq()


replay_dag()
