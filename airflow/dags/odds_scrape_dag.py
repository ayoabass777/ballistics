"""
Odds Scrape DAG — pull live SportyBet pre-match odds for upcoming fixtures.

Runs independently of the consensus engine: it reads not-started fixtures
straight from `raw.raw_fixtures`, matches them to SportyBet events (country/date
+ fuzzy team names), and upserts decimal odds into `raw.sportybet_odds` (plus a
best-effort raw land to S3). The consensus engine later joins these odds for EV
filtering. See engine/eval/scrape_sportybet.py.

Runs every 4 hours so far-out fixtures get priced as their books open (~T-3) and
prices stay fresh near kickoff.

Task graph:  scrape_odds
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# The engine package is bind-mounted at /opt/airflow/engine (docker-compose).
# The scraper reaches Postgres via ENGINE_DB_HOST (set to the container name).
os.environ.setdefault("ENGINE_DB_HOST", "ballistics_postgres")

default_args = {
    "owner": "Ayomide Abass",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 20),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

SCRAPE_DAYS_AHEAD = int(os.getenv("ODDS_SCRAPE_DAYS", "7"))


@dag(
    dag_id="odds_scrape",
    default_args=default_args,
    schedule="0 */4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["odds", "sportybet", "engine"],
    doc_md=__doc__,
)
def odds_scrape_dag():

    @task
    def scrape_odds(**context) -> dict:
        from etl.src.logger import get_logger
        from engine.eval.scrape_sportybet import (
            fixtures_from_db, scrape_odds as run_scrape, persist_pg, persist_s3,
        )

        log = get_logger("odds_scrape")
        fixtures = fixtures_from_db(days=SCRAPE_DAYS_AHEAD)
        log.info("Loaded %d upcoming fixtures to price", len(fixtures))
        if not fixtures:
            return {"fixtures": 0, "priced": 0, "rows": 0}

        odds = run_scrape(fixtures)
        rows = persist_pg(odds)
        s3_uri = persist_s3(odds)
        log.info("Priced %d fixtures → %d odds rows upserted (s3=%s)",
                 len(odds), rows, s3_uri)
        return {"fixtures": len(fixtures), "priced": len(odds),
                "rows": rows, "s3": s3_uri}

    scrape_odds()


odds_scrape_dag()
