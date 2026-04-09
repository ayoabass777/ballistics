"""
Bootstrap raw fixture-related schemas/tables for a new environment.

The SQL bootstrap includes:
- raw.raw_fixtures
- raw.raw_league_standings
- elo.elo_rating
- public.team_form

Runs an idempotent SQL DDL script using the ETL database config, then validates
the resulting fixture_id contract with the shared schema checks.
"""
from __future__ import annotations

from pathlib import Path

from etl.src.config import EXTRACT_FIXTURES_LOG
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger
from etl.src.schema_checks import ensure_raw_fixtures_bootstrap_ready

logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)

DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "bootstrap_raw_fixtures_schema.sql"


def bootstrap_raw_fixtures_schema() -> None:
    if not DDL_PATH.is_file():
        raise FileNotFoundError(f"DDL file not found: {DDL_PATH}")

    ddl_sql = DDL_PATH.read_text(encoding="utf-8")

    with get_db_connection() as conn, conn.cursor() as cur:
        logger.info("Applying bootstrap DDL: %s", DDL_PATH)
        cur.execute(ddl_sql)
        conn.commit()
        ensure_raw_fixtures_bootstrap_ready(cur)
        logger.info("Bootstrap complete: raw.raw_fixtures is ready for fixture ETL.")


if __name__ == "__main__":
    try:
        bootstrap_raw_fixtures_schema()
    except Exception as exc:
        logger.error("Failed to bootstrap raw fixtures schema: %s", exc, exc_info=True)
        raise
