"""
Bootstrap the `app` schema (user accounts, D127) for a new environment.

Runs an idempotent SQL DDL script using the ETL database config, then checks
the account tables exist.
"""
from __future__ import annotations

from pathlib import Path

from etl.src.config import EXTRACT_FIXTURES_LOG
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger

logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)

DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "bootstrap_app_schema.sql"


def bootstrap_app_schema() -> None:
    if not DDL_PATH.is_file():
        raise FileNotFoundError(f"DDL file not found: {DDL_PATH}")

    ddl_sql = DDL_PATH.read_text(encoding="utf-8")

    with get_db_connection() as conn, conn.cursor() as cur:
        logger.info("Applying app bootstrap DDL: %s", DDL_PATH)
        cur.execute(ddl_sql)
        conn.commit()
        cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'app'")
        if cur.fetchone()[0] < 8:
            raise RuntimeError("app schema incomplete after bootstrap")
        logger.info("Bootstrap complete: app tables are ready.")


if __name__ == "__main__":
    try:
        bootstrap_app_schema()
    except Exception as exc:
        logger.error("Failed to bootstrap app schema: %s", exc, exc_info=True)
        raise
