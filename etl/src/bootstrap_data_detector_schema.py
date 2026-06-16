"""
Bootstrap data flow detector schema/tables for a new environment.

Runs an idempotent SQL DDL script using the ETL database config, then validates
that the detector tables needed by pipeline instrumentation exist.
"""
from __future__ import annotations

from pathlib import Path

from etl.src.config import EXTRACT_FIXTURES_LOG
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger
from etl.src.schema_checks import ensure_data_detector_ready

logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)

DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "bootstrap_data_detector_schema.sql"


def bootstrap_data_detector_schema() -> None:
    if not DDL_PATH.is_file():
        raise FileNotFoundError(f"DDL file not found: {DDL_PATH}")

    ddl_sql = DDL_PATH.read_text(encoding="utf-8")

    with get_db_connection() as conn, conn.cursor() as cur:
        logger.info("Applying data detector bootstrap DDL: %s", DDL_PATH)
        cur.execute(ddl_sql)
        conn.commit()
        ensure_data_detector_ready(cur)
        logger.info("Bootstrap complete: data_detector tables are ready.")


if __name__ == "__main__":
    try:
        bootstrap_data_detector_schema()
    except Exception as exc:
        logger.error("Failed to bootstrap data detector schema: %s", exc, exc_info=True)
        raise
