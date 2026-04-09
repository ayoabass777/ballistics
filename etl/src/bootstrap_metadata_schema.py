"""
Bootstrap metadata/dimension tables required by the ETL pipeline.

Applies idempotent DDL and validates the resulting metadata schema contract.
"""
from __future__ import annotations

from pathlib import Path

from etl.src.config import EXTRACT_FIXTURES_LOG
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger
from etl.src.schema_checks import ensure_metadata_bootstrap_ready

logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)

DDL_PATH = Path(__file__).resolve().parents[1] / "sql" / "bootstrap_metadata_schema.sql"


def bootstrap_metadata_schema() -> None:
    if not DDL_PATH.is_file():
        raise FileNotFoundError(f"DDL file not found: {DDL_PATH}")

    ddl_sql = DDL_PATH.read_text(encoding="utf-8")

    with get_db_connection() as conn, conn.cursor() as cur:
        logger.info("Applying metadata bootstrap DDL: %s", DDL_PATH)
        cur.execute(ddl_sql)
        conn.commit()
        ensure_metadata_bootstrap_ready(cur)
        logger.info("Bootstrap complete: metadata tables are ready for ETL.")


if __name__ == "__main__":
    try:
        bootstrap_metadata_schema()
    except Exception as exc:
        logger.error("Failed to bootstrap metadata schema: %s", exc, exc_info=True)
        raise

