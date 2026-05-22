"""
Table snapshot detector records.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json

from etl.src.data_detector.common import airflow_identity, logger
from etl.src.extract_metadata import get_db_connection


def _existing_columns(cur, table_schema: str, table_name: str) -> List[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        """,
        (table_schema, table_name),
    )
    return [row[0] for row in cur.fetchall()]


def record_table_snapshot(
    *,
    table_schema: str,
    table_name: str,
    run_id: Optional[str] = None,
    dag_id: Optional[str] = None,
    pipeline_run_id: Optional[int] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Record row count and available freshness watermarks for a table.
    """
    with get_db_connection() as conn, conn.cursor() as cur:
        columns = set(_existing_columns(cur, table_schema, table_name))
        if not columns:
            raise RuntimeError(f"Missing table {table_schema}.{table_name}; cannot record table snapshot.")

        select_parts = ["COUNT(*)::bigint AS row_count"]
        if "created_at" in columns:
            select_parts.append("MAX(created_at) AS max_created_at")
        else:
            select_parts.append("NULL::timestamptz AS max_created_at")
        if "updated_at" in columns:
            select_parts.append("MAX(updated_at) AS max_updated_at")
        else:
            select_parts.append("NULL::timestamptz AS max_updated_at")
        if "kickoff_utc" in columns:
            select_parts.append("MAX(kickoff_utc) AS max_kickoff_utc")
        else:
            select_parts.append("NULL::timestamptz AS max_kickoff_utc")

        cur.execute(
            sql.SQL("SELECT {} FROM {}.{}").format(
                sql.SQL(", ").join(sql.SQL(part) for part in select_parts),
                sql.Identifier(table_schema),
                sql.Identifier(table_name),
            )
        )
        row_count, max_created_at, max_updated_at, max_kickoff_utc = cur.fetchone()

        cur.execute(
            """
            INSERT INTO data_detector.table_snapshots (
                pipeline_run_id,
                run_id,
                dag_id,
                table_schema,
                table_name,
                row_count,
                max_created_at,
                max_updated_at,
                max_kickoff_utc,
                details
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                pipeline_run_id,
                run_id,
                dag_id,
                table_schema,
                table_name,
                row_count,
                max_created_at,
                max_updated_at,
                max_kickoff_utc,
                Json(details or {}),
            ),
        )
        conn.commit()


def record_table_snapshot_from_context(
    context: Dict[str, Any],
    *,
    table_schema: str,
    table_name: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Best-effort Airflow-context wrapper for table snapshots.
    """
    identity = airflow_identity(context)
    try:
        record_table_snapshot(
            run_id=identity["run_id"],
            dag_id=identity["dag_id"],
            table_schema=table_schema,
            table_name=table_name,
            details=details,
        )
    except (RuntimeError, psycopg2.Error, OSError) as exc:
        logger.warning("Failed recording table snapshot for %s.%s: %s", table_schema, table_name, exc)
    except Exception as exc:
        logger.warning("Unexpected detector failure while recording table snapshot for %s.%s: %s", table_schema, table_name, exc)
