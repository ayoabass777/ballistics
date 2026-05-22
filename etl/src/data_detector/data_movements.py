"""
Data movement detector records.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.extras import Json

from etl.src.data_detector.common import airflow_identity, logger
from etl.src.extract_metadata import get_db_connection


def record_data_movement(
    *,
    movement_type: str,
    source_system: str,
    status: str,
    run_id: Optional[str] = None,
    dag_id: Optional[str] = None,
    task_id: Optional[str] = None,
    source_name: Optional[str] = None,
    source_s3_key: Optional[str] = None,
    target_schema: Optional[str] = None,
    target_table: Optional[str] = None,
    row_count: Optional[int] = None,
    inserted_count: Optional[int] = None,
    updated_count: Optional[int] = None,
    deleted_count: Optional[int] = None,
    failed_count: int = 0,
    watermark_column: Optional[str] = None,
    watermark_min: Optional[datetime] = None,
    watermark_max: Optional[datetime] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Insert one data movement audit record.
    """
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_detector.data_movements (
                run_id,
                dag_id,
                task_id,
                movement_type,
                source_system,
                source_name,
                source_s3_key,
                target_schema,
                target_table,
                row_count,
                inserted_count,
                updated_count,
                deleted_count,
                failed_count,
                watermark_column,
                watermark_min,
                watermark_max,
                status,
                details
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                dag_id,
                task_id,
                movement_type,
                source_system,
                source_name,
                source_s3_key,
                target_schema,
                target_table,
                row_count,
                inserted_count,
                updated_count,
                deleted_count,
                failed_count,
                watermark_column,
                watermark_min,
                watermark_max,
                status,
                Json(details or {}),
            ),
        )
        conn.commit()


def record_data_movement_from_context(
    context: Dict[str, Any],
    *,
    movement_type: str,
    source_system: str,
    status: str,
    source_name: Optional[str] = None,
    source_s3_key: Optional[str] = None,
    target_schema: Optional[str] = None,
    target_table: Optional[str] = None,
    row_count: Optional[int] = None,
    inserted_count: Optional[int] = None,
    updated_count: Optional[int] = None,
    deleted_count: Optional[int] = None,
    failed_count: int = 0,
    watermark_column: Optional[str] = None,
    watermark_min: Optional[datetime] = None,
    watermark_max: Optional[datetime] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Best-effort Airflow-context wrapper for data movement records.
    """
    identity = airflow_identity(context)
    try:
        record_data_movement(
            run_id=identity["run_id"],
            dag_id=identity["dag_id"],
            task_id=identity["task_id"],
            movement_type=movement_type,
            source_system=source_system,
            source_name=source_name,
            source_s3_key=source_s3_key,
            target_schema=target_schema,
            target_table=target_table,
            row_count=row_count,
            inserted_count=inserted_count,
            updated_count=updated_count,
            deleted_count=deleted_count,
            failed_count=failed_count,
            watermark_column=watermark_column,
            watermark_min=watermark_min,
            watermark_max=watermark_max,
            status=status,
            details=details,
        )
    except (RuntimeError, psycopg2.Error, OSError) as exc:
        logger.warning("Failed recording data movement detector event (%s): %s", movement_type, exc)
    except Exception as exc:
        logger.warning("Unexpected detector failure while recording data movement (%s): %s", movement_type, exc)
