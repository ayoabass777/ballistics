"""
Pipeline run detector records.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.extras import Json

from etl.src.data_detector.common import coerce_iso, context_value, logger, utc_now
from etl.src.extract_metadata import get_db_connection


def _pipeline_run_payload(
    context: Dict[str, Any],
    status: str,
    error_message: Optional[str] = None,
) -> Dict[str, Any]:
    dag = context.get("dag")
    task = context.get("task")
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance") or context.get("ti")

    dag_id = getattr(dag, "dag_id", None) or getattr(task_instance, "dag_id", None)
    task_id = getattr(task, "task_id", None) or getattr(task_instance, "task_id", None)
    run_id = getattr(dag_run, "run_id", None) or context_value(context, "run_id")
    run_type = getattr(getattr(dag_run, "run_type", None), "value", None) or str(getattr(dag_run, "run_type", "") or "")

    metadata = {
        "try_number": getattr(task_instance, "try_number", None),
        "map_index": getattr(task_instance, "map_index", None),
        "logical_date": coerce_iso(context.get("logical_date")),
        "data_interval_start": coerce_iso(context.get("data_interval_start")),
        "data_interval_end": coerce_iso(context.get("data_interval_end")),
    }

    return {
        "run_id": run_id or "unknown",
        "dag_id": dag_id or "unknown",
        "task_id": task_id,
        "run_type": run_type or None,
        "status": status,
        "error_message": error_message,
        "metadata": metadata,
    }


def record_pipeline_run(
    *,
    run_id: str,
    dag_id: str,
    task_id: Optional[str],
    run_type: Optional[str],
    status: str,
    started_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    kafka_started_at: Optional[datetime] = None,
    kafka_finished_at: Optional[datetime] = None,
    kafka_topic: Optional[str] = None,
    kafka_partition: Optional[int] = None,
    kafka_start_offset: Optional[int] = None,
    kafka_finish_offset: Optional[int] = None,
    error_message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Upsert one task-level pipeline run state into data_detector.pipeline_runs.
    """
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_detector.pipeline_runs (
                run_id,
                dag_id,
                task_id,
                run_type,
                status,
                started_at,
                finished_at,
                kafka_started_at,
                kafka_finished_at,
                kafka_topic,
                kafka_partition,
                kafka_start_offset,
                kafka_finish_offset,
                error_message,
                metadata,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (dag_id, task_id, run_id) DO UPDATE SET
                run_type = COALESCE(EXCLUDED.run_type, data_detector.pipeline_runs.run_type),
                status = EXCLUDED.status,
                started_at = COALESCE(data_detector.pipeline_runs.started_at, EXCLUDED.started_at),
                finished_at = EXCLUDED.finished_at,
                kafka_started_at = COALESCE(data_detector.pipeline_runs.kafka_started_at, EXCLUDED.kafka_started_at),
                kafka_finished_at = EXCLUDED.kafka_finished_at,
                kafka_topic = COALESCE(EXCLUDED.kafka_topic, data_detector.pipeline_runs.kafka_topic),
                kafka_partition = COALESCE(EXCLUDED.kafka_partition, data_detector.pipeline_runs.kafka_partition),
                kafka_start_offset = COALESCE(data_detector.pipeline_runs.kafka_start_offset, EXCLUDED.kafka_start_offset),
                kafka_finish_offset = EXCLUDED.kafka_finish_offset,
                error_message = EXCLUDED.error_message,
                metadata = COALESCE(data_detector.pipeline_runs.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                updated_at = NOW()
            """,
            (
                run_id,
                dag_id,
                task_id,
                run_type,
                status,
                started_at,
                finished_at,
                kafka_started_at,
                kafka_finished_at,
                kafka_topic,
                kafka_partition,
                kafka_start_offset,
                kafka_finish_offset,
                error_message,
                Json(metadata or {}),
            ),
        )
        conn.commit()


def _record_from_context(context: Dict[str, Any], status: str, error_message: Optional[str] = None) -> None:
    payload = _pipeline_run_payload(context, status=status, error_message=error_message)
    event_time = utc_now()
    record_pipeline_run(
        run_id=payload["run_id"],
        dag_id=payload["dag_id"],
        task_id=payload["task_id"],
        run_type=payload["run_type"],
        status=payload["status"],
        started_at=event_time if status == "running" else None,
        finished_at=event_time if status in {"success", "failed"} else None,
        error_message=payload["error_message"],
        metadata=payload["metadata"],
    )


def _best_effort_record(context: Dict[str, Any], status: str, error_message: Optional[str] = None) -> None:
    try:
        _record_from_context(context, status=status, error_message=error_message)
    except (RuntimeError, psycopg2.Error, OSError) as exc:
        logger.warning("Failed recording pipeline run detector event (%s): %s", status, exc)
    except Exception as exc:
        logger.warning("Unexpected detector failure while recording pipeline run (%s): %s", status, exc)


def pipeline_run_started(context: Dict[str, Any]) -> None:
    """Airflow on_execute_callback."""
    _best_effort_record(context, status="running")


def pipeline_run_succeeded(context: Dict[str, Any]) -> None:
    """Airflow on_success_callback."""
    _best_effort_record(context, status="success")


def pipeline_run_failed(context: Dict[str, Any]) -> None:
    """Airflow on_failure_callback."""
    exception = context.get("exception")
    _best_effort_record(
        context,
        status="failed",
        error_message=str(exception) if exception is not None else None,
    )
