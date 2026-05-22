"""
Shared helpers for data detector modules.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from etl.src.config import UPDATE_FIXTURES_LOG
from etl.src.logger import get_logger

logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def coerce_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def coerce_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def context_value(context: Dict[str, Any], name: str) -> Optional[str]:
    value = context.get(name)
    return str(value) if value is not None else None


def airflow_identity(context: Dict[str, Any]) -> Dict[str, Optional[str]]:
    dag = context.get("dag")
    task = context.get("task")
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance") or context.get("ti")

    dag_id = getattr(dag, "dag_id", None) or getattr(task_instance, "dag_id", None)
    task_id = getattr(task, "task_id", None) or getattr(task_instance, "task_id", None)
    run_id = getattr(dag_run, "run_id", None) or context_value(context, "run_id")
    return {
        "run_id": run_id or "unknown",
        "dag_id": dag_id or "unknown",
        "task_id": task_id,
    }
