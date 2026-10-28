"""Immediate Slack and email alerts for failed Airflow tasks in the ingestion DAGs."""

from __future__ import annotations

from typing import Any

from etl.src.data_detector.pipeline_runs import pipeline_run_failed
from etl.src.data_detector import notify


def pipeline_run_failed_with_alert(context: dict[str, Any]) -> None:
    """Record the failure and alert every configured channel when Airflow marks a task failed."""
    pipeline_run_failed(context)
    task = context.get("task_instance") or context.get("ti")
    dag_id = getattr(task, "dag_id", None) or "unknown_dag"
    task_id = getattr(task, "task_id", None) or "unknown_task"
    run_id = context.get("run_id") or getattr(task, "run_id", "unknown_run")
    error = context.get("exception")
    subject = f"[Ballistics] task failed: {dag_id}.{task_id}"
    body = (
        f"Airflow task failed after its configured retries.\n"
        f"DAG: {dag_id}\nTask: {task_id}\nRun: {run_id}\n"
        f"Error: {error if error is not None else 'see Airflow task logs'}\n"
    )
    notify.send_failure_alert(subject, body)
