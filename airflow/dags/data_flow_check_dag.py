"""
Daily data flow check (etl/src/data_detector/daily_flow.py).

Runs at 09:00 UTC, after fixture_update_dag and engine_predict for the same
logical date have had time to finish (both fire at 00:00 UTC; consensus has
been finishing around 03:00 UTC). It checks every dataset in
etl/src/data_detector/datasets.yml for that date, records the result per day in
data_detector.daily_flow_checks, opens/resolves `daily_flow` quality events,
and alerts through the channels configured in the environment (Slack webhook
and/or SMTP email; see etl/src/data_detector/notify.py).

The task fails when any dataset failed or is missing, so the run is red in
Airflow as well as alerted.
"""
from datetime import date, datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

default_args = {
    "owner": "Ayomide Abass",
    "depends_on_past": False,
    "start_date": datetime(2026, 10, 1),
    "retries": 0,
}


def check_date(context, now=None):
    """Which pipeline day to check.

    Scheduled: the 09:00 run on day D covers [D-1 09:00, D 09:00), so its
    logical date is D-1, the same as the @daily pipeline runs that fired at
    00:00 on D. Manual: the logical date is the trigger time, which would
    check a day that has not run yet, so use the last day whose pipelines
    have had until 09:00 UTC to finish. A {"ds": "YYYY-MM-DD"} conf overrides.
    """
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    if conf.get("ds"):
        return date.fromisoformat(conf["ds"])
    if context.get("dag_run") is not None and context["dag_run"].run_type == "scheduled":
        return date.fromisoformat(context["ds"])
    now = now or datetime.now(timezone.utc)
    return (now - timedelta(hours=9)).date() - timedelta(days=1)


def airflow_task_state(dag_id: str, task_id: str, ds: date):
    """Airflow's own state for a @daily task on logical date ds (None if no run).

    Fallback for the detector: tasks that ran before their pipeline_run_*
    callbacks were deployed, or whose callback failed, still count."""
    from airflow.models import DagRun
    logical = datetime(ds.year, ds.month, ds.day, tzinfo=timezone.utc)
    runs = DagRun.find(dag_id=dag_id, execution_date=logical)
    if not runs:
        return None
    ti = runs[0].get_task_instance(task_id)
    return ti.state if ti is not None else None


@dag(
    dag_id="data_flow_check",
    default_args=default_args,
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["data_detector", "monitoring"],
    doc_md=__doc__,
)
def data_flow_check_dag():

    @task
    def check_daily_flow(**context) -> dict:
        from etl.src.data_detector.daily_flow import run
        ds = check_date(context)
        summary = run(ds, task_state=airflow_task_state)
        if summary["failed_or_missing"]:
            raise AirflowException(
                f"Data did not flow for {ds}: {', '.join(summary['failed_or_missing'])}")
        return summary

    check_daily_flow()


data_flow_check_dag()
