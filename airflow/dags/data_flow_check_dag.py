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
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

default_args = {
    "owner": "Ayomide Abass",
    "depends_on_past": False,
    "start_date": datetime(2026, 10, 1),
    "retries": 0,
}


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
        # The 09:00 run on day D covers [D-1 09:00, D 09:00): its logical date is
        # D-1, the same logical date as the @daily pipeline runs that fired at
        # 00:00 on D.
        ds = date.fromisoformat(context["ds"])
        summary = run(ds)
        if summary["failed_or_missing"]:
            raise AirflowException(
                f"Data did not flow for {ds}: {', '.join(summary['failed_or_missing'])}")
        return summary

    check_daily_flow()


data_flow_check_dag()
