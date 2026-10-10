"""
Engine Predict DAG — daily fan-out of the four models → event-gated consensus.

Each model runs as an independent DockerOperator task (ballistics-engine image),
writing its picks to raw.model_picks and a readiness marker to raw.model_runs.
Bayesian additionally persists its full fixture-analysis audit to
raw.model_analysis; consensus picks remain filtered in raw.model_picks.
A reschedule-mode quorum sensor gates consensus: it fires when >= QUORUM models
are ready, OR all four have finished, OR a grace window has passed since the first
finished — so a slow/failed model never blocks the slip. Consensus then builds
from raw.model_picks (--from-picks) and writes raw.consensus_pool + the JSON feed.

Task graph:
    wait_for_dbt_refresh
            ├──► refresh_elo ──► mismatch
            └──► [poisson] [bayesian] [dixon_coles]
                                  wait_for_quorum  ──►  consensus
                                                            │
                                                            ▼
                                                  verify_daily_shortlists
See docs/engine_orchestration.md.
"""

import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

MODELS = ("poisson", "mismatch", "bayesian", "dixon_coles")
QUORUM = 3                          # proceed once this many models are ready
GRACE = timedelta(minutes=30)       # ...or this long after the first finishes
DAYS_AHEAD = int(os.getenv("ENGINE_PREDICT_DAYS", "10"))
BANKROLL = os.getenv("ENGINE_BANKROLL", "300000")

PROJECT_ROOT = os.getenv("PROJECT_ROOT")
if not PROJECT_ROOT:
    raise RuntimeError("PROJECT_ROOT must be set (host repo path) for DockerOperator mounts.")

# Bind-mount live code + data so calibration_maps.json and consensus_slips_<ds>.json
# persist on the host across containers.
ENGINE_MOUNTS = [
    Mount(source=f"{PROJECT_ROOT}/engine", target="/opt/engine", type="bind"),
    Mount(source=f"{PROJECT_ROOT}/etl", target="/opt/etl", type="bind"),
    Mount(source=f"{PROJECT_ROOT}/data", target="/opt/data", type="bind"),
]
DB_ENV = {k: os.getenv(k) for k in ("DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT")}
DB_ENV["ENGINE_DB_HOST"] = os.getenv("ENGINE_DB_HOST", "ballistics_postgres")
# engine/eval/db.py reads ENGINE_DB_HOST; the etl helper (used by elo_service and
# the backtest modules) reads DB_HOST and defaults to localhost, which is dead
# inside the container.  Set both so either import path resolves to Postgres.
DB_ENV["DB_HOST"] = DB_ENV["ENGINE_DB_HOST"]

ENGINE_OPERATOR_KWARGS = {
    "image": "ballistics-engine:latest",
    "api_version": "auto",
    "auto_remove": True,
    "mount_tmp_dir": False,
    "network_mode": "ballistics_network",
    "mounts": ENGINE_MOUNTS,
    "environment": DB_ENV,
    "working_dir": "/opt",
}

default_args = {
    "owner": "Ayomide Abass",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="engine_predict",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["engine", "consensus", "fan-out"],
    doc_md=__doc__,
)
def engine_predict_dag():

    # fixture_update_dag and this DAG share the same daily logical date.  The
    # engine reads dbt facts only (raw.fact_head_to_head*, raw.fact_fixture_xg),
    # so it must wait for the dbt run that rebuilds them, which runs after the
    # fixture load, xG and standings.  Waiting only for xG (as before) let every
    # model read the previous night's facts (seen Oct 5-7: models finished
    # before run_dbt started).  Reschedule mode frees the worker while we wait.
    wait_for_dbt_refresh = ExternalTaskSensor(
        task_id="wait_for_dbt_refresh",
        external_dag_id="fixture_update_dag",
        external_task_id="run_dbt",
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )

    refresh_elo = DockerOperator(
        task_id="refresh_elo",
        command=["python", "-m", "engine.elo.elo_service"],
        **ENGINE_OPERATOR_KWARGS,
    )

    model_tasks = {
        m: DockerOperator(
            task_id=f"model_{m}",
            command=["python", "-m", "engine.eval.run_model",
                     "--model", m, "--run-ds", "{{ ds }}", "--days", str(DAYS_AHEAD)],
            **ENGINE_OPERATOR_KWARGS,
        )
        for m in MODELS
    }

    @task.sensor(task_id="wait_for_quorum", poke_interval=60, timeout=60 * 60,
                 mode="reschedule")
    def wait_for_quorum(**ctx) -> PokeReturnValue:
        # Native task (engine/ is mounted into Airflow; needs only psycopg2).
        from engine.eval.picks_store import quorum_state
        st = quorum_state(ctx["ds"])
        done = len(st["ready"]) >= QUORUM or st["n_finished"] >= len(MODELS)
        if not done and st["first_finished"] is not None:
            done = datetime.now(timezone.utc) - st["first_finished"] >= GRACE
        return PokeReturnValue(is_done=done, xcom_value=st["ready"])

    consensus = DockerOperator(
        task_id="consensus",
        command=["python", "-m", "engine.eval.consensus_slips",
                 "--from-picks", "--run-ds", "{{ ds }}", "--bankroll", str(BANKROLL)],
        **ENGINE_OPERATOR_KWARGS,
    )

    verify_daily_shortlists = DockerOperator(
        task_id="verify_daily_shortlists",
        command=["python", "-m", "engine.eval.validate_daily_shortlists",
                 "--run-ds", "{{ ds }}"],
        **ENGINE_OPERATOR_KWARGS,
    )

    gate = wait_for_quorum()
    # Independent of prediction success: refresh outcomes for prior forecasts.
    initialise_forecast_history = DockerOperator(
        task_id="initialise_forecast_history",
        command=["python", "-m", "engine.eval.forecast_history", "init"],
        **ENGINE_OPERATOR_KWARGS,
    )
    settle_forecast_history = DockerOperator(
        task_id="settle_forecast_history",
        command=["python", "-m", "engine.eval.forecast_history", "settle"],
        **ENGINE_OPERATOR_KWARGS,
    )
    wait_for_dbt_refresh >> initialise_forecast_history >> settle_forecast_history
    wait_for_dbt_refresh >> [
        model_tasks["poisson"],
        model_tasks["bayesian"],
        model_tasks["dixon_coles"],
    ]
    wait_for_dbt_refresh >> refresh_elo >> model_tasks["mismatch"]
    list(model_tasks.values()) >> gate >> consensus >> verify_daily_shortlists


engine_predict_dag()
