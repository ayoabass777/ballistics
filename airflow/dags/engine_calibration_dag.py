"""
Engine Calibration DAG — weekly walk-forward refit of the PAVA calibration maps.

Runs `calibration_wf` in the ballistics-engine image, regenerating
engine/eval/calibration_maps.json (bind-mounted, so the predict DAG's containers
pick up the fresh maps). Kept separate from engine_predict because it is expensive
(walk-forward over ~60k predictions) and only needs refitting when model logic or
the underlying data materially changes. See docs/engine_orchestration.md.
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_ROOT = os.getenv("PROJECT_ROOT")
if not PROJECT_ROOT:
    raise RuntimeError("PROJECT_ROOT must be set (host repo path) for DockerOperator mounts.")

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

default_args = {
    "owner": "Ayomide Abass",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="engine_calibration",
    default_args=default_args,
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["engine", "calibration"],
    doc_md=__doc__,
)
def engine_calibration_dag():
    calibration_wf = DockerOperator(
        task_id="calibration_wf",
        image="ballistics-engine:latest",
        api_version="auto",
        auto_remove=True,
        mount_tmp_dir=False,
        network_mode="ballistics_network",
        mounts=ENGINE_MOUNTS,
        environment=DB_ENV,
        working_dir="/opt",
        command=["python", "-m", "engine.eval.calibration_wf"],
    )

    # D120: log watched markets (Match Over 1.5, all vs Dixon-Coles alone) after
    # every refit; appends engine/eval/watch/*.csv and prints FLAG lines on drift.
    market_watch = DockerOperator(
        task_id="market_watch",
        image="ballistics-engine:latest",
        api_version="auto",
        auto_remove=True,
        mount_tmp_dir=False,
        network_mode="ballistics_network",
        mounts=ENGINE_MOUNTS,
        environment=DB_ENV,
        working_dir="/opt",
        command=["python", "-m", "engine.eval.diagnostics.market_watch"],
    )
    calibration_wf >> market_watch


engine_calibration_dag()
