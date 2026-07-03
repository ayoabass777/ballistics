"""
Engine Predict DAG — daily fan-out of the four models → event-gated consensus.

Each model runs as an independent DockerOperator task (ballistics-engine image),
writing its picks to raw.model_picks and a readiness marker to raw.model_runs.
A reschedule-mode quorum sensor gates consensus: it fires when >= QUORUM models
are ready, OR all four have finished, OR a grace window has passed since the first
finished — so a slow/failed model never blocks the slip. Consensus then builds
from raw.model_picks (--from-picks) and writes raw.consensus_pool + the JSON feed.

Task graph:
    [poisson] [mismatch] [bayesian] [dixon_coles]   (parallel model tasks)
                        wait_for_quorum  ──►  consensus
See docs/engine_orchestration.md.
"""

import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
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

    model_tasks = [
        DockerOperator(
            task_id=f"model_{m}",
            command=["python", "-m", "engine.eval.run_model",
                     "--model", m, "--run-ds", "{{ ds }}", "--days", str(DAYS_AHEAD)],
            **ENGINE_OPERATOR_KWARGS,
        )
        for m in MODELS
    ]

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

    gate = wait_for_quorum()
    model_tasks >> gate >> consensus


engine_predict_dag()
