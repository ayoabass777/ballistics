"""engine_predict's consensus gate (codebase audit 2026-11-04, unit 5.2).

The quorum sensor used to sit downstream of all three model tasks with the
default all_success rule, so one failed model skipped the sensor and consensus
with it. The gate now starts alongside the models and decides from their
raw.model_runs markers (engine.eval.picks_store.gate_open).

The decision tests run everywhere. The graph tests need Airflow, which only
the CI `dag-tests` job installs; elsewhere they skip.
"""
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from engine.eval.picks_store import gate_open

GRACE = timedelta(minutes=30)
T0 = datetime(2026, 11, 13, 0, 9, tzinfo=timezone.utc)


def state(ready=(), failed=(), others=0, first=T0):
    n = len(ready) + len(failed) + others
    return {"ready": list(ready), "failed": list(failed), "n_finished": n,
            "first_finished": first if n else None}


def opens(st, minutes_after_first):
    return gate_open(st, 3, 2, GRACE, T0 + timedelta(minutes=minutes_after_first))


def test_all_three_ready_opens_at_once():
    assert opens(state(["bayesian", "dixon_coles", "mismatch"]), 2)


def test_two_ready_waits_for_the_third_within_grace():
    # A normal night: bayesian and dixon_coles finish a minute or two before
    # mismatch. Consensus must not run without mismatch.
    st = state(["bayesian", "dixon_coles"])
    assert not opens(st, 1)
    assert not opens(st, 29)


def test_slow_model_costs_at_most_the_grace_window():
    assert opens(state(["bayesian", "dixon_coles"]), 30)


def test_failed_model_waits_grace_for_its_retry_then_opens():
    st = state(["bayesian", "dixon_coles"], failed=["mismatch"])
    assert not opens(st, 5)   # Airflow retries the model after 5 minutes
    assert opens(st, 30)


def test_fewer_than_quorum_ready_never_opens():
    # A one-model consensus isn't one: the sensor times out and alerts instead.
    assert not opens(state(["bayesian"], failed=["dixon_coles", "mismatch"]), 120)
    assert not opens(state(["bayesian"]), 120)


def test_nothing_finished_stays_closed():
    assert not opens(state(), 120)


def test_all_finished_without_errors_opens_even_if_some_were_empty():
    # 'empty' = ran fine and found no picks; no reason to wait for it.
    assert opens(state(["bayesian", "dixon_coles"], others=1), 0)


# ── Graph (needs Airflow) ─────────────────────────────────────────────────────

MODEL_TASKS = {"model_mismatch", "model_bayesian", "model_dixon_coles"}


@pytest.fixture(scope="module")
def dag():
    pytest.importorskip("airflow")
    pytest.importorskip("airflow.providers.docker")
    from airflow.models import DagBag

    os.environ.setdefault("PROJECT_ROOT", str(Path.cwd()))
    bag = DagBag(dag_folder="airflow/dags/engine_predict_dag.py", include_examples=False)
    assert bag.import_errors == {}
    return bag.dags["engine_predict"]   # get_dag() would need a metadata DB


def test_gate_is_not_downstream_of_any_model(dag):
    gate = dag.get_task("wait_for_quorum")
    assert gate.upstream_task_ids == {"wait_for_dbt_refresh"}
    for t in MODEL_TASKS:
        assert "wait_for_quorum" not in dag.get_task(t).get_flat_relative_ids(upstream=False)


def test_models_still_wait_for_dbt_and_mismatch_for_elo(dag):
    assert dag.get_task("model_bayesian").upstream_task_ids == {"wait_for_dbt_refresh"}
    assert dag.get_task("model_dixon_coles").upstream_task_ids == {"wait_for_dbt_refresh"}
    assert dag.get_task("model_mismatch").upstream_task_ids == {"refresh_elo"}
    assert dag.get_task("refresh_elo").upstream_task_ids == {"wait_for_dbt_refresh"}


def test_consensus_runs_after_the_gate_only(dag):
    assert dag.get_task("consensus").upstream_task_ids == {"wait_for_quorum"}
    assert dag.get_task("verify_daily_shortlists").upstream_task_ids == {"consensus"}


def test_gate_is_a_rescheduling_sensor_with_a_timeout(dag):
    gate = dag.get_task("wait_for_quorum")
    assert gate.mode == "reschedule"
    assert gate.trigger_rule == "all_success"   # upstream is dbt only
    assert 30 * 60 < gate.timeout <= 3 * 60 * 60


def test_forecast_history_is_independent_of_predictions(dag):
    assert dag.get_task("initialise_forecast_history").upstream_task_ids == {"wait_for_dbt_refresh"}
    assert dag.get_task("settle_forecast_history").upstream_task_ids == {"initialise_forecast_history"}
