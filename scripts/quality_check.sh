#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
export PROJECT_ROOT

python3 -m unittest discover -s tests

python3 -m py_compile \
  airflow/dags/bootstrap_dag.py \
  airflow/dags/fixture_update.py \
  airflow/dags/replay_dag.py \
  etl/src/data_detector/__init__.py \
  etl/src/data_detector/common.py \
  etl/src/data_detector/data_movements.py \
  etl/src/data_detector/pipeline_runs.py \
  etl/src/data_detector/quality_events.py \
  etl/src/data_detector/table_snapshots.py \
  etl/src/data_detector/watermarks.py \
  etl/src/fixture_correction/__init__.py \
  etl/src/fixture_correction/fixture_correction.py \
  etl/src/fixture_correction/main.py \
  etl/src/update_standings.py
