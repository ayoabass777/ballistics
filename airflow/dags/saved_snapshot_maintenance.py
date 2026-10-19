"""Shared Saved payload maintenance. Paused on creation; retention is opt-in.

Mount the account repository and its environment in the worker, then configure
ACCOUNT_PROJECT_ROOT. No fallback to the public crest bucket is permitted.
"""
import os
import subprocess
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

@dag(schedule='15 4 * * *', start_date=datetime(2026, 10, 19), catchup=False,
     is_paused_upon_creation=True, max_active_runs=1,
     default_args={'owner': 'ballistics', 'retries': 1, 'retry_delay': timedelta(minutes=5)},
     tags=['account', 'retention'])
def saved_snapshot_maintenance():
    @task
    def maintain():
        root = os.getenv('ACCOUNT_PROJECT_ROOT')
        if not root:
            raise AirflowSkipException('ACCOUNT_PROJECT_ROOT mount is not configured')
        # Account module owns DB and archive configuration; subprocess does not print credentials.
        subprocess.run([sys.executable, '-m', 'scripts.maintain_saved_snapshots'], cwd=root, check=True)
    maintain()

saved_snapshot_maintenance()
