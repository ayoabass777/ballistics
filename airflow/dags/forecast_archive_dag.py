"""Daily verified incremental forecast archive; PostgreSQL originals retained."""
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable

@dag(dag_id='forecast_history_archive', schedule='45 5 * * *',
     start_date=datetime(2026,10,19), catchup=False, max_active_runs=1,
     is_paused_upon_creation=True,
     default_args={'owner':'ballistics','retries':2,'retry_delay':timedelta(minutes=5)},
     tags=['forecast','archive'])
def forecast_history_archive():
    @task
    def incremental():
        import boto3
        from engine.eval.db import get_conn
        from engine.eval.forecast_archive import archive
        bucket=Variable.get('forecast_archive_bucket', default_var=os.getenv('FORECAST_ARCHIVE_BUCKET'))
        if not bucket: raise ValueError('Configure forecast_archive_bucket before enabling this DAG')
        os.environ.setdefault('ENGINE_DB_HOST',os.getenv('DB_HOST','ballistics_postgres'))
        conn=get_conn()
        try: return archive(conn,boto3.client('s3'),bucket,batch_size=500,mode='incremental')
        finally: conn.close()
    incremental()

forecast_history_archive()
