FROM apache/airflow:2.9.3-python3.10

WORKDIR /opt/airflow
ENV PYTHONPATH=${PYTHONPATH}:/opt/airflow

COPY etl/requirements.txt /tmp/etl-requirements.txt
RUN pip install --no-cache-dir -r /tmp/etl-requirements.txt

COPY airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt

USER airflow
