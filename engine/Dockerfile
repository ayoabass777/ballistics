# ballistics-engine — image for running the prediction/consensus/calibration
# modules under Airflow via DockerOperator (mirrors ballistics-dbt). Carries the
# heavy stack the Airflow image lacks (scipy/statsmodels/scikit-learn).
#
# Build from the repo root so engine/, etl/ and data/ are in the context:
#   docker build -t ballistics-engine:latest -f engine/Dockerfile .
FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc g++ libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# engine deps + etl deps (engine imports etl.src.* which pulls requests etc.)
COPY engine/requirements.txt /tmp/engine-requirements.txt
COPY etl/requirements.txt /tmp/etl-requirements.txt
RUN pip install --no-cache-dir -r /tmp/engine-requirements.txt -r /tmp/etl-requirements.txt

WORKDIR /opt
ENV PYTHONPATH=/opt
# Baked in as a fallback; the DAG bind-mounts these live so calibration_maps.json
# and consensus_slips_<ds>.json persist on the host across containers.
COPY engine/ /opt/engine/
COPY etl/ /opt/etl/
COPY data/ /opt/data/

CMD ["python", "-c", "import engine.eval.consensus_slips; print('ballistics-engine ok')"]
