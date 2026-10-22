# ballistics-research-api — the read-only research API (Discover, Match
# details, Forecast record) behind the frontend's server-side proxy.
#
# Build from the repo root (engine.api imports engine.eval helpers):
#   docker build -t ballistics-research-api -f engine/api/Dockerfile .
# Engine output files are excluded by engine/api/Dockerfile.dockerignore; the
# API reads only the database (FORECAST_API_DSN, a read-only role).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt

COPY engine/api/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && useradd --system --no-create-home api

WORKDIR /opt
COPY engine/ /opt/engine/
# Some working-tree files are owner-only (600); the API runs as a non-root user.
RUN chmod -R a+rX /opt/engine

USER api
EXPOSE 8001
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8001/openapi.json', timeout=3)"
CMD ["uvicorn", "engine.api.app:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "2"]
