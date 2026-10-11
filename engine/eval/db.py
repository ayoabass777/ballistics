"""Shared Postgres connection for the engine.

Credentials come from the repo `.env` (gitignored) — never hardcode them here.
Note: `.env`'s DB_HOST is the Docker container name (`ballistics_postgres`), which
only resolves inside the compose network. The engine runs on the host, so we
default the host to `localhost` and take an override from `ENGINE_DB_HOST`
(set that to `ballistics_postgres` when running inside a container).
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2

try:  # load repo-root .env regardless of cwd
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except Exception:  # noqa: BLE001 — dotenv optional; env may already be set
    pass


def get_conn():
    """psycopg2 connection using env-provided credentials."""
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "ballistics"),
        user=os.getenv("DB_USER", "ballistics_user"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("ENGINE_DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
    )
