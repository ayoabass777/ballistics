"""
S3 landing zone operations for the Ballistics pipeline.

Handles writing raw extracted fixtures to S3 (landing zone),
reading them back for loading into Postgres, and managing
a dead letter queue (DLQ) for failed extractions.

S3 prefix structure:
    full_load/{league_id}/{season}/{ds}/fixtures.json   — successful extractions
    incremental/{ds}/fixtures.json                       — daily updates
    dlq/{league_id}/{season}/{ds}/error.json             — failed extractions with error context

The DLQ enables a replay pattern: failed extractions are stored with
enough context to retry without re-discovering what failed.
"""

import json
import csv
import os
import traceback
from datetime import datetime, timezone
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

import boto3
import psycopg2

from etl.src.config import EXTRACT_FIXTURES_LOG
from etl.src.logger import get_logger

logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)

S3_BUCKET = os.getenv("S3_BUCKET", "ballistics-raw")

RAW_TABLE = "raw.raw_fixtures"
RAW_COLUMNS = [
    "api_fixture_id",
    "api_league_id",
    "season",
    "kickoff_utc",
    "fixture_status",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "home_team_halftime_goal",
    "away_team_halftime_goal",
    "home_team_fulltime_goal",
    "away_team_fulltime_goal",
    "home_fulltime_result",
    "away_fulltime_result",
    "home_halftime_result",
    "away_halftime_result",
    "round",
]
COPY_SQL = f"COPY {RAW_TABLE} ({', '.join(RAW_COLUMNS)}) FROM STDIN WITH CSV NULL ''"


# ---------------------------------------------------------------------------
# S3 Write (landing zone)
# ---------------------------------------------------------------------------

def write_fixtures_to_s3(
    fixtures: List[Dict[str, Any]],
    api_league_id: int,
    season_year: int,
    ds: str,
    bucket: str = S3_BUCKET,
) -> str:
    s3_key = f"full_load/{api_league_id}/{season_year}/{ds}/fixtures.json"
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(fixtures, default=str),
        ContentType="application/json",
    )
    logger.info("Wrote %d fixtures to s3://%s/%s", len(fixtures), bucket, s3_key)
    return s3_key


def write_raw_payload_to_s3(
    payload: List[Dict[str, Any]],
    api_league_id: int,
    season_year: int,
    ds: str,
    bucket: str = S3_BUCKET,
) -> str:
    """Land the untouched API response, so a field we don't extract today can
    be recovered later without another API pull."""
    s3_key = f"raw_payload/fixtures/{api_league_id}/{season_year}/{ds}/fixtures.json"
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(payload, default=str),
        ContentType="application/json",
    )
    logger.info("Wrote raw payload (%d fixtures) to s3://%s/%s", len(payload), bucket, s3_key)
    return s3_key


def write_incremental_to_s3(
    fixtures: List[Dict[str, Any]],
    ds: str,
    bucket: str = S3_BUCKET,
) -> str:
    """Write incremental fixture updates to S3 landing zone."""
    s3_key = f"incremental/{ds}/fixtures.json"
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(fixtures, default=str),
        ContentType="application/json",
    )
    logger.info("Wrote %d incremental fixtures to s3://%s/%s", len(fixtures), bucket, s3_key)
    return s3_key


# ---------------------------------------------------------------------------
# S3 Read
# ---------------------------------------------------------------------------

def read_fixtures_from_s3(s3_key: str, bucket: str = S3_BUCKET) -> List[Dict[str, Any]]:
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=s3_key)
    fixtures = json.loads(obj["Body"].read().decode("utf-8"))
    logger.info("Read %d fixtures from s3://%s/%s", len(fixtures), bucket, s3_key)
    return fixtures


# ---------------------------------------------------------------------------
# Postgres Load (from S3)
# ---------------------------------------------------------------------------

def _records_to_buffer(fixtures: List[Dict[str, Any]]) -> Tuple[StringIO, int]:
    buffer = StringIO()
    writer = csv.writer(buffer)
    for fixture in fixtures:
        row = ["" if fixture.get(col) is None else fixture.get(col) for col in RAW_COLUMNS]
        writer.writerow(row)
    buffer.seek(0)
    return buffer, len(fixtures)


def load_fixtures_from_s3(
    s3_key: str,
    conn: psycopg2.extensions.connection,
    bucket: str = S3_BUCKET,
    source: str = "bootstrap",
    detailed: bool = False,
):
    """Load a landed league season into raw.raw_fixtures.

    Rows that can't be loaded (no fixture id, kickoff, status or team id) are
    parked in the row-level DLQ instead of failing the whole COPY
    (etl/src/fixtures_dlq.py, D148). Returns the loaded count, or with
    detailed=True the LoadResult (loaded, parked, ...).
    """
    from etl.src.dlq import LoadResult
    from etl.src.fixtures_dlq import load_league_fixtures

    fixtures = read_fixtures_from_s3(s3_key, bucket)
    if not fixtures:
        logger.info("Empty file at s3://%s/%s. Skipping.", bucket, s3_key)
        return LoadResult(loaded=0) if detailed else 0

    parts = s3_key.split("/")
    league = {"api_league_id": int(parts[1]), "season": int(parts[2])}
    ds = parts[3] if len(parts) > 4 else ""

    try:
        result = load_league_fixtures(fixtures, league=league, ds=ds, conn=conn, source=source)
    except Exception:
        conn.rollback()
        raise
    logger.info("Loaded %d fixtures from s3://%s/%s into %s (%d parked)",
                result.loaded, bucket, s3_key, RAW_TABLE, len(result.parked))
    return result if detailed else result.loaded


# ---------------------------------------------------------------------------
# Dead Letter Queue (DLQ)
# ---------------------------------------------------------------------------

def write_to_dlq(
    api_league_id: int,
    season_year: int,
    ds: str,
    error: Exception,
    league_name: str = "",
    source_s3_key: str | None = None,
    failure_stage: str = "extract",
    bucket: str = S3_BUCKET,
) -> str:
    """
    Write a failed extraction to the DLQ prefix in S3.
    Stores error context for replay — the replay task will re-extract from API.

    Returns the DLQ S3 key.
    """
    s3_key = f"dlq/{api_league_id}/{season_year}/{ds}/error.json"
    payload = {
        "api_league_id": api_league_id,
        "season_year": season_year,
        "league_name": league_name,
        "ds": ds,
        "failure_stage": failure_stage,
        "source_s3_key": source_s3_key,
        "error": str(error),
        "traceback": traceback.format_exc(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    # Retry state for the hourly replay (etl/src/dlq.py, D147).
    from etl.src.dlq import new_state
    payload.update(new_state())
    payload["last_error"] = str(error)[:500]

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(payload, default=str),
        ContentType="application/json",
    )

    logger.warning("DLQ entry written: s3://%s/%s — %s", bucket, s3_key, str(error))
    return s3_key


def list_dlq_entries(bucket: str = S3_BUCKET) -> List[Dict[str, Any]]:
    """
    List all DLQ error entries in S3.
    Returns list of dicts with league_id, season, ds, and s3_key.
    """
    s3 = boto3.client("s3")
    entries = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix="dlq/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/error.json"):
                parts = key.split("/")
                # dlq/{league_id}/{season}/{ds}/error.json (standings and
                # resolved entries live under dlq/standings/, dlq/resolved/)
                if len(parts) == 5 and parts[1].isdigit():
                    entries.append({
                        "api_league_id": int(parts[1]),
                        "season_year": int(parts[2]),
                        "ds": parts[3],
                        "error_key": key,
                        "last_modified": obj["LastModified"].isoformat(),
                    })

    logger.info("Found %d DLQ entries in s3://%s/dlq/", len(entries), bucket)
    return entries
