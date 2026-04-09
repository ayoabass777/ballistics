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
from typing import Any, Dict, List, Tuple

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
) -> int:
    fixtures = read_fixtures_from_s3(s3_key, bucket)
    if not fixtures:
        logger.info("Empty file at s3://%s/%s. Skipping.", bucket, s3_key)
        return 0

    parts = s3_key.split("/")
    api_league_id = int(parts[1])
    season_year = int(parts[2])

    cur = conn.cursor()
    try:
        cur.execute(
            f"DELETE FROM {RAW_TABLE} WHERE api_league_id = %s AND season = %s",
            (api_league_id, season_year),
        )
        buffer, count = _records_to_buffer(fixtures)
        cur.copy_expert(COPY_SQL, buffer)
        logger.info("Loaded %d fixtures from s3://%s/%s into %s", count, bucket, s3_key, RAW_TABLE)
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Dead Letter Queue (DLQ)
# ---------------------------------------------------------------------------

def write_to_dlq(
    api_league_id: int,
    season_year: int,
    ds: str,
    error: Exception,
    league_name: str = "",
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
        "error": str(error),
        "traceback": traceback.format_exc(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

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
                # dlq/{league_id}/{season}/{ds}/error.json
                if len(parts) == 5:
                    entries.append({
                        "api_league_id": int(parts[1]),
                        "season_year": int(parts[2]),
                        "ds": parts[3],
                        "error_key": key,
                        "last_modified": obj["LastModified"].isoformat(),
                    })

    logger.info("Found %d DLQ entries in s3://%s/dlq/", len(entries), bucket)
    return entries


def replay_from_dlq(
    entry: Dict[str, Any],
    conn: psycopg2.extensions.connection,
    bucket: str = S3_BUCKET,
) -> bool:
    """
    Replay a single DLQ entry by re-extracting from the API and loading to Postgres.
    On success, writes to landing zone, cleans up DLQ entry, returns True.
    On failure, returns False (entry stays in DLQ for next attempt).
    """
    api_league_id = entry["api_league_id"]
    season_year = entry["season_year"]
    ds = entry["ds"]

    from etl.src.extract_fixtures import (
        FIXTURES_ENDPOINT,
        extract_fixtures_field,
        fetch_fixtures,
    )

    logger.info("Replaying DLQ entry: league %s, season %s", api_league_id, season_year)
    raw = fetch_fixtures(FIXTURES_ENDPOINT, params={"league": api_league_id, "season": season_year})
    fixtures = extract_fixtures_field(raw)

    if not fixtures:
        logger.warning("No fixtures from API for league %s season %s. Replay failed.", api_league_id, season_year)
        return False

    # Write to landing zone
    landing_key = write_fixtures_to_s3(fixtures, api_league_id, season_year, ds, bucket)

    # Load into Postgres
    count = load_fixtures_from_s3(landing_key, conn, bucket)
    if count == 0:
        return False

    # Clean up DLQ
    _cleanup_dlq_entry(api_league_id, season_year, ds, bucket)
    logger.info("Replayed %d fixtures for league %s season %s. DLQ cleaned up.", count, api_league_id, season_year)
    return True


def _cleanup_dlq_entry(
    api_league_id: int,
    season_year: int,
    ds: str,
    bucket: str = S3_BUCKET,
) -> None:
    """Remove DLQ error.json for a given entry."""
    s3 = boto3.client("s3")
    key = f"dlq/{api_league_id}/{season_year}/{ds}/error.json"
    try:
        s3.delete_object(Bucket=bucket, Key=key)
    except Exception:
        pass
