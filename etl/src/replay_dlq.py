"""
DLQ replay orchestration for failed fixture extractions.

This module coordinates API re-fetch, S3 landing, raw table load, detector
movement records, and DLQ cleanup. Low-level S3 operations remain in
etl.src.s3_landing.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import psycopg2

from etl.src import dlq

from etl.src.config import EXTRACT_FIXTURES_LOG
from etl.src.data_detector import (
    fixture_kickoff_watermark,
    record_data_movement_from_context,
    record_table_snapshot_from_context,
)
from etl.src.extract_fixtures import (
    FIXTURES_ENDPOINT,
    extract_fixtures_field,
    fetch_fixtures,
)
from etl.src.logger import get_logger
from etl.src.s3_landing import S3_BUCKET, load_fixtures_from_s3, write_fixtures_to_s3

logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)


def replay_from_dlq(
    entry: Dict[str, Any],
    conn: psycopg2.extensions.connection,
    context: Optional[Dict[str, Any]] = None,
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
    error_key = entry.get("error_key")

    logger.info("Replaying DLQ entry: league %s, season %s", api_league_id, season_year)
    raw = fetch_fixtures(FIXTURES_ENDPOINT, params={"league": api_league_id, "season": season_year})
    fixtures = extract_fixtures_field(raw)

    if not fixtures:
        logger.warning("No fixtures from API for league %s season %s. Replay failed.", api_league_id, season_year)
        if context is not None:
            record_data_movement_from_context(
                context,
                movement_type="dlq_replay_to_s3",
                source_system="api_sports",
                source_name=f"fixtures:{api_league_id}:{season_year}",
                row_count=0,
                inserted_count=0,
                failed_count=1,
                status="failed",
                details={
                    "api_league_id": api_league_id,
                    "season_year": season_year,
                    "dlq_error_key": error_key,
                    "reason": "no fixtures returned from API",
                },
            )
        return False

    landing_key = write_fixtures_to_s3(fixtures, api_league_id, season_year, ds, bucket)
    watermark_column, watermark_min, watermark_max = fixture_kickoff_watermark(fixtures)
    if context is not None:
        record_data_movement_from_context(
            context,
            movement_type="dlq_replay_to_s3",
            source_system="api_sports",
            source_name=f"fixtures:{api_league_id}:{season_year}",
            source_s3_key=landing_key,
            row_count=len(fixtures),
            inserted_count=len(fixtures),
            failed_count=0,
            watermark_column=watermark_column,
            watermark_min=watermark_min,
            watermark_max=watermark_max,
            status="success",
            details={
                "api_league_id": api_league_id,
                "season_year": season_year,
                "dlq_error_key": error_key,
            },
        )
        record_table_snapshot_from_context(
            context,
            table_schema="raw",
            table_name="raw_fixtures",
            details={
                "movement_type": "s3_to_raw",
                "source_s3_key": landing_key,
                "api_league_id": api_league_id,
                "season_year": season_year,
                "replay": True,
            },
        )

    count = load_fixtures_from_s3(landing_key, conn, bucket, source="replay")
    if count == 0:
        if context is not None:
            record_data_movement_from_context(
                context,
                movement_type="s3_to_raw",
                source_system="s3",
                source_s3_key=landing_key,
                target_schema="raw",
                target_table="raw_fixtures",
                row_count=0,
                inserted_count=0,
                failed_count=1,
                watermark_column=watermark_column,
                watermark_min=watermark_min,
                watermark_max=watermark_max,
                status="failed",
                details={
                    "api_league_id": api_league_id,
                    "season_year": season_year,
                    "dlq_error_key": error_key,
                    "reason": "zero rows loaded during replay",
                },
            )
        return False

    if context is not None:
        record_data_movement_from_context(
            context,
            movement_type="s3_to_raw",
            source_system="s3",
            source_s3_key=landing_key,
            target_schema="raw",
            target_table="raw_fixtures",
            row_count=count,
            inserted_count=count,
            failed_count=0,
            watermark_column=watermark_column,
            watermark_min=watermark_min,
            watermark_max=watermark_max,
            status="success",
            details={
                "api_league_id": api_league_id,
                "season_year": season_year,
                "dlq_error_key": error_key,
                "replay": True,
            },
        )

    cleanup_dlq_entry(api_league_id, season_year, ds, bucket)
    logger.info("Replayed %d fixtures for league %s season %s. DLQ cleaned up.", count, api_league_id, season_year)
    return True


def cleanup_dlq_entry(
    api_league_id: int,
    season_year: int,
    ds: str,
    bucket: str = S3_BUCKET,
) -> None:
    """Move a replayed entry's error.json to dlq/resolved/fixtures/ (audit trail)."""
    key = f"dlq/{api_league_id}/{season_year}/{ds}/error.json"
    try:
        entry = dlq.read_json(key, bucket)
        if entry is None:
            return
        dlq.resolve_entry(key, entry, "replay", bucket, group="fixtures")
    except Exception:
        logger.warning("Failed to clean up DLQ key s3://%s/%s", bucket, key, exc_info=True)


# ---------------------------------------------------------------------------
# Hourly retry policy for fixture entries (D147)
# ---------------------------------------------------------------------------

def due_fixture_entries(entries: List[Dict[str, Any]], bucket: str = S3_BUCKET
                        ) -> List[Dict[str, Any]]:
    """Listed entries whose retry is due (open and past next_retry_at), plus
    dead ones on their daily recheck (no nightly run re-extracts a league).
    Entries written before D147 have no retry state and are due now."""
    due = []
    for entry in entries:
        body = dlq.read_json(entry["error_key"], bucket) or {}
        if dlq.is_due(body) or dlq.dead_recheck_due(body):
            due.append({**entry, "state": dlq.with_state(body)})
    return due


def fixture_attempt_failed(entry: Dict[str, Any], error: str, bucket: str = S3_BUCKET) -> bool:
    """Count a failed replay; after MAX_ATTEMPTS the entry is dead and alerted.
    Returns True when the entry just became dead."""
    key = entry["error_key"]
    body = entry.get("state") or dlq.read_json(key, bucket) or {}
    body, became_dead = dlq.record_failed_attempt(body, error)
    dlq.write_json(key, body, bucket)
    if became_dead:
        dlq.alert_dead("fixtures", key, body,
                       f"Fixture extraction for league {entry['api_league_id']} "
                       f"season {entry['season_year']} (failed on {entry['ds']})",
                       clears="Rechecked once a day: it resolves itself when the extraction succeeds. "
                              "To force it, re-run bootstrap for that league season.")
    return became_dead
