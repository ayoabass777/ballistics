"""
CLI entry for updating fixtures and writing changes directly to raw.raw_fixtures
via batched upserts (execute_values). Defaults to updating by specific fixture IDs;
batch mode groups by league-season date ranges.
"""
import argparse
import os
from typing import Any, Dict, List

import psycopg2.extras
from psycopg2.extensions import AsIs

from etl.src.logger import get_logger
from etl.src.config import UPDATE_FIXTURES_LOG
from etl.src.extract_metadata import get_db_connection
from etl.src.schema_checks import ensure_raw_fixtures_bootstrap_ready
from etl.src.update_fixtures import (
    batch_update_fixtures,
    to_update_fixture_ids,
    update_by_ids,
)

logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)

RAW_TABLE = "raw.raw_fixtures"
RAW_COLUMNS = [
    "fixture_id",  # DB primary key (may be None -> DEFAULT)
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


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _dedupe_updates_by_api_fixture_id(updates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Collapse duplicate payload rows by api_fixture_id to avoid double-updating the
    same row in one INSERT ... ON CONFLICT statement.
    """
    deduped: List[Dict[str, Any]] = []
    index_by_api_id: Dict[str, int] = {}
    duplicate_count = 0

    for record in updates:
        api_fixture_id = record.get("api_fixture_id")
        if api_fixture_id is None:
            deduped.append(dict(record))
            continue

        key = str(api_fixture_id)
        existing_index = index_by_api_id.get(key)
        if existing_index is None:
            index_by_api_id[key] = len(deduped)
            deduped.append(dict(record))
            continue

        duplicate_count += 1
        merged = deduped[existing_index]
        for field, value in record.items():
            if value is not None or field not in merged or merged.get(field) is None:
                merged[field] = value

    if duplicate_count:
        logger.warning(
            "Collapsed %d duplicate fixture update rows by api_fixture_id before DB apply.",
            duplicate_count,
        )

    return deduped


def _attach_existing_fixture_ids(cur, updates: List[Dict[str, Any]]) -> None:
    """
    Backfill warehouse fixture_id values for updates that only contain api_fixture_id.

    This prevents batch mode from inserting duplicate raw.raw_fixtures rows when the
    fixture already exists in the warehouse.
    """
    missing_api_ids: List[int] = []
    seen_missing: set[int] = set()

    for record in updates:
        if record.get("fixture_id") is not None:
            continue
        api_id = _safe_int(record.get("api_fixture_id"))
        if api_id is None:
            continue
        if api_id not in seen_missing:
            missing_api_ids.append(api_id)
            seen_missing.add(api_id)

    if not missing_api_ids:
        return

    cur.execute(
        f"""
        SELECT fixture_id, api_fixture_id
        FROM {RAW_TABLE}
        WHERE api_fixture_id = ANY(%s)
        """,
        (missing_api_ids,),
    )

    existing_by_api_id: Dict[int, int] = {}
    duplicate_api_ids: set[int] = set()
    for fixture_id, api_fixture_id in cur.fetchall():
        db_fixture_id = _safe_int(fixture_id)
        db_api_id = _safe_int(api_fixture_id)
        if db_fixture_id is None or db_api_id is None:
            continue
        prior = existing_by_api_id.get(db_api_id)
        if prior is not None and prior != db_fixture_id:
            duplicate_api_ids.add(db_api_id)
            continue
        existing_by_api_id[db_api_id] = db_fixture_id

    if duplicate_api_ids:
        sample = ", ".join(str(v) for v in sorted(duplicate_api_ids)[:10])
        raise RuntimeError(
            "Duplicate rows detected in raw.raw_fixtures for api_fixture_id(s): "
            f"{sample}. Clean up duplicates before applying fixture updates."
        )

    attached_count = 0
    for record in updates:
        if record.get("fixture_id") is not None:
            continue
        api_id = _safe_int(record.get("api_fixture_id"))
        if api_id is None:
            continue
        existing_fixture_id = existing_by_api_id.get(api_id)
        if existing_fixture_id is not None:
            record["fixture_id"] = existing_fixture_id
            attached_count += 1

    if attached_count:
        logger.info(
            "Backfilled %d existing fixture_id values via api_fixture_id before upsert.",
            attached_count,
        )


def _normalize_updates(updates: List[Dict[str, Any]]) -> List[List[Any]]:
    """
    Normalize updates into ordered row lists matching RAW_COLUMNS.
    Missing fields are treated as NULL, fixture_id None maps to DEFAULT.
    """
    rows: List[List[Any]] = []
    for record in updates:
        row = []
        for col in RAW_COLUMNS:
            val = record.get(col)
            if col == "fixture_id" and val is None:
                row.append(AsIs("DEFAULT"))
            else:
                row.append(None if val is None else val)
        rows.append(row)
    return rows


def apply_updates_to_db(updates: List[Dict[str, Any]], chunk_size: int = 1000) -> int:
    """
    Stream fixture updates into raw.raw_fixtures using COPY + UPDATE (and insert missing).
    Returns number of rows processed.
    """
    if not updates:
        logger.info("No updates to apply; exiting.")
        return 0

    prepared_updates = _dedupe_updates_by_api_fixture_id(updates)
    if not prepared_updates:
        logger.info("Updates payload is empty; nothing to apply.")
        return 0

    update_cols = [c for c in RAW_COLUMNS if c not in {"api_fixture_id", "fixture_id"}]
    set_pairs = ", ".join(
        f"{col} = COALESCE(EXCLUDED.{col}, {RAW_TABLE}.{col})" for col in update_cols
    )
    insert_sql = f"""
        INSERT INTO {RAW_TABLE} ({', '.join(RAW_COLUMNS)})
        VALUES %s
        ON CONFLICT (fixture_id) DO UPDATE SET
            {set_pairs},
            updated_at = NOW()
    """
    row_template = "(" + ", ".join(["%s"] * len(RAW_COLUMNS)) + ")"

    total_processed = 0
    with get_db_connection() as conn, conn.cursor() as cur:
        ensure_raw_fixtures_bootstrap_ready(cur)
        logger.info("Schema check passed: raw.raw_fixtures.fixture_id contract is valid.")
        _attach_existing_fixture_ids(cur, prepared_updates)

        rows = _normalize_updates(prepared_updates)
        logger.info("Applying %d fixture updates to %s", len(rows), RAW_TABLE)
        if not rows:
            logger.info("Updates payload is empty after preparation; nothing to apply.")
            return 0

        for start in range(0, len(rows), chunk_size):
            chunk = rows[start : start + chunk_size]
            psycopg2.extras.execute_values(
                cur,
                insert_sql,
                chunk,
                template=row_template,
                page_size=chunk_size,
            )
            conn.commit()

            total_processed += len(chunk)
            logger.info("Applied %d fixture updates (chunk %d-%d)", len(chunk), start, start + len(chunk))

    logger.info("Finished applying fixture updates; total processed: %d", total_processed)
    return total_processed


def run(mode: str, from_date: str | None, to_date: str | None, chunk_size: int) -> int:
    """
    Execute fixture updates in the requested mode and apply directly to DB.
    """
    if mode == "batch":
        logger.info(
            "Running batch_update_fixtures (from_date=%s, to_date=%s, chunk_size=%s)",
            from_date,
            to_date,
            chunk_size,
        )
        updates = batch_update_fixtures(
            output_filename=None,
            from_date=from_date,
            to_date=to_date,
            write_output=False,
        )
    else:
        logger.info("Running update_by_ids as default flow (chunk_size=%s)", chunk_size)
        ids = to_update_fixture_ids()
        updates = update_by_ids(ids)

    return apply_updates_to_db(updates, chunk_size=chunk_size)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update fixtures and apply directly to raw.raw_fixtures.")
    parser.add_argument(
        "--mode",
        choices=["ids", "batch"],
        default="ids",
        help="Update mode: 'ids' uses specific fixture IDs (default); 'batch' groups by league-season range.",
    )
    parser.add_argument(
        "--from-date",
        dest="from_date",
        type=str,
        help="ISO date (YYYY-MM-DD) to fetch fixtures from (batch mode only).",
    )
    parser.add_argument(
        "--to-date",
        dest="to_date",
        type=str,
        help="ISO date (YYYY-MM-DD) to fetch fixtures up to (batch mode only).",
    )
    parser.add_argument(
        "--chunk-size",
        dest="chunk_size",
        type=int,
        default=1000,
        help="Number of rows to COPY per chunk when applying updates.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    if not os.getenv("UPDATE_FIXTURES_LOG"):
        logger.warning("Environment variable UPDATE_FIXTURES_LOG is not set; using default path: %s", UPDATE_FIXTURES_LOG)
    args = parse_args()
    try:
        updated_count = run(args.mode, args.from_date, args.to_date, args.chunk_size)
        logger.info("update_fixtures_main finished: %s updates applied to DB.", updated_count)
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received; shutting down update fixtures.")
    except Exception as exc:
        logger.error("Error running update fixtures: %s", exc, exc_info=True)
