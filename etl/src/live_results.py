"""Live results feed raw.raw_fixtures; the nightly sweep checks every one.

Promotion (live poller): when a tracked fixture leaves the live list, the
poller fetches it by id (fixtures?ids=), the same call the nightly update
makes. A final result (FT, AET, PEN with both full-time goals) is flattened
by the nightly extractor, landed in S3 under incremental/live/, and upserted
through the nightly loader (apply_updates_to_db: same row checks, same
quarantine). raw.live_fixture_state records what was promoted.

Sweep (fixture_update_dag, daily): every promoted fixture not yet swept is
re-fetched by id with the fixtures that still lack a result (20 per call).
Only a result the provider now answers differently is rewritten; one it
agrees with is just stamped swept_at, so raw_fixtures.updated_at and dbt's
incremental staging are untouched. Changed answers are alerted once per run.

Guard: a final status without both full-time goals is never written; that
row is what crashed Elo on 2026-11-01 (is_played with a NULL score).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from etl.src.live_scores import FINAL
from etl.src.logger import get_logger

logger = get_logger(__name__)

SCORE_FIELDS = ("fixture_status", "home_team_fulltime_goal", "away_team_fulltime_goal")

MARK_PROMOTED_SQL = """
    UPDATE raw.live_fixture_state
    SET promoted_at = %(now)s, promoted_status = %(fixture_status)s,
        promoted_home_goals = %(home_team_fulltime_goal)s,
        promoted_away_goals = %(away_team_fulltime_goal)s,
        swept_at = NULL, sweep_changed = NULL
    WHERE api_fixture_id = %(api_fixture_id)s
"""

# Promoted, not yet checked, and at least an hour old so the provider has
# settled (the sweep runs at 00:00 UTC; a 23:55 final waits a night).
UNSWEPT_SQL = """
    SELECT fixture_id, api_fixture_id FROM raw.live_fixture_state
    WHERE promoted_at IS NOT NULL AND swept_at IS NULL
      AND promoted_at < now() - interval '1 hour'
"""

PROMOTED_SQL = """
    SELECT api_fixture_id, promoted_status, promoted_home_goals, promoted_away_goals
    FROM raw.live_fixture_state
    WHERE api_fixture_id = ANY(%(ids)s) AND promoted_at IS NOT NULL AND swept_at IS NULL
"""

MARK_SWEPT_SQL = """
    UPDATE raw.live_fixture_state SET swept_at = %(now)s, sweep_changed = %(changed)s
    WHERE api_fixture_id = %(api_fixture_id)s
"""


def is_final_with_score(row: Dict[str, Any]) -> bool:
    return (row.get("fixture_status") in FINAL
            and row.get("home_team_fulltime_goal") is not None
            and row.get("away_team_fulltime_goal") is not None
            and not row.get("_dlq_reason"))


def promotable(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Provider fixture items → raw_fixtures rows safe to write now."""
    from etl.src.extract_fixtures import extract_fixtures_field
    from etl.src.fixtures_dlq import fixture_row_problem

    rows = extract_fixtures_field(list(items))
    return [r for r in rows if is_final_with_score(r) and fixture_row_problem(r) is None]


def live_s3_key(now: datetime) -> str:
    # One object per promotion, never overwritten.
    return f"incremental/live/{now:%Y-%m-%d}/{now:%H%M%S%f}.json"


def write_to_s3(rows: List[Dict[str, Any]], now: datetime) -> str:
    import boto3
    from etl.src.s3_landing import S3_BUCKET

    key = live_s3_key(now)
    boto3.client("s3").put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(rows, default=str),
                                  ContentType="application/json")
    return key


def record_promotion(rows: List[Dict[str, Any]], key: str, applied: int) -> None:
    from etl.src.data_detector import fixture_kickoff_watermark
    from etl.src.data_detector.data_movements import record_data_movement

    column, lo, hi = fixture_kickoff_watermark(rows)
    record_data_movement(
        movement_type="s3_to_raw", source_system="s3", source_name="fixtures:live_final",
        source_s3_key=key, target_schema="raw", target_table="raw_fixtures", dag_id="live_poller",
        task_id="promote_results", row_count=len(rows), updated_count=applied, status="success",
        watermark_column=column, watermark_min=lo, watermark_max=hi,
        details={"api_fixture_ids": [r["api_fixture_id"] for r in rows]})


class Promoter:
    """Writes live final results into raw.raw_fixtures. IO is injected so the
    poller tests can run without S3, Postgres or the loader."""

    def __init__(self, connect: Callable[[], Any],
                 write: Callable[[List[Dict[str, Any]], datetime], str] = write_to_s3,
                 apply: Optional[Callable[[List[Dict[str, Any]]], int]] = None,
                 record: Callable[[List[Dict[str, Any]], str, int], None] = record_promotion):
        if apply is None:
            from etl.src.update_fixtures_main import apply_updates_to_db as apply
        self.connect, self.write, self.apply, self.record = connect, write, apply, record

    def __call__(self, items: List[Dict[str, Any]], now: datetime) -> int:
        rows = promotable(items)
        if not rows:
            return 0
        # Land first: a load from S3 can be replayed; a load that skipped it can't.
        key = self.write(rows, now)
        applied = self.apply(rows)
        with self.connect() as conn, conn.cursor() as cur:
            for row in rows:
                cur.execute(MARK_PROMOTED_SQL, {**{k: row.get(k) for k in SCORE_FIELDS},
                                                "api_fixture_id": row["api_fixture_id"], "now": now})
            conn.commit()
        try:
            self.record(rows, key, applied)
        except Exception as exc:  # the audit row must not undo a good load
            logger.warning("Could not record live promotion (%s): %s", type(exc).__name__, exc)
        logger.info("Promoted %d live final results to raw_fixtures (s3://%s)", len(rows), key)
        return applied


# ---------------------------------------------------------------------------
# Nightly sweep
# ---------------------------------------------------------------------------

def unswept_fixture_ids(cur) -> List[Tuple[int, str]]:
    """(fixture_id, api_fixture_id) for promoted fixtures the sweep must check,
    in the shape update_fixtures.to_update_fixture_ids returns."""
    cur.execute(UNSWEPT_SQL)
    return [(fid, str(api)) for fid, api in cur.fetchall()]


def compare(promoted: Dict[str, Any], row: Dict[str, Any]) -> Optional[str]:
    """None when the sweep agrees with what was promoted, else what changed."""
    was = (promoted["promoted_status"], promoted["promoted_home_goals"], promoted["promoted_away_goals"])
    now = tuple(row.get(k) for k in SCORE_FIELDS)
    if now == was:
        return None
    return f"{was[0]} {was[1]}-{was[2]} → {now[0]} {now[1]}-{now[2]}"


def sweep(cur, updates: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Tuple[int, bool]], List[str]]:
    """Split the nightly payload for the sweep.

    Returns (rows to apply, (api_fixture_id, changed) to stamp after the load,
    one line per changed result). A promoted fixture the provider still agrees
    with is not rewritten: it is only stamped, so its updated_at (and dbt's
    incremental staging) is left alone. Changed ones and every fixture that
    was never promoted are applied as before. A promoted fixture missing from
    the payload stays unswept and is checked again the next night.
    """
    by_api = {int(r["api_fixture_id"]): r for r in updates
              if r.get("api_fixture_id") is not None and not r.get("_dlq_reason")}
    if not by_api:
        return updates, [], []
    cur.execute(PROMOTED_SQL, {"ids": list(by_api)})
    unchanged, stamps, lines = set(), [], []
    for api_id, status, home, away in cur.fetchall():
        row = by_api[int(api_id)]
        diff = compare({"promoted_status": status, "promoted_home_goals": home,
                        "promoted_away_goals": away}, row)
        stamps.append((int(api_id), diff is not None))
        if diff:
            lines.append(f"{row.get('home_team_name')} v {row.get('away_team_name')} (api {api_id}): {diff}")
        else:
            unchanged.add(int(api_id))
    apply = [r for r in updates if r.get("api_fixture_id") is None
             or int(r["api_fixture_id"]) not in unchanged]
    return apply, stamps, lines


def mark_swept(cur, stamps: List[Tuple[int, bool]], now: Optional[datetime] = None) -> None:
    now = now or datetime.now(timezone.utc)
    for api_id, changed in stamps:
        cur.execute(MARK_SWEPT_SQL, {"now": now, "changed": changed, "api_fixture_id": api_id})
