"""Page-ready match sections per engine run, for GET /api/match/{fixture_id}.

  raw.engine_runs    — one row per run_ds: status, fixtures written, finish time
  raw.match_analysis — one payload per (run_ds, fixture): strength, patterns,
                       goals, form, discover (engine/api/match_detail.py builders)

run_ds is the Airflow logical date ({{ ds }}) passed to consensus_slips; with an
@daily schedule the run that fires on 14 Oct carries run_ds 2026-10-13, so
finished_at is stored alongside it.

A run is written in one transaction (delete + insert + mark complete), so the
API sees either the previous version of that run or the whole new one, and
only runs marked 'complete' are ever served. Re-running a day replaces it.

Backfill from saved exports:
    python -m engine.eval.match_store backfill engine/eval/consensus_slips_2026-10-13.json
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from psycopg2.extras import execute_values

from engine.eval.db import get_conn

DDL = """
CREATE TABLE IF NOT EXISTS raw.engine_runs (
    run_ds       date        PRIMARY KEY,
    status       text        NOT NULL,          -- 'complete'
    n_fixtures   int         NOT NULL DEFAULT 0,
    generated_at timestamptz,                   -- export build time
    finished_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.match_analysis (
    run_ds      date        NOT NULL,
    fixture_id  bigint      NOT NULL,
    payload     jsonb       NOT NULL,
    written_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_ds, fixture_id)
);
-- Match details reads a fixture's most recent run (its last pre-kickoff
-- forecast once the match starts). Also in db/migrations/0002.
CREATE INDEX IF NOT EXISTS match_analysis_fixture_idx
    ON raw.match_analysis (fixture_id, run_ds DESC);
"""


def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
        from engine.eval.card_versions import DDL as CARD_DDL
        cur.execute(CARD_DDL)


def build_rows(output: dict) -> list[tuple[int, dict]]:
    """(fixture_id, page-ready sections) for every fixture in an export."""
    from engine.api.match_detail import engine_sections, index_export
    from engine.api.discover import snapshot
    index = index_export(output)
    return [(fid, {**engine_sections(f, index), "discovery": snapshot(f)})
            for fid, f in index["fixtures"].items()]


def _parse_generated(output: dict):
    raw = output.get("generated")
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M UTC").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def write_match_analysis(run_ds: str, output: dict, conn=None) -> int:
    """Replace one run's match sections and mark the run complete, atomically."""
    rows = build_rows(output)
    own = conn is None
    conn = conn or get_conn()
    try:
        with conn:                                   # one transaction
            ensure_tables(conn)
            with conn.cursor() as cur:
                from engine.eval.card_versions import publish
                publish(cur, rows, run_ds)
                cur.execute("DELETE FROM raw.match_analysis WHERE run_ds = %s", (run_ds,))
                if rows:
                    execute_values(
                        cur,
                        "INSERT INTO raw.match_analysis (run_ds, fixture_id, payload) VALUES %s",
                        [(run_ds, fid, json.dumps(sec, default=str)) for fid, sec in rows],
                    )
                cur.execute(
                    """INSERT INTO raw.engine_runs (run_ds, status, n_fixtures, generated_at, finished_at)
                       VALUES (%s, 'complete', %s, %s, now())
                       ON CONFLICT (run_ds) DO UPDATE SET
                           status = EXCLUDED.status,
                           n_fixtures = EXCLUDED.n_fixtures,
                           generated_at = EXCLUDED.generated_at,
                           finished_at = EXCLUDED.finished_at""",
                    (run_ds, len(rows), _parse_generated(output)),
                )
        return len(rows)
    finally:
        if own:
            conn.close()


def backfill(paths: list[str]) -> None:
    for path in paths:
        output = json.loads(Path(path).read_text(encoding="utf-8"))
        run_ds = output.get("run_ds")
        if not run_ds:
            raise SystemExit(f"{path}: no run_ds in export")
        n = write_match_analysis(run_ds, output)
        print(f"{path}: run_ds={run_ds} fixtures={n}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Match page sections per engine run.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backfill", help="Write saved consensus_slips_<date>.json exports.")
    b.add_argument("paths", nargs="+")
    args = parser.parse_args()
    if args.cmd == "backfill":
        backfill(args.paths)


if __name__ == "__main__":
    main()
