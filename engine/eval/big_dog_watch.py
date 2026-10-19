"""Shadow log for the Big dog Dixon-Coles cross-check (D136, 2026-10-18).

Each consensus run upserts the published Big dog shortlist into
raw.big_dog_watch with the Dixon-Coles win chance for the Elo favourite
(dc_win_prob) and whether it clears BIG_DOG_DC_CONFIRM (dc_confirms). Nothing
user-facing reads it. The daily data flow check settles rows against
raw.engine_head_to_head and reports confirmed vs not-confirmed win rates
(etl/src/data_detector/model_watch.py), so the backtest finding (Jun-Oct 2026:
confirmed 76% vs dropped 69%, too few to act on) can be re-tested on live picks.

Rows keep the latest pre-kickoff values; a run after kickoff never overwrites.
"""
from __future__ import annotations

from engine.eval.db import get_conn

DDL = """
CREATE TABLE IF NOT EXISTS raw.big_dog_watch (
    fixture_id    bigint      PRIMARY KEY,
    first_run_ds  date        NOT NULL,
    last_run_ds   date        NOT NULL,
    kickoff       timestamptz NOT NULL,
    league        text,
    country       text,
    favourite     text        NOT NULL,
    underdog      text,
    fav_is_home   boolean     NOT NULL,
    elo_gap       numeric     NOT NULL,
    dc_win_prob   numeric,                 -- NULL: no Dixon-Coles goal rates for the fixture
    dc_confirms   boolean,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
"""

UPSERT = """
INSERT INTO raw.big_dog_watch AS w
    (fixture_id, first_run_ds, last_run_ds, kickoff, league, country, favourite,
     underdog, fav_is_home, elo_gap, dc_win_prob, dc_confirms, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (fixture_id) DO UPDATE SET
    last_run_ds = EXCLUDED.last_run_ds, kickoff = EXCLUDED.kickoff,
    favourite = EXCLUDED.favourite, underdog = EXCLUDED.underdog,
    fav_is_home = EXCLUDED.fav_is_home, elo_gap = EXCLUDED.elo_gap,
    dc_win_prob = EXCLUDED.dc_win_prob, dc_confirms = EXCLUDED.dc_confirms,
    updated_at = now()
WHERE w.kickoff > now()
"""


def watch_rows(run_ds: str, shortlist: list[dict]) -> list[tuple]:
    rows = []
    for p in shortlist or []:
        if not p.get("kickoff") or not p.get("favourite"):
            continue
        rows.append((int(p["fixture_id"]), run_ds, run_ds, p["kickoff"], p.get("league"),
                     p.get("country"), p["favourite"], p.get("underdog"),
                     p["favourite"] == p.get("home"), p["elo_gap"],
                     p.get("dc_win_prob"), p.get("dc_confirms")))
    return rows


def write_big_dog_watch(run_ds: str, shortlist: list[dict], conn=None) -> int:
    """Upsert pre-kickoff Big dogs; returns rows sent."""
    rows = watch_rows(run_ds, shortlist)
    own = conn is None
    conn = conn or get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
            for r in rows:
                cur.execute(UPSERT, r)
        conn.commit()
    finally:
        if own:
            conn.close()
    return len(rows)
