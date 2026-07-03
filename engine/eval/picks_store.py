"""
Durable store for the fan-out engine pipeline.

Three tables couple the model tasks, the quorum sensor, and consensus without
XCom or in-process calls (see docs/engine_orchestration.md):

  raw.model_picks    — one row per (run_ds, model, fixture, unified_market)
  raw.model_runs     — readiness marker each model writes when it finishes
  raw.consensus_pool — consensus output, run_ds-keyed, for queryable history

All writes are run_ds-keyed upserts, so re-running a day is idempotent.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from psycopg2.extras import Json, execute_values

from engine.eval.db import get_conn

MODELS = ("poisson", "mismatch", "bayesian", "dixon_coles")


# ── DDL ──────────────────────────────────────────────────────────────────────

_DDL = """
-- Surrogate PK (not a natural key): one model can emit several signals for the
-- same (fixture, unified_market) and consensus keeps them all. Idempotency comes
-- from delete-by-(run_ds, model) before each insert, not a unique constraint.
CREATE TABLE IF NOT EXISTS raw.model_picks (
    id              bigserial   PRIMARY KEY,
    run_ds          date        NOT NULL,
    model           text        NOT NULL,
    fixture_id      bigint      NOT NULL,
    unified_market  text        NOT NULL,
    probability     numeric,
    raw_probability numeric,
    detail          jsonb,
    written_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS model_picks_run_idx ON raw.model_picks (run_ds, model);

CREATE TABLE IF NOT EXISTS raw.model_runs (
    run_ds      date        NOT NULL,
    model       text        NOT NULL,
    status      text        NOT NULL,          -- 'ok' | 'empty' | 'error'
    n_picks     int         NOT NULL DEFAULT 0,
    finished_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_ds, model)
);

CREATE TABLE IF NOT EXISTS raw.consensus_pool (
    run_ds              date        NOT NULL,
    fixture_id          bigint      NOT NULL,
    unified_market      text        NOT NULL,
    sportybet_market    text,
    sportybet_selection text,
    line                numeric,
    n_models            int,
    models              text[],
    avg_confidence      numeric,
    max_confidence      numeric,
    consensus_score     numeric,
    est_odds            numeric,
    real_odds           numeric,
    odds                numeric,
    ev                  numeric,
    has_odds            boolean,
    market_tier         text,
    strength_rank       jsonb,     -- full strength profile (home/away splits)
    league              text,
    country             text,
    kickoff             timestamptz,
    generated_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_ds, fixture_id, unified_market)
);
"""


def ensure_tables(conn=None) -> None:
    own = conn is None
    conn = conn or get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()
    finally:
        if own:
            conn.close()


# ── model picks + readiness marker ───────────────────────────────────────────

def write_model_picks(run_ds: str, model: str, picks: list[dict],
                      conn=None) -> int:
    """Upsert a model's picks for run_ds. `picks` items:
    {fixture_id, unified_market, probability, raw_probability, detail}.
    Replaces any prior rows for (run_ds, model) so re-runs are clean.
    """
    own = conn is None
    conn = conn or get_conn()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM raw.model_picks WHERE run_ds = %s AND model = %s",
                (run_ds, model),
            )
            rows = [
                (run_ds, model, int(p["fixture_id"]), p["unified_market"],
                 p.get("probability"), p.get("raw_probability"),
                 json.dumps(p.get("detail") or {}, default=str))
                for p in picks
            ]
            if rows:
                execute_values(
                    cur,
                    """INSERT INTO raw.model_picks
                       (run_ds, model, fixture_id, unified_market,
                        probability, raw_probability, detail)
                       VALUES %s""",
                    rows,
                )
        conn.commit()
        return len(rows)
    finally:
        if own:
            conn.close()


def write_model_run(run_ds: str, model: str, status: str, n_picks: int,
                    conn=None) -> None:
    own = conn is None
    conn = conn or get_conn()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO raw.model_runs (run_ds, model, status, n_picks, finished_at)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (run_ds, model) DO UPDATE SET
                       status = EXCLUDED.status,
                       n_picks = EXCLUDED.n_picks,
                       finished_at = EXCLUDED.finished_at""",
                (run_ds, model, status, n_picks, datetime.now(timezone.utc)),
            )
        conn.commit()
    finally:
        if own:
            conn.close()


def ready_models(run_ds: str, conn=None) -> list[str]:
    """Models that finished successfully for run_ds (drives the quorum sensor)."""
    own = conn is None
    conn = conn or get_conn()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT model FROM raw.model_runs WHERE run_ds = %s AND status = 'ok'",
                (run_ds,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def quorum_state(run_ds: str, conn=None) -> dict:
    """Snapshot for the consensus gate:
    {ready: [models ok], n_finished: total markers, first_finished: datetime|None}.
    """
    own = conn is None
    conn = conn or get_conn()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT
                       array_remove(array_agg(model) FILTER (WHERE status='ok'), NULL),
                       count(*),
                       min(finished_at)
                   FROM raw.model_runs WHERE run_ds = %s""",
                (run_ds,),
            )
            ready, n_finished, first = cur.fetchone()
            return {"ready": list(ready or []), "n_finished": int(n_finished or 0),
                    "first_finished": first}
    finally:
        if own:
            conn.close()


def read_model_picks(run_ds: str, conn=None) -> list[dict]:
    """All model picks for run_ds as flat dicts (consensus groups them)."""
    own = conn is None
    conn = conn or get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT model, fixture_id, unified_market, probability,
                          raw_probability, detail
                   FROM raw.model_picks WHERE run_ds = %s""",
                (run_ds,),
            )
            return [
                {"model": m, "fixture_id": int(f), "unified_market": u,
                 "probability": float(p) if p is not None else None,
                 "raw_probability": float(rp) if rp is not None else None,
                 "detail": d or {}}
                for m, f, u, p, rp, d in cur.fetchall()
            ]
    finally:
        if own:
            conn.close()


# ── consensus pool ───────────────────────────────────────────────────────────

def write_consensus_pool(run_ds: str, ranked: list[dict], conn=None) -> int:
    """Upsert the consensus pool for run_ds (replaces prior rows)."""
    own = conn is None
    conn = conn or get_conn()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM raw.consensus_pool WHERE run_ds = %s", (run_ds,))
            rows = [
                (run_ds, int(p["fixture_id"]), p["unified_market"],
                 p.get("sportybet_market"), p.get("sportybet_selection"), p.get("line"),
                 p.get("n_models"), p.get("models"),
                 p.get("avg_confidence"), p.get("max_confidence"),
                 p.get("consensus_score"), p.get("est_odds"), p.get("real_odds"),
                 p.get("odds"), p.get("ev"), p.get("has_odds"),
                 p.get("market_tier"), Json(p.get("strength_rank")),
                 p.get("league"), p.get("country"), p.get("kickoff"))
                for p in ranked
            ]
            if rows:
                execute_values(
                    cur,
                    """INSERT INTO raw.consensus_pool
                       (run_ds, fixture_id, unified_market, sportybet_market,
                        sportybet_selection, line, n_models, models,
                        avg_confidence, max_confidence, consensus_score, est_odds,
                        real_odds, odds, ev, has_odds, market_tier, strength_rank,
                        league, country, kickoff)
                       VALUES %s""",
                    rows,
                )
        conn.commit()
        return len(rows)
    finally:
        if own:
            conn.close()
