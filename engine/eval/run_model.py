"""
Single-model runner for the fan-out engine pipeline.

Runs ONE prediction model, normalises its picks with the shared
`*_signals()` helpers (same code the in-process consensus uses), and writes
them to `raw.model_picks` plus a `raw.model_runs` readiness marker. Bayesian
also writes its complete per-fixture belief audit to `raw.model_analysis` so
the fan-in report does not have to reconstruct analysis from filtered picks.
Each model is an independent Airflow task; the quorum sensor gates consensus
on the markers.

Usage:
    python -m engine.eval.run_model --model dixon_coles --run-ds 2026-07-03 --days 7
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from engine.eval import picks_store as ps
from engine.eval.db import get_conn
from engine.eval.consensus_slips import (
    poisson_signals, mismatch_signals, bayesian_signals, dixon_signals,
    annotate_bayesian_analysis,
)


def _home_away_map(fixture_ids) -> dict:
    ids = [int(f) for f in fixture_ids]
    if not ids:
        return {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT fixture_id, home_team_name, away_team_name
                   FROM raw.fact_fixtures WHERE fixture_id = ANY(%s)""",
                (ids,),
            )
            return {int(r[0]): (r[1], r[2]) for r in cur.fetchall()}
    finally:
        conn.close()


def _run_poisson(days: int) -> list[dict]:
    from engine.eval.predict_upcoming import predict_upcoming
    return poisson_signals(predict_upcoming(days=days))


def _run_mismatch(days: int) -> list[dict]:
    from engine.eval.predict_mismatch import predict_mismatch
    picks, _strength = predict_mismatch(days=days)
    return mismatch_signals(picks)


def _run_bayesian(days: int, include_analysis: bool = False):
    from engine.eval.bayesian_streaks import get_fixture_beliefs, _get_conn
    conn = _get_conn()
    try:
        beliefs = get_fixture_beliefs(
            conn, days=days, include_below_threshold=include_analysis
        )
    finally:
        conn.close()
    home_away = _home_away_map(beliefs.keys())
    if not include_analysis:
        return bayesian_signals(beliefs, home_away)
    analysis = annotate_bayesian_analysis(beliefs, home_away)
    return bayesian_signals(analysis, home_away), analysis


def _run_dixon(days: int) -> list[dict]:
    from engine.eval.predict_dixon_coles import predict_dixon_coles
    return dixon_signals(predict_dixon_coles(days=days))


RUNNERS = {
    "poisson": _run_poisson,
    "mismatch": _run_mismatch,
    "bayesian": _run_bayesian,
    "dixon_coles": _run_dixon,
}


def run_and_store(model: str, run_ds: str, days: int) -> int:
    """Run one model and persist its picks + readiness marker. Returns pick count."""
    if model == "bayesian":
        signals, analysis = _run_bayesian(days, include_analysis=True)
        ps.write_model_analysis(run_ds, model, analysis)
    else:
        signals = RUNNERS[model](days)
    n = ps.write_model_picks(run_ds, model, signals)
    ps.write_model_run(run_ds, model, "ok" if n else "empty", n)
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description="Run one engine model and persist its picks")
    ap.add_argument("--model", required=True, choices=list(RUNNERS))
    ap.add_argument("--run-ds", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    help="prediction run date (YYYY-MM-DD)")
    ap.add_argument("--days", type=int, default=7, help="days ahead to scan")
    args = ap.parse_args()

    try:
        n = run_and_store(args.model, args.run_ds, args.days)
        print(f"{args.model}: wrote {n} picks for run_ds={args.run_ds}")
    except Exception:
        # Mark the run errored so the quorum sensor sees it finished (not counted
        # toward quorum), then fail the task so Airflow records it.
        ps.write_model_run(args.run_ds, args.model, "error", 0)
        raise


if __name__ == "__main__":
    main()
