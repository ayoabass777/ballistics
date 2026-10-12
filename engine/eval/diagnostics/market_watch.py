"""Market watch: track calibration of chosen markets across calibration refits.

Reads engine/eval/calibration_report.json (written by calibration_wf), appends
one row per (market, source) to engine/eval/watch/<slug>.csv, and flags drift
against the first row with at least MIN_N picks (baseline). Idempotent: a report already logged (same
`generated`/mtime stamp) is skipped.

Watched now (D120): "Match Over 1.5" — all sources, and Dixon–Coles alone
(Poisson retired 2026-10-11, so DC now carries most Over 1.5 picks by itself).

Usage:
    python -m engine.eval.diagnostics.market_watch [--report PATH]
Exit code 0 always (monitoring, not a gate); prints FLAG lines on drift.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path

EVAL = Path(__file__).resolve().parents[1]
WATCH_DIR = EVAL / "watch"

WATCHES = [
    # (market, source label, filter on the `models` field)
    ("Match Over 1.5", "all", lambda m: True),
    ("Match Over 1.5", "dixon_coles_only", lambda m: m == "dixon_coles"),
]
# Drift flags (percentage points / Brier units)
MAX_ABS_OVERCONF = 0.03      # |predicted - hit| above 3 pts
MAX_BRIER_RISE = 0.010       # Brier worse than baseline by more than 0.010
MIN_N = 100                  # below this, report but don't flag

FIELDS = ["logged_at", "report_stamp", "market", "source", "n", "avg_predicted",
          "hit_rate", "overconfidence", "brier", "flag"]


def _stats(rows):
    n = len(rows)
    if not n:
        return None
    p = sum(r["predicted_prob"] for r in rows) / n
    h = sum(1.0 if r["actual"] else 0.0 for r in rows) / n
    b = sum((r["predicted_prob"] - (1.0 if r["actual"] else 0.0)) ** 2 for r in rows) / n
    return {"n": n, "avg_predicted": round(p, 4), "hit_rate": round(h, 4),
            "overconfidence": round(p - h, 4), "brier": round(b, 4)}


def _slug(market: str) -> str:
    return market.lower().replace(" ", "_").replace(".", "_")


def run(report_path: Path, stamp: str | None = None) -> list[str]:
    rep = json.loads(report_path.read_text())
    stamp = stamp or rep.get("generated") or datetime.fromtimestamp(
        os.path.getmtime(report_path), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    preds = rep.get("predictions") or []
    WATCH_DIR.mkdir(parents=True, exist_ok=True)
    flags = []
    for market, source, keep in WATCHES:
        path = WATCH_DIR / f"{_slug(market)}.csv"
        existing = list(csv.DictReader(path.open())) if path.exists() else []
        if any(r["report_stamp"] == stamp and r["source"] == source for r in existing):
            continue
        s = _stats([p for p in preds if p.get("market") == market and keep(p.get("models", ""))])
        if s is None:
            continue
        base = next((r for r in existing if r["source"] == source and int(r["n"]) >= MIN_N), None)
        why = []
        if s["n"] >= MIN_N:
            if abs(s["overconfidence"]) > MAX_ABS_OVERCONF:
                why.append(f"overconfidence {s['overconfidence']*100:+.1f} pts")
            if base and s["brier"] - float(base["brier"]) > MAX_BRIER_RISE:
                why.append(f"Brier {float(base['brier']):.4f} -> {s['brier']:.4f}")
        row = {"logged_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "report_stamp": stamp, "market": market, "source": source, **s,
               "flag": "; ".join(why)}
        new = not path.exists()
        with path.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            if new:
                w.writeheader()
            w.writerow(row)
        line = (f"{market} [{source}] n={s['n']} pred={s['avg_predicted']:.3f} "
                f"hit={s['hit_rate']:.3f} overconf={s['overconfidence']*100:+.1f}pts brier={s['brier']:.4f}")
        print(("FLAG " if why else "ok   ") + line + (f"  <- {row['flag']}" if why else ""))
        if why:
            flags.append(line)
    return flags


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default=str(EVAL / "calibration_report.json"))
    ap.add_argument("--stamp", default=None, help="override the report stamp (backfill)")
    a = ap.parse_args()
    run(Path(a.report), a.stamp)
