"""Market watch: track calibration of chosen markets across calibration refits.

Reads engine/eval/calibration_report.json (written by calibration_wf), adds
one row per (market, source) to raw.market_watch, and flags drift against the
first row with at least MIN_N picks (baseline). Idempotent: a report already
logged (same `generated`/mtime stamp) is skipped. Until 2026-11-18 the rows
went to engine/eval/watch/<market>.csv; --import-csv loads such a file once.

Watched now (D120): "Match Over 1.5" — all sources, and Dixon–Coles alone
(Poisson retired 2026-10-11, so DC now carries most Over 1.5 picks by itself).

Usage:
    python -m engine.eval.diagnostics.market_watch [--report PATH]
    python -m engine.eval.diagnostics.market_watch --import-csv PATH
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

# Rows of one market in logging order (id), oldest first.
SELECT_SQL = """
SELECT report_stamp, source, n, brier
FROM raw.market_watch
WHERE market = %s
ORDER BY id
"""
INSERT_SQL = f"""
INSERT INTO raw.market_watch ({", ".join(FIELDS)})
VALUES ({", ".join(["%s"] * len(FIELDS))})
ON CONFLICT (market, source, report_stamp) DO NOTHING
"""


def _stats(rows):
    n = len(rows)
    if not n:
        return None
    p = sum(r["predicted_prob"] for r in rows) / n
    h = sum(1.0 if r["actual"] else 0.0 for r in rows) / n
    b = sum((r["predicted_prob"] - (1.0 if r["actual"] else 0.0)) ** 2 for r in rows) / n
    return {"n": n, "avg_predicted": round(p, 4), "hit_rate": round(h, 4),
            "overconfidence": round(p - h, 4), "brier": round(b, 4)}


def new_rows(preds: list[dict], stamp: str, existing: list[dict],
             now: str | None = None) -> list[dict]:
    """Rows to log for this report. `existing` is what raw.market_watch holds
    (dicts with market, report_stamp, source, n, brier), oldest first."""
    now = now or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out = []
    for market, source, keep in WATCHES:
        mine = [r for r in existing if r["market"] == market and r["source"] == source]
        if any(r["report_stamp"] == stamp for r in mine):
            continue
        s = _stats([p for p in preds if p.get("market") == market and keep(p.get("models", ""))])
        if s is None:
            continue
        base = next((r for r in mine if int(r["n"]) >= MIN_N), None)
        why = []
        if s["n"] >= MIN_N:
            if abs(s["overconfidence"]) > MAX_ABS_OVERCONF:
                why.append(f"overconfidence {s['overconfidence']*100:+.1f} pts")
            if base and s["brier"] - float(base["brier"]) > MAX_BRIER_RISE:
                why.append(f"Brier {float(base['brier']):.4f} -> {s['brier']:.4f}")
        out.append({"logged_at": now, "report_stamp": stamp, "market": market,
                    "source": source, **s, "flag": "; ".join(why)})
    return out


def _existing(cur) -> list[dict]:
    rows = []
    for market in dict.fromkeys(m for m, _, _ in WATCHES):
        cur.execute(SELECT_SQL, (market,))
        rows += [{"market": market, "report_stamp": st, "source": src, "n": n, "brier": b}
                 for st, src, n, b in cur.fetchall()]
    return rows


def _insert(cur, rows: list[dict]) -> None:
    for r in rows:
        cur.execute(INSERT_SQL, [(r.get(f) or "") if f == "flag" else r[f] for f in FIELDS])


def _with_cursor(conn, fn):
    from engine.eval.db import get_conn
    own = conn is None
    conn = conn or get_conn()
    try:
        with conn.cursor() as cur:
            result = fn(cur)
        conn.commit()
        return result
    finally:
        if own:
            conn.close()


def run(report_path: Path, stamp: str | None = None, conn=None) -> list[str]:
    rep = json.loads(report_path.read_text())
    stamp = stamp or rep.get("generated") or datetime.fromtimestamp(
        os.path.getmtime(report_path), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    preds = rep.get("predictions") or []

    def log(cur):
        rows = new_rows(preds, stamp, _existing(cur))
        _insert(cur, rows)
        return rows

    flags = []
    for row in _with_cursor(conn, log):
        line = (f"{row['market']} [{row['source']}] n={row['n']} pred={row['avg_predicted']:.3f} "
                f"hit={row['hit_rate']:.3f} overconf={row['overconfidence']*100:+.1f}pts "
                f"brier={row['brier']:.4f}")
        print(("FLAG " if row["flag"] else "ok   ") + line
              + (f"  <- {row['flag']}" if row["flag"] else ""))
        if row["flag"]:
            flags.append(line)
    return flags


def import_csv(path: Path, conn=None) -> int:
    """Load a pre-2026-11-18 watch CSV in file order; rows already in the
    table (same market, source, report stamp) are skipped. Returns rows read."""
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    _with_cursor(conn, lambda cur: _insert(cur, rows))
    return len(rows)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default=str(EVAL / "calibration_report.json"))
    ap.add_argument("--stamp", default=None, help="override the report stamp (backfill)")
    ap.add_argument("--import-csv", default=None, help="load an old watch CSV instead")
    a = ap.parse_args()
    if a.import_csv:
        print(f"read {import_csv(Path(a.import_csv))} rows from {a.import_csv}")
    else:
        run(Path(a.report), a.stamp)
