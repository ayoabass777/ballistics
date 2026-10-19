"""Model watch for the daily data email: shadow checks we are tracking before
deciding on them. Monitoring only -- nothing here fails the data flow check.

  over_1_5   Match Over 1.5 calibration after each weekly refit (D120), read
             from engine/eval/watch/match_over_1_5.csv (market_watch writes it)
  big_dogs   Big dog Dixon-Coles cross-check (D136): settled Big dogs from
             raw.big_dog_watch, favourites DC backs at >= 65% vs the rest
"""
from __future__ import annotations

import csv
import math
from datetime import date
from pathlib import Path

from etl.src.data_detector.common import logger

ROOT = Path(__file__).resolve().parents[3]           # repo root / /opt/airflow
OVER_1_5_CSV = ROOT / "engine" / "eval" / "watch" / "match_over_1_5.csv"
SOURCE_LABELS = {"all": "all models", "dixon_coles_only": "Dixon-Coles alone"}
BIG_DOG_MIN_N = 100        # confirmed Big dogs needed before the comparison means anything
Z = 1.645

BIG_DOG_SQL = """
SELECT w.kickoff, w.league, w.favourite, w.underdog, w.elo_gap, w.dc_win_prob,
       w.dc_confirms, h.goals_for, h.goals_against
FROM raw.big_dog_watch w
JOIN raw.engine_head_to_head h
  ON h.fixture_id = w.fixture_id AND h.is_home = w.fav_is_home
WHERE w.kickoff < now() AND h.goals_for IS NOT NULL
ORDER BY w.kickoff
"""


def wilson(k: int, n: int) -> tuple[float, float]:
    if n == 0:
        return 0.0, 0.0
    p = k / n
    centre, margin = p + Z * Z / (2 * n), Z * math.sqrt(p * (1 - p) / n + Z * Z / (4 * n * n))
    d = 1 + Z * Z / n
    return (centre - margin) / d, (centre + margin) / d


def over_1_5(path: Path = OVER_1_5_CSV) -> dict | None:
    """Latest refit per source, with the previous refit for comparison."""
    try:
        rows = list(csv.DictReader(path.open()))
    except OSError:
        return None
    if not rows:
        return None
    stamps = list(dict.fromkeys(r["report_stamp"] for r in rows))
    latest, prev = stamps[-1], (stamps[-2] if len(stamps) > 1 else None)
    out = []
    for r in (r for r in rows if r["report_stamp"] == latest):
        before = next((p for p in rows if p["report_stamp"] == prev and p["source"] == r["source"]), None)
        out.append({"source": SOURCE_LABELS.get(r["source"], r["source"]), "n": int(r["n"]),
                    "predicted": float(r["avg_predicted"]), "hit": float(r["hit_rate"]),
                    "overconfidence": float(r["overconfidence"]),
                    "previous_overconfidence": float(before["overconfidence"]) if before else None,
                    "flag": r.get("flag") or ""})
    return {"refit": latest, "rows": out}


def big_dogs(cur, ds: date) -> dict | None:
    """Settled shadow Big dogs: confirmed vs not, plus those played on ds."""
    cur.execute("SELECT to_regclass('raw.big_dog_watch') IS NOT NULL")
    if not cur.fetchone()[0]:
        return None
    cur.execute(BIG_DOG_SQL)
    groups = {True: [0, 0], False: [0, 0]}
    day = []
    for kickoff, league, fav, dog, gap, dc, confirms, gf, ga in cur.fetchall():
        won = gf > ga
        if confirms is not None:
            groups[bool(confirms)][0] += won
            groups[bool(confirms)][1] += 1
        if kickoff.date() == ds:
            day.append({"match": f"{fav} v {dog}", "league": league, "score": f"{gf}-{ga}",
                        "gap": int(gap), "dc": None if dc is None else float(dc),
                        "result": "won" if won else ("draw" if gf == ga else "lost")})

    def summary(k, n):
        lo, hi = wilson(k, n)
        return {"n": n, "won": k, "rate": k / n if n else None, "low": lo, "high": hi}

    yes, no = summary(*groups[True]), summary(*groups[False])
    if yes["n"] < BIG_DOG_MIN_N:
        verdict = f"collecting ({yes['n']} of {BIG_DOG_MIN_N} confirmed Big dogs settled)"
    elif no["n"] and yes["low"] > no["high"]:
        verdict = "cross-check separates: confirmed favourites win clearly more"
    elif no["n"] and yes["rate"] <= no["rate"]:
        verdict = "FLAG cross-check not helping: confirmed favourites do not win more"
    else:
        verdict = "confirmed ahead, ranges still overlap"
    return {"confirmed": yes, "not_confirmed": no, "day": day, "verdict": verdict}


def collect(cur, ds: date) -> dict:
    watch = {"over_1_5": over_1_5()}
    cur.execute("SAVEPOINT model_watch")
    try:
        watch["big_dogs"] = big_dogs(cur, ds)
        cur.execute("RELEASE SAVEPOINT model_watch")
    except Exception as exc:
        cur.execute("ROLLBACK TO SAVEPOINT model_watch")
        logger.warning("model watch big dogs unavailable: %s", exc)
        watch["big_dogs"] = None
    return watch
