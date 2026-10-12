"""What if older matches count less? Big dog records and upset rates with time decay.

Run from the repo root (needs the local DB):
    python -m engine.eval.diagnostics.big_dog_decay
    python -m engine.eval.diagnostics.big_dog_decay --half-lives 0 3 2 1 --changed-only

Each match is weighted 0.5 ** (age_in_years / half_life): with a 2-year half-life a
match from 2 years ago counts half, 4 years ago a quarter. half_life 0 = no decay
(today's rule). Because decay shrinks the effective sample, the Big dog floor uses
the effective number of matches (Kish: (Σw)² / Σw²) and the Wilson lower bound is
computed on that effective count. Same rules as the engine otherwise: Big dog = flat
pre-match Elo gap > 250; ≥ 30 effective Big dog matches; lower bound ≥ 60%.
Upset rate is shown decayed too, for reference only.
"""
import argparse
import math
from collections import defaultdict
from datetime import datetime, timezone

from engine.eval.db import get_conn
from engine.eval.consensus_slips import (
    ELO_HUGE_GAP_MIN, ELO_LEAGUE_MIN_LOW, ELO_LEAGUE_MIN_MATCHES, wilson_lower)

SQL = """
    SELECT f.league_name, COALESCE(rf.country_name, ''), rf.kickoff_utc,
           f.goals_for, f.goals_against,
           COALESCE(eh.rating_before_flat, eh.rating_before)
             - COALESCE(ea.rating_before_flat, ea.rating_before) AS diff
    FROM raw.fact_head_to_head f
    JOIN raw.fact_fixtures rf ON rf.fixture_id = f.fixture_id
    JOIN elo.team_elo_history eh ON eh.fixture_id = f.fixture_id AND eh.team_id = f.team_id
    JOIN elo.team_elo_history ea ON ea.fixture_id = f.fixture_id AND ea.team_id = f.opponent_id
    WHERE f.is_played AND f.is_home
"""


def summarise(rows, half_life, now):
    """rows: (age_years, is_big_dog, fav_won, drew, upset) per match → stats."""
    w_all = up = 0.0
    bw = bw2 = won = drew = 0.0
    oldest = 0.0
    for age, big, fav_won, is_draw, upset in rows:
        w = 1.0 if half_life == 0 else 0.5 ** (age / half_life)
        w_all += w; up += w * upset
        if big:
            bw += w; bw2 += w * w; won += w * fav_won; drew += w * is_draw
            oldest = max(oldest, age)
    n_eff = (bw * bw / bw2) if bw2 else 0.0
    rate = won / bw if bw else None
    low = wilson_lower(round(rate * n_eff), round(n_eff)) if bw and n_eff >= 1 else None
    ok = bool(n_eff >= ELO_LEAGUE_MIN_MATCHES and low is not None and low >= ELO_LEAGUE_MIN_LOW)
    return {"upset": up / w_all if w_all else None, "n_eff": n_eff, "rate": rate,
            "low": low, "ok": ok}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--half-lives", type=float, nargs="+", default=[0, 3, 2, 1],
                    help="years; 0 = no decay")
    ap.add_argument("--changed-only", action="store_true",
                    help="only leagues whose status differs across half-lives")
    ap.add_argument("--country")
    args = ap.parse_args()
    now = datetime.now(timezone.utc)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL)
            raw = cur.fetchall()
    finally:
        conn.close()
    by = defaultdict(list)
    for lg, ct, ko, hg, ag, diff in raw:
        if diff is None or diff == 0:
            continue
        age = max(0.0, (now - ko).total_seconds() / (365.25 * 86400))
        big = abs(diff) > ELO_HUGE_GAP_MIN
        fav_home = diff > 0
        fav_won = (hg > ag) if fav_home else (ag > hg)
        upset = (ag > hg) if fav_home else (hg > ag)
        by[(lg, ct)].append((age, big, fav_won, hg == ag, upset))
    hl = args.half_lives
    print("Big dog status with older matches weighted down. n = effective Big dog matches.\n"
          f"Rule: n ≥ {ELO_LEAGUE_MIN_MATCHES} and low ≥ {ELO_LEAGUE_MIN_LOW:.0%}. "
          "Half-life 0 = today's rule (no decay).\n")
    head = f"{'league':34s} {'all':>5s}"
    for h in hl:
        tag = "none" if h == 0 else f"{h:g}y"
        head += f" | {tag:>4s}: {'n':>4s} {'win':>5s} {'low':>5s} {'up':>5s}  "
    print(head)
    out = []
    for key, rows in by.items():
        if args.country and key[1].lower() != args.country.lower():
            continue
        if len(rows) < 50:
            continue
        stats = [summarise(rows, h, now) for h in hl]
        if args.changed_only and len({s["ok"] for s in stats}) == 1:
            continue
        out.append((key, len(rows), stats))
    out.sort(key=lambda x: (-sum(s["ok"] for s in x[2]), -(x[2][-1]["low"] or 0)))
    fmt = lambda v: f"{v:5.0%}" if v is not None else "    -"
    for (lg, ct), n_all, stats in out:
        line = f"{(lg + ' (' + ct + ')')[:34]:34s} {n_all:5d}"
        for s in stats:
            line += (f" |       {s['n_eff']:4.0f} {fmt(s['rate'])} {fmt(s['low'])} "
                     f"{fmt(s['upset'])} {'IN ' if s['ok'] else 'out'}")
        print(line)


if __name__ == "__main__":
    main()
