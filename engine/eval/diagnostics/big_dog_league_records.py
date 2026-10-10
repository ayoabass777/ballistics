"""Which leagues get Big dogs: today's rule vs the D81 rule (no upset gate, buffer, top flight).

Run from the repo root (needs the local DB):
    python -m engine.eval.diagnostics.big_dog_league_records            # all leagues
    python -m engine.eval.diagnostics.big_dog_league_records --changed  # only leagues that differ
    python -m engine.eval.diagnostics.big_dog_league_records --country France

Today (D74): upset rate ≤ 27% AND ≥ 30 Big dog matches AND Big dog low ≥ 60%.
D81: no upset gate; top flight (tier 1) only; ≥ 30 Big dog matches; joins at low
≥ 60%, and a league already in (big_dog_leagues_state.json, or today's list on the
first run) stays until its low falls below 57%. Read-only: does not write the state file.
"""
import argparse

from engine.eval.db import get_conn
from engine.eval.consensus_slips import (
    BIG_DOG_STATE_PATH, ELO_LOW_VOLATILITY_MAX, ELO_VOLATILITY_MIN_HISTORY,
    _load_elo_gap_group_record, _load_league_elo_volatility, _load_league_tiers,
    classify_big_dog_leagues, league_big_dogs_trusted, load_big_dog_state)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--country")
    ap.add_argument("--changed", action="store_true")
    args = ap.parse_args()
    conn = get_conn()
    try:
        vol = _load_league_elo_volatility(conn)
        group = _load_elo_gap_group_record(conn, vol, max_upset=1.0)
        tiers = _load_league_tiers(conn)
    finally:
        conn.close()
    today = {f"{r['league']} ({r['country']})" for r in group["by_league"]
             if league_big_dogs_trusted(r)
             and vol[(r["league"], r["country"])]["upset_rate"] <= ELO_LOW_VOLATILITY_MAX
             and vol[(r["league"], r["country"])]["matches"] >= ELO_VOLATILITY_MIN_HISTORY}
    prev = load_big_dog_state()
    seed = "state file" if prev else "today's list (first run)"
    prev = prev or today
    rows = classify_big_dog_leagues(group["by_league"], tiers, prev)
    if args.country:
        rows = [r for r in rows if r["country"].lower() == args.country.lower()]
    for r in rows:
        r["today"] = "in" if f"{r['league']} ({r['country']})" in today else "out"
        r["upset"] = vol[(r["league"], r["country"])]["upset_rate"]
    if args.changed:
        rows = [r for r in rows if r["today"] != r["status"]]
    rows.sort(key=lambda r: (r["status"] != "in", r["today"] == r["status"], -(r["win_rate_low"] or 0)))
    n_today, n_new = len(today), sum(r["status"] == "in" for r in classify_big_dog_leagues(group["by_league"], tiers, prev))
    print(f"Buffer seeded from {seed}. Leagues with Big dogs: today {n_today} → D81 {n_new}\n")
    print(f"{'league':36s} {'tier':>4s} {'upset':>6s} {'bigdog':>6s} {'win%':>5s} {'low':>5s}  {'today':5s} {'D81':4s} {'change':8s} reason")
    for r in rows:
        ch = "" if r["today"] == r["status"] else ("GAINS" if r["status"] == "in" else "LOSES")
        print(f"{(r['league'] + ' (' + r['country'] + ')')[:36]:36s} {str(r['tier'] or '?'):>4s} "
              f"{r['upset']:6.1%} {r['matches'] or 0:6d} {(r['win_rate'] or 0):5.0%} {(r['win_rate_low'] or 0):5.0%}  "
              f"{r['today']:5s} {r['status']:4s} {ch:8s} {r['reason']}")


if __name__ == "__main__":
    main()
