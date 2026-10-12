"""Which teams are the Big dogs in a league, and how did each do?

Run from the repo root (needs the local DB):
    python -m engine.eval.diagnostics.big_dog_teams --league "Eerste Divisie" --country Netherlands
    python -m engine.eval.diagnostics.big_dog_teams --league "1. Division" --country Belarus

Big dog match = flat pre-match Elo gap > 250 (the engine rule). Lists every team
that was the favourite in such a match: matches, won/drew/lost, seasons, and its
last Big dog match. Second-tier leagues often have reserve or B teams, which the
'reserve?' flag marks (name contains II, B, U19/U21/U23, Jong, Reserves).
"""
import argparse
import re
from collections import defaultdict

from engine.eval.db import get_conn
from engine.eval.consensus_slips import ELO_HUGE_GAP_MIN

SQL = """
    SELECT rf.kickoff_utc, f.team_name, f.opponent_name, f.goals_for, f.goals_against,
           COALESCE(eh.rating_before_flat, eh.rating_before)
             - COALESCE(ea.rating_before_flat, ea.rating_before) AS diff
    FROM raw.fact_head_to_head f
    JOIN raw.fact_fixtures rf ON rf.fixture_id = f.fixture_id
    JOIN elo.team_elo_history eh ON eh.fixture_id = f.fixture_id AND eh.team_id = f.team_id
    JOIN elo.team_elo_history ea ON ea.fixture_id = f.fixture_id AND ea.team_id = f.opponent_id
    WHERE f.is_played AND f.is_home AND f.league_name = %s AND COALESCE(rf.country_name, '') = %s
"""
_SUFFIX = re.compile(r"\s+(II|B|U\d{2}|Reserves?)$", re.I)
_PREFIX = re.compile(r"^Jong\s+", re.I)


def reserve_parent(name: str, clubs: set[str]) -> str | None:
    """Parent club if `name` is its reserve/youth side AND the parent exists in the data.

    'Minsk II' -> 'Minsk', 'Jong Utrecht' -> 'Utrecht'/'FC Utrecht'; 'Willem II' -> None
    because no club called 'Willem' exists.
    """
    lower = {c.lower(): c for c in clubs}
    for base in (_SUFFIX.sub("", name), _PREFIX.sub("", name)):
        if base == name:
            continue
        for cand in (base, f"FC {base}", f"Fc {base}", f"{base} FC"):
            if cand.lower() in lower and cand.lower() != name.lower():
                return lower[cand.lower()]
        pref = [c for c in clubs if c.lower().startswith(base.lower() + " ")
                and c.lower() != name.lower() and not _SUFFIX.search(c)]
        if pref:                                  # 'Bate II' -> 'Bate Borisov'
            return pref[0]
        if name.lower().startswith("jong "):   # Jong sides are always reserves
            return base
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--country", required=True)
    args = ap.parse_args()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL, (args.league, args.country))
            rows = cur.fetchall()
    finally:
        conn.close()
    clubs = {r[1] for r in rows} | {r[2] for r in rows}
    is_res = {c: reserve_parent(c, clubs) is not None for c in clubs}
    split = {True: [0, 0], False: [0, 0]}   # underdog reserve? -> [matches, fav wins]
    teams = defaultdict(lambda: {"n": 0, "w": 0, "d": 0, "l": 0, "seasons": set(), "last": None, "opps": defaultdict(int)})
    total = 0
    for ko, home, away, hg, ag, diff in rows:
        if diff is None or abs(diff) <= ELO_HUGE_GAP_MIN:
            continue
        total += 1
        fav, dog = (home, away) if diff > 0 else (away, home)
        fg, dg = (hg, ag) if diff > 0 else (ag, hg)
        t = teams[fav]
        t["n"] += 1
        t["w" if fg > dg else "d" if fg == dg else "l"] += 1
        t["seasons"].add(ko.year)
        t["last"] = max(t["last"] or ko, ko)
        t["opps"][dog] += 1
        sp = split[is_res[dog]]; sp[0] += 1; sp[1] += fg > dg
    print(f"{args.league} ({args.country}): {total} Big dog matches, {len(teams)} different favourites\n")
    print(f"{'favourite':28s} {'n':>4s} {'W':>4s} {'D':>4s} {'L':>4s} {'win%':>5s}  {'years':12s} {'last':10s} reserve?  most-beaten underdog")
    for name, t in sorted(teams.items(), key=lambda x: -x[1]["n"]):
        yrs = f"{min(t['seasons'])}-{max(t['seasons'])}" if len(t["seasons"]) > 1 else str(min(t["seasons"]))
        opp = max(t["opps"].items(), key=lambda x: x[1])
        print(f"{name[:28]:28s} {t['n']:4d} {t['w']:4d} {t['d']:4d} {t['l']:4d} {t['w']/t['n']:5.0%}  {yrs:12s} "
              f"{t['last'].date().isoformat():10s} {'yes' if is_res[name] else '':8s}  {opp[0][:20]} ({opp[1]})")
    from engine.eval.consensus_slips import wilson_lower, ELO_LEAGUE_MIN_LOW, ELO_LEAGUE_MIN_MATCHES
    print()
    for res, label in ((True, "underdog is a reserve/youth side"), (False, "underdog is a first team")):
        n, w = split[res]
        if n:
            low = wilson_lower(w, n)
            ok = n >= ELO_LEAGUE_MIN_MATCHES and low >= ELO_LEAGUE_MIN_LOW
            print(f"{label:34s} {n:4d} matches  won {w:4d} ({w/n:4.0%})  low {low:4.0%}"
                  + ("" if res else f"  -> {'passes' if ok else 'fails'} the 30-match / 60% rule on its own"))


if __name__ == "__main__":
    main()
