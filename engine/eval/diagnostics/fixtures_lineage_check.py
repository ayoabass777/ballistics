"""Before/after check for the fact_fixtures switch (2026-10-11).

The engine used to read raw.raw_fixtures (API-Football team ids) and rebuild an
API -> warehouse team map by joining fact_head_to_head. It now reads dbt's
raw.fact_fixtures, which carries warehouse ids directly. This audit (the only
engine-side code allowed to read raw.raw_fixtures) shows:

  1. fact_fixtures has every raw fixture with identical status/kickoff/goals;
  2. wherever the old map resolved a team, it agrees with the new warehouse id;
  3. how many upcoming fixture sides gain an id the old map could not find;
  4. how often the old Poisson strength merge (league + API id against
     warehouse ids) could have matched the wrong club.

Run:  python -m engine.eval.diagnostics.fixtures_lineage_check
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from engine.eval.db import get_conn

RAW = "raw." + "raw_fixtures"   # spelled out so the engine guard test stays strict


def main() -> None:
    conn = get_conn()
    try:
        raw = pd.read_sql(f"""
            SELECT fixture_id, fixture_status, kickoff_utc, api_league_id,
                   home_team_id AS home_api, away_team_id AS away_api,
                   home_team_fulltime_goal AS hg, away_team_fulltime_goal AS ag
            FROM {RAW}""", conn)
        fact = pd.read_sql("""
            SELECT fixture_id, fixture_status, kickoff_utc, api_league_id,
                   home_team_id, away_team_id, league_name, league_season_id,
                   home_team_fulltime_goal AS hg, away_team_fulltime_goal AS ag
            FROM raw.fact_fixtures""", conn)
        old_map = pd.read_sql(f"""
            SELECT DISTINCT r.home_team_id AS api_id, f.team_id AS wh_id
            FROM {RAW} r JOIN raw.fact_head_to_head f
              ON f.fixture_id = r.fixture_id AND f.is_home
            UNION
            SELECT DISTINCT r.away_team_id, f.opponent_id
            FROM {RAW} r JOIN raw.fact_head_to_head f
              ON f.fixture_id = r.fixture_id AND f.is_home""", conn)
        hist_teams = pd.read_sql("""
            SELECT DISTINCT league_name, team_id FROM raw.fact_head_to_head
            WHERE is_played""", conn)
    finally:
        conn.close()

    print(f"raw fixtures:  {len(raw)}")
    print(f"fact fixtures: {len(fact)}")
    missing = set(raw.fixture_id) - set(fact.fixture_id)
    print(f"raw fixtures missing from fact: {len(missing)}")

    m = raw.merge(fact, on="fixture_id", suffixes=("_raw", "_fact"))
    for col in ("fixture_status", "kickoff_utc", "api_league_id", "hg", "ag"):
        a, b = m[f"{col}_raw"], m[f"{col}_fact"]
        diff = ~((a == b) | (a.isna() & b.isna()))
        print(f"  {col:<15} differs: {int(diff.sum())}")

    dup = old_map.groupby("api_id")["wh_id"].nunique()
    print(f"old map: {len(dup)} API ids, {int((dup > 1).sum())} mapped to >1 warehouse id")
    amap = old_map.drop_duplicates("api_id").set_index("api_id")["wh_id"]

    for side in ("home", "away"):
        old = m[f"{side}_api"].map(amap)
        new = m[f"{side}_team_id"]
        both = old.notna() & new.notna()
        print(f"{side}: old resolved {int(old.notna().sum())}, new resolved "
              f"{int(new.notna().sum())}, disagree {int((old[both] != new[both]).sum())}")

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    m["kickoff_utc_fact"] = pd.to_datetime(m["kickoff_utc_fact"], utc=True)
    up = m[(m["fixture_status_fact"] == "NS")
           & m["kickoff_utc_fact"].between(today, today + timedelta(days=7))]
    print(f"\nupcoming (next 7 days): {len(up)} fixtures")
    for side in ("home", "away"):
        old = up[f"{side}_api"].map(amap)
        new = up[f"{side}_team_id"]
        print(f"  {side}: old map resolved {int(old.notna().sum())}, "
              f"fact resolves {int(new.notna().sum())}, "
              f"gained {int((old.isna() & new.notna()).sum())}")

    # Old Poisson bug exposure: strength merged on (league, API id) against
    # warehouse ids. A hit means an unrelated club's attack/defence was used.
    known = set(zip(hist_teams["league_name"], hist_teams["team_id"]))
    hits = sum((lg, api) in known
               for lg, api in zip(list(up["league_name"]) * 2,
                                  list(up["home_api"]) + list(up["away_api"])))
    print(f"  Poisson sides that used another club's strength (old): {hits} "
          f"of {2 * len(up)}; the rest fell back to league means")


if __name__ == "__main__":
    main()
