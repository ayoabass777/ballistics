"""Match page Context: league standings and head-to-head for one fixture.

Contract: docs/product/match-detail-contract.md §3.2 and §5. Reads dbt models
only (raw.fact_fixtures, raw_mart.mart_league_standings, raw.fact_head_to_head),
warehouse ids only. Standings and H2H are a current snapshot (D69); the
payload carries standings_updated_at so the page can show it.

CLI (prints the payload for one fixture):
    FORECAST_API_DSN=... python -m engine.api.match_context 126487
"""
import json
import sys

from engine.api.crests import crest_url

H2H_LIMIT = 5

FIXTURE_SQL = """
SELECT f.fixture_id, f.league_season_id, f.league_id, f.league_name, f.country_name,
       kickoff_utc, home_team_id, away_team_id, home_team_name, away_team_name,
       home_logo_file_id, away_logo_file_id, l.logo_file_id AS league_logo_file_id
FROM raw.fact_fixtures f
LEFT JOIN dim.dim_leagues l ON l.league_id = f.league_id
WHERE f.fixture_id = %(fixture_id)s
"""

STANDINGS_SQL = """
SELECT team_id, team_name, group_name, group_order, rank, games_played, wins,
       draws, losses, goals_for, goals_against, goal_diff, points, form,
       to_jsonb(s)->'home_record' AS home_record,
       to_jsonb(s)->'away_record' AS away_record,
       description, standings_updated_at, standings_format, n_groups,
       ranks_repeat, is_team_current_group, standings_incomplete, unprocessed_rows,
       is_stale_row
FROM raw_mart.mart_league_standings s
WHERE league_season_id = %(league_season_id)s
ORDER BY group_order, rank, team_name
"""

FALLBACK_FIXTURES_SQL = """
SELECT home_team_id, away_team_id, home_team_fulltime_goal, away_team_fulltime_goal
FROM raw.fact_fixtures
WHERE league_season_id = %(league_season_id)s
  AND is_played AND stage = 'league'
  AND kickoff_utc < LEAST(CURRENT_TIMESTAMP, %(cutoff)s)
  AND home_team_id IS NOT NULL AND away_team_id IS NOT NULL
  AND home_team_fulltime_goal IS NOT NULL AND away_team_fulltime_goal IS NOT NULL
"""

# One row per meeting, seen from today's home team. Played only (fact filter).
H2H_SQL = """
SELECT fixture_id, kickoff_utc, is_home, goals_for, goals_against,
       team_name, opponent_name
FROM raw.fact_head_to_head
WHERE team_id = %(home_team_id)s AND opponent_id = %(away_team_id)s
  AND kickoff_utc < LEAST(%(cutoff)s, CURRENT_TIMESTAMP)
  AND is_played AND goals_for IS NOT NULL AND goals_against IS NOT NULL
ORDER BY kickoff_utc DESC, fixture_id DESC
LIMIT %(limit)s
"""


def team_position(rows, team_id):
    """The team's row in its current group, or None ("Not in table yet")."""
    if team_id is None:
        return None
    mine = [r for r in rows if r["team_id"] == team_id and r["is_team_current_group"]]
    if not mine:
        return None
    r = mine[0]
    group = [g for g in rows if g["group_name"] == r["group_name"]]
    return {
        "team_id": team_id, "group_name": r["group_name"],
        "show_group_name": (r["n_groups"] or 1) > 1,
        "rank": None if r["ranks_repeat"] else r["rank"],
        "source": "official",
        "group_size": len(group), "points": r["points"],
        "games_played": r["games_played"], "wins": r["wins"],
        "draws": r["draws"], "losses": r["losses"], "goal_diff": r["goal_diff"],
        "form": r["form"], "description": r["description"],
        "ranks_repeat": bool(r["ranks_repeat"]),
        "stale": bool(r.get("is_stale_row")),   # last good row; the API's latest was unprocessable (D147)
    }


def calculated_overall_positions(rows, fixtures, home_id, away_id):
    """Fixture-derived fallback only when the provider has one unambiguous table."""
    if not rows or any(r["standings_format"] != "single" or r["ranks_repeat"] for r in rows):
        return {"home": None, "away": None}
    group = rows[0]["group_name"]
    scores = {}
    for f in fixtures:
        hg, ag = f["home_team_fulltime_goal"], f["away_team_fulltime_goal"]
        for team_id, gf, ga in ((f["home_team_id"], hg, ag),
                                (f["away_team_id"], ag, hg)):
            s = scores.setdefault(team_id, {"points": 0, "goal_diff": 0, "goals_for": 0,
                                             "games_played": 0})
            s["points"] += 3 if gf > ga else 1 if gf == ga else 0
            s["goal_diff"] += gf - ga
            s["goals_for"] += gf
            s["games_played"] += 1
    if not scores:
        return {"home": None, "away": None}
    ranked = sorted(scores, key=lambda i: (-scores[i]["points"],
                                           -scores[i]["goal_diff"],
                                           -scores[i]["goals_for"], i))

    def position(team_id):
        if team_id not in scores:
            return None
        return {"team_id": team_id, "group_name": group, "show_group_name": False,
                "rank": ranked.index(team_id) + 1, "group_size": len(ranked),
                "points": scores[team_id]["points"],
                "games_played": scores[team_id]["games_played"],
                "source": "calculated", "stale": False, "ranks_repeat": False}

    return {"home": position(home_id), "away": position(away_id)}


def calculated_venue_positions(rows, fixtures, home_id, away_id):
    """Fixture-derived venue fallback while provider split records are absent."""
    if not rows or any(r["standings_format"] != "single" or r["ranks_repeat"] for r in rows):
        return {"home": None, "away": None}
    team_ids = {r["team_id"] for r in rows if r["team_id"] is not None}
    team_ids.update(i for f in fixtures for i in (f["home_team_id"], f["away_team_id"]))
    scores = {venue: {i: {"points": 0, "goal_diff": 0, "goals_for": 0, "games_played": 0}
                      for i in team_ids} for venue in ("home", "away")}
    for f in fixtures:
        hg, ag = f["home_team_fulltime_goal"], f["away_team_fulltime_goal"]
        for venue, team_id, gf, ga in (("home", f["home_team_id"], hg, ag),
                                       ("away", f["away_team_id"], ag, hg)):
            s = scores[venue][team_id]
            s["points"] += 3 if gf > ga else 1 if gf == ga else 0
            s["goal_diff"] += gf - ga
            s["goals_for"] += gf
            s["games_played"] += 1

    def position(team_id, venue):
        if team_id not in team_ids or not scores[venue][team_id]["games_played"]:
            return None
        ranked = sorted(team_ids, key=lambda i: (-scores[venue][i]["points"],
                                                 -scores[venue][i]["goal_diff"],
                                                 -scores[venue][i]["goals_for"], i))
        mine = scores[venue][team_id]
        return {"rank": ranked.index(team_id) + 1, "group_size": len(ranked),
                "points": mine["points"], "games_played": mine["games_played"],
                "group_name": rows[0]["group_name"], "show_group_name": False,
                "source": "calculated"}

    return {"home": position(home_id, "home"), "away": position(away_id, "away")}


def venue_positions(rows, home_id, away_id):
    """Rank provider home/away records within the current standings group."""
    current = {r["team_id"]: r for r in rows
               if r["is_team_current_group"] and not r["ranks_repeat"]}

    def position(team_id, venue):
        if team_id not in current:
            return None
        group = current[team_id]["group_name"]
        peers = [r for r in current.values() if r["group_name"] == group]
        # Partial/old refreshes must not masquerade as a complete venue table.
        records = {r["team_id"]: r.get(f"{venue}_record") for r in peers}
        if any(not isinstance(rec, dict) or rec.get("played") is None
               or rec.get("win") is None or rec.get("draw") is None
               or not isinstance(rec.get("goals"), dict)
               or rec["goals"].get("for") is None or rec["goals"].get("against") is None
               for rec in records.values()):
            return None
        mine = records[team_id]
        if not mine["played"]:
            return None

        def score(i):
            rec = records[i]
            goals = rec["goals"]
            return (3 * rec["win"] + rec["draw"],
                    goals["for"] - goals["against"], goals["for"])

        ranked = sorted(records, key=lambda i: tuple(-x for x in score(i)) + (i,))
        return {"rank": ranked.index(team_id) + 1, "group_size": len(peers),
                "points": score(team_id)[0], "games_played": mine["played"],
                "group_name": group,
                "show_group_name": (current[team_id]["n_groups"] or 1) > 1,
                "source": "provider_split"}

    return {"home": position(home_id, "home"), "away": position(away_id, "away")}


def summarise_h2h(meetings, home_name, away_name):
    """Tally and goal stats from newest-first meetings seen from today's home team."""
    n = len(meetings)
    if n == 0:
        return {"meetings": 0, "games": [], "message": "No previous meetings in our records"}
    home_w = sum(m["goals_for"] > m["goals_against"] for m in meetings)
    away_w = sum(m["goals_for"] < m["goals_against"] for m in meetings)
    games = []
    for m in meetings:
        h, a = (home_name, away_name) if m["is_home"] else (away_name, home_name)
        hg, ag = (m["goals_for"], m["goals_against"]) if m["is_home"] else (m["goals_against"], m["goals_for"])
        winner = h if hg > ag else a if ag > hg else None
        games.append({"fixture_id": m["fixture_id"], "kickoff_utc": _iso(m["kickoff_utc"]),
                      "home": h, "away": a, "home_goals": hg, "away_goals": ag, "winner": winner})
    out = {"meetings": n, "since": games[-1]["kickoff_utc"],
           "tally": {"home_wins": home_w, "draws": n - home_w - away_w, "away_wins": away_w},
           "games": games}
    if n >= 3:
        totals = [m["goals_for"] + m["goals_against"] for m in meetings]
        at_home = [m for m in meetings if m["is_home"]]
        out["stats"] = {
            "goals_per_game": round(sum(totals) / n, 1),
            "over_2_5": sum(t > 2.5 for t in totals),
            "both_scored": sum(m["goals_for"] > 0 and m["goals_against"] > 0 for m in meetings),
            "at_home_team": {
                "meetings": len(at_home),
                "home_wins": sum(m["goals_for"] > m["goals_against"] for m in at_home),
                "draws": sum(m["goals_for"] == m["goals_against"] for m in at_home),
                "away_wins": sum(m["goals_for"] < m["goals_against"] for m in at_home)}}
    else:
        out["stats"] = None
        out["stats_note"] = "too few for goal stats"
    return out


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


def build_context(cur, fixture_id):
    """Payload for one fixture, or None when the fixture is unknown."""
    cur.execute(FIXTURE_SQL, {"fixture_id": fixture_id})
    fx = cur.fetchone()
    if fx is None:
        return None
    cur.execute(STANDINGS_SQL, {"league_season_id": fx["league_season_id"]})
    rows = cur.fetchall()
    home_id, away_id = fx["home_team_id"], fx["away_team_id"]
    h2h = {"meetings": 0, "games": [], "message": "No previous meetings in our records"}
    if home_id is not None and away_id is not None:
        cur.execute(H2H_SQL, {"home_team_id": home_id, "away_team_id": away_id,
                              "cutoff": fx["kickoff_utc"], "limit": H2H_LIMIT})
        h2h = summarise_h2h(cur.fetchall(), fx["home_team_name"], fx["away_team_name"])
    venue = venue_positions(rows, home_id, away_id)
    home_pos, away_pos = team_position(rows, home_id), team_position(rows, away_id)
    needs_home = home_id is not None and (home_pos is None or home_pos["rank"] is None or home_pos["stale"])
    needs_away = away_id is not None and (away_pos is None or away_pos["rank"] is None or away_pos["stale"])
    needs_venue = ((home_id is not None and venue["home"] is None)
                   or (away_id is not None and venue["away"] is None))
    if ((needs_home or needs_away or needs_venue) and rows and rows[0].get("standings_updated_at")
            and all(r["standings_format"] == "single" and not r["ranks_repeat"] for r in rows)):
        cur.execute(FALLBACK_FIXTURES_SQL, {"league_season_id": fx["league_season_id"],
                                            "cutoff": rows[0]["standings_updated_at"]})
        fixtures = cur.fetchall()
        fallback = calculated_overall_positions(rows, fixtures, home_id, away_id)
        if needs_home:
            home_pos = fallback["home"] or home_pos
        if needs_away:
            away_pos = fallback["away"] or away_pos
        if needs_venue:
            venue_fallback = calculated_venue_positions(rows, fixtures, home_id, away_id)
            venue = {side: venue[side] or venue_fallback[side] for side in ("home", "away")}
    same_group = bool(home_pos and away_pos and home_pos["group_name"] == away_pos["group_name"]
                      and home_pos.get("source") == away_pos.get("source"))
    hidden = {r["group_name"] for r in rows if r["ranks_repeat"]}
    return {
        "fixture": {"fixture_id": fx["fixture_id"], "league_id": fx["league_id"],
                    "league": fx["league_name"], "country": fx["country_name"],
                    "kickoff_utc": _iso(fx["kickoff_utc"]),
                    "home": fx["home_team_name"], "away": fx["away_team_name"],
                    "home_team_id": home_id, "away_team_id": away_id,
                    "league_logo_url": crest_url("leagues", fx.get("league_logo_file_id")),
                    "home_crest_url": crest_url("teams", fx.get("home_logo_file_id")),
                    "away_crest_url": crest_url("teams", fx.get("away_logo_file_id"))},
        "standings_updated_at": _iso(rows[0]["standings_updated_at"]) if rows else None,
        "position": {"home": home_pos, "away": away_pos,
                     "venue": venue,
                     "points_gap": abs(home_pos["points"] - away_pos["points"]) if same_group and home_pos["points"] is not None and away_pos["points"] is not None else None},
        "table": {"available": any(r["group_name"] not in hidden for r in rows),
                  "standings_format": rows[0]["standings_format"] if rows else None,
                  # D147: the API sent rows we couldn't load; they wait in the DLQ
                  # and the table may be out of date until a clean refresh.
                  "incomplete": any(r.get("standings_incomplete") for r in rows),
                  "unprocessed_rows": max((r.get("unprocessed_rows") or 0 for r in rows), default=0),
                  "groups": _groups([r for r in rows if r["group_name"] not in hidden])},
        "h2h": h2h,
    }


def _groups(rows):
    groups = {}
    for r in rows:
        g = groups.setdefault(r["group_name"], {"group_name": r["group_name"],
                                                "group_order": r["group_order"], "rows": []})
        g["rows"].append({**{k: r[k] for k in ("rank", "team_id", "team_name", "games_played",
                                               "goal_diff", "points", "description")},
                          "stale": bool(r.get("is_stale_row"))})
    return sorted(groups.values(), key=lambda g: g["group_order"] or 0)


def build_context_page(cur, fixture_id):
    """/context payload with its `data_quality` block (D148)."""
    from engine.api import data_quality as dq
    page = dq.decorate(dq.Component(lambda c, fixture_id: build_context(c, fixture_id)),
                       dq.StandingsQuality, dq.Freshness, dq.FixtureRowsQuality)
    return page.build(cur, fixture_id=fixture_id)


def main(argv):
    from engine.api.forecast_records import read_cursor
    fixture_id = int(argv[1])
    with read_cursor() as cur:
        payload = build_context(cur, fixture_id)
    if payload is None:
        print(f"fixture {fixture_id} not found", file=sys.stderr)
        return 1
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
