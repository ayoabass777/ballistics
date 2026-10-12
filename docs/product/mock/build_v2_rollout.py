"""Build match-detail v2 page specs for the Discover flow fixtures (D119).

Source: engine/eval/consensus_slips_2026-10-07.json (the frozen run behind the
Discover cards). Output: docs/product/mock/v2-rollout-2026-10-07.json — one
spec per fixture with the exact strings the Figma v2 components need.
Follows match-detail-contract.md and D115 (no gaps, thresholds or model names).
Standings position and head-to-head are not in the export; marked unavailable.
"""
import json, sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "engine/eval/consensus_slips_2026-10-07.json"
OUT = Path(__file__).with_name("v2-rollout-2026-10-07.json")
IDS = [126487, 60892, 81893, 79909, 71009, 45061, 85316, 63544,
       110189, 128054, 110194, 66421, 79912, 124383, 107534, 81805, 86064,
       63542, 42665, 63543, 67724, 63547, 40116, 45065, 55123, 119643, 45064]

EV = {"win": "Won", "loss": "Lost", "draw": "Drew", "unbeaten": "Unbeaten", "winless": "Winless",
      "concede_1": "Conceded 1+", "concede_2": "Conceded 2+", "score_1goal": "Scored 1+",
      "score_2goals": "Scored 2+", "clean_sheet": "Clean sheet", "goalless": "Failed to score",
      "BTTS": "Both teams scored", "Over_1_5": "Over 1.5 goals", "Over_2_5": "Over 2.5 goals",
      "Over_3_5": "Over 3.5 goals", "Over_4_5": "Over 4.5 goals", "Under_1_5": "Under 1.5 goals",
      "Under_2_5": "Under 2.5 goals", "Under_3_5": "Under 3.5 goals", "Under_4_5": "Under 4.5 goals"}
PAT = {"well_above": "Much more often than usual", "above": "More often than usual",
       "typical": "Typical for this league", "below": "Less often than usual",
       "well_below": "Much less often than usual"}
GOAL = {k: v.replace("often", "likely") for k, v in PAT.items()}
COMPLEMENT = {"win": "winless", "winless": "win", "loss": "unbeaten", "unbeaten": "loss",
              "concede_1": "clean_sheet", "clean_sheet": "concede_1",
              "score_1goal": "goalless", "goalless": "score_1goal",
              **{f"Over_{n}_5": f"Under_{n}_5" for n in range(1, 5)},
              **{f"Under_{n}_5": f"Over_{n}_5" for n in range(1, 5)}}
MIRROR = {"loss": "win", "win": "loss", "unbeaten": "winless", "winless": "unbeaten",
          "concede_1": "score_1goal", "score_1goal": "concede_1",
          "concede_2": "score_2goals", "score_2goals": "concede_2",
          "clean_sheet": "goalless", "goalless": "clean_sheet"}


def pct(x): return f"{round(100 * x)}%"


def ko(s):
    d = datetime.fromisoformat(s)
    return d.strftime("%a ") + str(d.day) + d.strftime(" %b · %H:%M UTC")


def short(name, n=14):
    for pre in ("FK ", "FC ", "SC ", "AC "):
        if name.startswith(pre) and len(name) > n: name = name[len(pre):]
    return name


def pattern_rows(f):
    rows = [b for b in f["bayesian"] if b.get("primary") and b["n"] >= 6
            and b.get("league_comparison") in PAT and b["event"] != "halftime_win"]
    best = {}
    for b in rows:  # one row per (team, event)
        best[(b["side"], b["event"])] = b
    rows = list(best.values())
    # drop the weaker of complements within a team (D73; loss/winless, win/unbeaten kept per D106)
    out = []
    for b in rows:
        c = COMPLEMENT.get(b["event"]); o = best.get((b["side"], c)) if c else None
        if o and o.get("league_comparison") in PAT and o["n"] >= 6 and (
                abs(o["league_gap"]) > abs(b["league_gap"])
                or (abs(o["league_gap"]) == abs(b["league_gap"]) and b["league_gap"] < 0)):
            continue  # complements (win/winless, loss/unbeaten) say the same thing: keep the stronger
        out.append(b)
    return sorted(out, key=lambda b: -abs(b["league_gap"]))


NAMES = {}


def nm(b): return NAMES.get(b["side"], b["team"])


def side(b):
    return {"team": nm(b), "event": EV.get(b["event"], b["event"]), "record": f"{b['k']} / {b['n']}",
            "line": f"{PAT[b['league_comparison']]}\n{pct(b['k'] / b['n'])} vs league {pct(b['base_rate'])}"}


def patterns(f):
    NAMES.clear(); NAMES.update({"home": f["home"], "away": f["away"]})
    rows = pattern_rows(f); used = set(); pairs = []
    home = [b for b in rows if b["side"] == "home"]; away = [b for b in rows if b["side"] == "away"]
    for b in rows:
        if id(b) in used or len(pairs) == 2: continue
        other = away if b["side"] == "home" else home
        m = next((o for o in other if id(o) not in used and o["event"] == MIRROR.get(b["event"])), None) \
            or next((o for o in other if id(o) not in used and o["event"] == b["event"]), None)
        if not m: continue
        h, a = (b, m) if b["side"] == "home" else (m, b)
        if h["event"] in ("win", "loss", "draw", "unbeaten", "winless"): label = "Results"
        elif h["event"].startswith("concede") or h["event"] in ("clean_sheet",):
            label = f"{short(nm(h))} defence vs {short(nm(a))} attack"
        elif h["event"].startswith("score") or h["event"] == "goalless":
            label = f"{short(nm(h))} attack vs {short(nm(a))} defence"
        else: label = EV.get(h["event"], h["event"])
        pairs.append({"label": label, "home": side(h), "away": side(a)}); used |= {id(h), id(a)}
    also = [{"text": f"{nm(b)} · {EV.get(b['event'], b['event'])}", "record": f"{b['k']} / {b['n']}",
             "line": f"{PAT[b['league_comparison']]} · {pct(b['k'] / b['n'])} vs league {pct(b['base_rate'])}"}
            for b in rows if id(b) not in used][:2]
    short_teams = [t for t, s in ((f["home"], "home"), (f["away"], "away"))
                   if not any(b["side"] == s and b.get("primary") and b["n"] >= 6 for b in f["bayesian"])]
    return {"pairs": pairs, "also": also, "too_early": short_teams}


def goals(f):
    rows = [d for d in f.get("dixon_coles", []) if not d["market"].endswith(" Win")
            and d.get("league_comparison") in GOAL]
    rows = sorted(rows, key=lambda d: -d["probability"])[:3]
    out = []
    for d in rows:
        ctx = d.get("league_rate_context", "all")
        lab = GOAL[d["league_comparison"]] + f" · league {pct(d['league_rate'])}" + (f" ({ctx})" if ctx != "all" else "")
        m = d["market"].replace("Match Goals 1-4", "Match goals 1–4").replace(" Over ", " over ").replace(" Under ", " under ")
        m = m.replace("Match over", "Match over").replace("BTTS", "Both teams to score")
        out.append({"market": m, "value": pct(d["probability"]), "label": lab})
    return {"rows": out, "all_typical": bool(out) and all("Typical" in r["label"] for r in out)}


def strength(f, S):
    sl = {x["fixture_id"]: x for x in S["elo_gap_shortlist"]}
    byl = {(r["league"], r["country"]): r for r in S["elo_gap_group"]["by_league"]}
    mm = f.get("mismatch") or []
    if not mm: return {"state": "none"}
    gap = mm[0]["gap"]
    rec = byl.get((f["league"], f["country"]))
    fav = (f.get("discover_pick") or {}).get("favourite")
    if f["fixture_id"] in sl:
        x = sl[f["fixture_id"]]
        r = rec or {"drew": None, "lost": None}
        return {"state": "big_dog", "fav": x["favourite"], "dog": x["underdog"],
                "record_title": f"Big dogs in {f['league']}",
                "record": f"Won {x['league_big_dog_won']} of {x['league_big_dog_matches']} · {pct(x['league_big_dog_win_rate'])}",
                "record_note": f"Across past seasons in this league: drew {r['drew']}, lost {r['lost']}." if r.get("drew") is not None else ""}
    bdl = {(r["league"], r["country"]): r for r in S["elo_gap_group"].get("big_dog_leagues", [])}
    why_off = (bdl.get((f["league"], f["country"])) or {}).get("reason", "")
    if gap > 250 and "tier" in why_off:
        return {"state": "label_off", "record_title": "Big dog labels",
                "record": "Top-flight leagues only",
                "record_note": f"{f['league']} is not a top-flight league, so no Big dog label is shown."}
    if gap > 250 and rec and why_off.startswith("<"):
        return {"state": "label_off", "record_title": f"Favourites in {f['league']}",
                "record": f"Won {rec['won']} of {rec['matches']} · {pct(rec['win_rate'])}",
                "record_note": "Not enough past matches like this in this league for a Big dog label yet."}
    if gap > 250 and rec:
        return {"state": "label_off", "record_title": f"Favourites in {f['league']}",
                "record": f"Won {rec['won']} of {rec['matches']} · {pct(rec['win_rate'])}",
                "record_note": f"Favourites in this league have been too unpredictable for a Big dog label (drew {rec['drew']}, lost {rec['lost']})."}
    return {"state": "close"}


def why(f):
    p = f.get("discover_pick") or {}
    return {"strength": "Clear strength advantage", "goals": "Goal estimate well above the league",
            "patterns": "Record well above the league"}.get(p.get("list"), "Worth a closer look")


def main():
    S = json.loads(SRC.read_text())
    fa = {f["fixture_id"]: f for f in S["fixture_analysis"]}
    specs = []
    for i in IDS:
        f = fa[i]; st = f["strength"]
        def cell(t, k): return f"{st[t]['overall'][k]:.2f}"
        def rank(t, k): o = st[t]["overall"]; return f"#{o[k]} of {o['league_size']}"
        specs.append({
            "fixture_id": i, "home": f["home"], "away": f["away"], "league": f["league"], "country": f["country"],
            "kickoff": ko(f["kickoff"]), "bar_title": f"{short(f['home'])} v {short(f['away'])}",
            "bar_sub": f"{f['league']} · {ko(f['kickoff'])}", "why": why(f), "pick_list": (f.get('discover_pick') or {}).get('list'),
            "context": {"home_scored": cell("home_team", "attack_rate"), "away_scored": cell("away_team", "attack_rate"),
                        "home_conceded": cell("home_team", "defense_rate"), "away_conceded": cell("away_team", "defense_rate"),
                        "home_attack_rank": rank("home_team", "attack_rank"), "away_attack_rank": rank("away_team", "attack_rank"),
                        "home_defence_rank": rank("home_team", "defense_rank"), "away_defence_rank": rank("away_team", "defense_rank")},
            "strength": strength(f, S), "patterns": patterns(f), "goals": goals(f)})
    OUT.write_text(json.dumps({"run_ds": S["run_ds"], "source": SRC.name, "fixtures": specs}, ensure_ascii=False, indent=1))
    print(len(specs), "specs ->", OUT)


if __name__ == "__main__":
    main()
