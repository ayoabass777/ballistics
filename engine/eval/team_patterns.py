"""Team patterns selection for the match page (contract §3.4, D122).

Input: one fixture's ``bayesian[]`` rows after ``attach_league_comparisons``
(each row has side, team, event, k, n, alpha, beta, base_rate, context,
display_eligible, league_gap, league_comparison, streak).

Qualification depends on the event family (D123):
  * Results and Attack/defence (team events): above the league average and
    league-difference confidence >= 0.75 -- P(theta > league base) when the team is above the league,
    P(theta < base) when below, from the record's Beta(alpha, beta). The
    engine's calibrated score is not used here: it is fitted on betting
    events only and floors rare events, so it cannot see e.g. Lost 9/10.
  * Match goals (Over/Under, BTTS): ``display_eligible`` (calibrated engine
    score >= 0.75), where that calibration is validated. These never take a
    top slot.
Top slots rank by |league gap|; confidence breaks ties.

Output per view ("venue": home team at home, away team away; "all": both
teams all venues): top_results, top_attack_defence, also (max 3), see_all.
Records carry display fields only -- no posterior, Beta parameters, gap or
confidence (D115: nothing that exposes the engine).
"""
from __future__ import annotations

from scipy.special import betainc

MIN_N = 6
TOP_CONFIDENCE = 0.75
ALSO_MAX = 3

RESULT_EVENTS = ("win", "loss", "draw", "winless", "unbeaten", "halftime_win")
ATTACK_DEFENCE_EVENTS = ("score_1goal", "score_2goals", "goalless",
                         "concede_1", "concede_2", "clean_sheet")
EXCLUDED_EVENTS = {"Over_4_5", "Under_1_5"}   # D124: never qualified; dropped
NO_COMPARISON = {"too_few_matches", "unavailable", None}

# Comparison wording (D126). Evidence, not gap size alone, decides the label:
#   more/less often  league-difference confidence >= 0.75 in that direction
#   much ...         and |gap| >= 15 points
#   typical          |gap| < 5 points with at least TYPICAL_MIN_N games
#   inconclusive     anything else: the page shows the record, no comparison line
TYPICAL_MIN_N = 12
TYPICAL_MAX_GAP = 0.05
WELL_GAP = 0.15

# Attack event for one team <-> defence event for the other.
CORRESPONDING = {
    "score_1goal": "concede_1", "concede_1": "score_1goal",
    "score_2goals": "concede_2", "concede_2": "score_2goals",
    "goalless": "clean_sheet", "clean_sheet": "goalless",
}

LABELS = {
    "win": "Won", "loss": "Lost", "draw": "Drew",
    "winless": "Winless", "unbeaten": "Unbeaten",
    "halftime_win": "Led at half-time",
    "score_1goal": "Scored 1+", "score_2goals": "Scored 2+",
    "goalless": "Failed to score",
    "concede_1": "Conceded 1+", "concede_2": "Conceded 2+",
    "clean_sheet": "Clean sheet", "BTTS": "Both teams scored",
}


def label(event: str) -> str:
    if event in LABELS:
        return LABELS[event]
    for prefix, word in (("Over_", "Over"), ("Under_", "Under")):
        if event.startswith(prefix):
            line = event[len(prefix):].replace("_", ".")
            return f"{word} {line} goals"
    return event


def ladder(event: str) -> str | None:
    """Rungs of the same ladder compete; at most one per team is kept."""
    if event in ("score_1goal", "score_2goals"):
        return "scored"
    if event in ("concede_1", "concede_2"):
        return "conceded"
    if event.startswith("Over_"):
        return "over"
    if event.startswith("Under_"):
        return "under"
    return None


def league_confidence(row: dict) -> float | None:
    """P(team rate differs from the league in the direction of the gap)."""
    a, b, base, gap = row.get("alpha"), row.get("beta"), row.get("base_rate"), row.get("league_gap")
    if a is None or b is None or base is None or gap is None or a <= 0 or b <= 0:
        return None
    below = float(betainc(a, b, min(max(float(base), 0.0), 1.0)))  # P(theta < base)
    return round(below if gap < 0 else 1.0 - below, 4)


def _gap(r: dict) -> float:
    return abs(r["league_gap"]) if r.get("league_gap") is not None else -1.0


def _rate(r: dict) -> float:
    return r["k"] / r["n"]


def _confidence(r: dict) -> float | None:
    conf = r.get("league_confidence")
    return conf if conf is not None else league_confidence(r)


def _team_event(event: str) -> bool:
    return event in RESULT_EVENTS or event in ATTACK_DEFENCE_EVENTS


def _qualifies(r: dict) -> bool:
    if (r.get("event") in EXCLUDED_EVENTS or (r.get("n") or 0) < MIN_N
            or (r.get("k") or 0) <= 0):
        return False
    if _team_event(r["event"]):
        # Above the league only: a below-league team record is the mirror of
        # an above-league one already shown (Won 2/10 <-> Winless 8/10).
        if r.get("league_comparison") in NO_COMPARISON or (r.get("league_gap") or 0) <= 0:
            return False
        conf = _confidence(r)
        return conf is not None and conf >= TOP_CONFIDENCE
    return bool(r.get("display_eligible"))


def _top_ok(r: dict) -> bool:
    """Team events qualify on confidence already; match goals never top."""
    return _team_event(r["event"]) and _qualifies(r)


def _top_key(r: dict):
    return (_gap(r), _confidence(r) or 0.0, r["n"])


def _dedupe_ladders(rows: list[dict]) -> list[dict]:
    best: dict[tuple, dict] = {}
    out = []
    for r in rows:
        key = ladder(r["event"])
        if key is None:
            out.append(r)
            continue
        k = (r["side"], key)
        cur = best.get(k)
        if cur is None or (_gap(r), _rate(r)) > (_gap(cur), _rate(cur)):
            best[k] = r
    return out + list(best.values())


def comparison_label(r: dict) -> str | None:
    """League comparison wording for one record, decided by evidence (D126)."""
    current = r.get("league_comparison")
    gap = r.get("league_gap")
    if current in NO_COMPARISON or gap is None:
        return current                      # too few league matches / unavailable
    conf = _confidence(r)
    if conf is not None and conf >= TOP_CONFIDENCE and gap != 0:
        much = abs(gap) >= WELL_GAP
        if gap > 0:
            return "well_above" if much else "above"
        return "well_below" if much else "below"
    if abs(gap) < TYPICAL_MAX_GAP and (r.get("n") or 0) >= TYPICAL_MIN_N:
        return "typical"
    return "inconclusive"


def _public(r: dict) -> dict:
    return {
        "side": r["side"], "team": r.get("team"), "event": r["event"],
        "label": label(r["event"]), "k": r["k"], "n": r["n"],
        "rate": round(_rate(r), 4),
        "league_rate": r.get("base_rate"),
        "league_comparison": comparison_label(r),
        "streak": r.get("streak") or 0,
        "context": r.get("context"),
    }


def _pick_attack_defence(cands: list[dict]) -> list[dict]:
    """Best corresponding pair across the two teams, else the best single."""
    by_side_event = {(r["side"], r["event"]): r for r in cands}
    best_pair, best_score = None, None
    for r in cands:
        if r["side"] != "home":
            continue
        other = by_side_event.get(("away", CORRESPONDING.get(r["event"])))
        if other is None:
            continue
        score = (_gap(r) + _gap(other), min(_top_key(r), _top_key(other)))
        if best_score is None or score > best_score:
            best_pair, best_score = [r, other], score
    if best_pair:
        return best_pair
    return [max(cands, key=_top_key)] if cands else []


def select_view(rows: list[dict]) -> dict:
    """Top Results, Top Attack/defence, Also and See all for one view."""
    qualified = [r for r in rows if _qualifies(r)]

    # Tops are picked before ladders collapse, so a corresponding pair
    # (e.g. Conceded 1+ <-> Scored 1+) survives even if a higher rung exists.
    top_results = []
    for side in ("home", "away"):
        cands = [r for r in qualified
                 if r["side"] == side and r["event"] in RESULT_EVENTS and _top_ok(r)]
        if cands:
            top_results.append(max(cands, key=_top_key))

    ad_cands = [r for r in qualified if r["event"] in ATTACK_DEFENCE_EVENTS and _top_ok(r)]
    top_ad = _pick_attack_defence(ad_cands)

    tops = top_results + top_ad
    used = {id(r) for r in tops}
    used_ladders = {(r["side"], ladder(r["event"])) for r in tops if ladder(r["event"])}
    rest = _dedupe_ladders([r for r in qualified if id(r) not in used
                            and (r["side"], ladder(r["event"])) not in used_ladders])
    rest.sort(key=lambda r: (_rate(r), r["n"], _gap(r)), reverse=True)
    return {
        "top_results": [_public(r) for r in top_results],
        "top_attack_defence": [_public(r) for r in top_ad],
        "also": [_public(r) for r in rest[:ALSO_MAX]],
        "see_all": [_public(r) for r in rest[ALSO_MAX:]],
    }


def _venue_rows(rows: list[dict]) -> list[dict]:
    return [r for r in rows if r.get("context") == r.get("side")]


def select_patterns(bayesian_rows: list[dict]) -> dict:
    """Both views plus the view the page opens on (the engine's primary)."""
    rows = [r for r in bayesian_rows if r.get("event") and r.get("side") in ("home", "away")]
    venue, overall = _venue_rows(rows), [r for r in rows if r.get("context") == "all"]
    primary_venue = {r["side"] for r in rows if r.get("primary") and r.get("context") == r.get("side")}
    default = "venue" if primary_venue == {"home", "away"} else "all"
    return {
        "default_view": default,
        "venue": select_view(venue) if venue else None,
        "all": select_view(overall),
    }


def attach_team_patterns(analysis: list[dict]) -> None:
    """Add league_confidence to each pattern row and fixture['patterns']."""
    for fixture in analysis:
        rows = fixture.get("bayesian") or []
        for r in rows:
            r["league_confidence"] = league_confidence(r)
        fixture["patterns"] = select_patterns(rows)
