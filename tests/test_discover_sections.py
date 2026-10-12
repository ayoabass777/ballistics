"""D121: each Discover section ranks fixtures on their best item in that section."""
from datetime import datetime, timezone

from engine.eval.consensus_slips import (
    attach_discover_picks, section_picks, select_rolling_sections,
)

NOW = datetime(2026, 10, 7, 12, tzinfo=timezone.utc)


def pat(team, event, k, n, base, comp="well_above"):
    return {"team": team, "event": event, "context": "all", "k": k, "n": n,
            "base_rate": base, "league_comparison": comp}


def dc(market, p, gap, comp="well_above"):
    return {"market": market, "probability": p, "league_gap": gap,
            "league_comparison": comp}


def fx(fid, kickoff="2026-10-08T12:00:00Z", **kw):
    base = {"fixture_id": fid, "home": "A", "away": "B", "league": "L", "country": "C",
            "kickoff": kickoff, "dixon_coles": [], "bayesian": [], "mismatch": []}
    return {**base, **kw}


def test_fixture_appears_in_every_section_it_qualifies_for():
    # Goals wins overall, but the fixture still gets its best pattern in Patterns.
    f = fx(1, dixon_coles=[dc("B Over 1.5", 0.80, 0.40)],
           bayesian=[pat("B", "win", 9, 10, 0.35), pat("A", "loss", 7, 10, 0.35)])
    attach_discover_picks([f], [], None, {})
    assert f["discover_pick"]["list"] == "goals"
    assert set(f["discover_sections"]) == {"goals", "patterns"}
    assert f["discover_sections"]["patterns"]["event"] == "win"   # 9/10 beats 7/10


def test_goals_section_uses_65_floor_but_standout_needs_70():
    f = fx(1, dixon_coles=[dc("B Over 0.5", 0.67, 0.30)])
    attach_discover_picks([f], [], None, {})
    assert f["discover_pick"] is None                      # below the 0.70 standout floor
    assert f["discover_sections"]["goals"]["market"] == "B Over 0.5"


def test_section_needs_score_above_zero_not_the_standout_floor():
    picks = section_picks([{"list": "patterns", "score": 0.05, "team": "A"},
                           {"list": "goals", "score": -0.02}], None, None)
    assert set(picks) == {"patterns"}


def test_ties_go_to_the_favourites_side():
    picks = section_picks([{"list": "patterns", "score": 0.2, "team": "A"},
                           {"list": "patterns", "score": 0.2, "team": "B"}], "B", "B")
    assert picks["patterns"]["team"] == "B" and picks["patterns"]["big_dog"] == "B"


def test_section_lists_rank_by_their_own_score_then_kickoff():
    data = []
    for fid, s, ko in [(1, 0.10, "2026-10-09T12:00:00Z"), (2, 0.30, "2026-10-09T12:00:00Z"),
                       (3, 0.10, "2026-10-08T12:00:00Z"), (4, 0.50, "2026-10-06T12:00:00Z")]:
        data.append(fx(fid, ko, discover_sections={"patterns": {"list": "patterns", "score": s}}))
    out = select_rolling_sections(data, now=NOW)
    assert [r["fixture_id"] for r in out["patterns"]["items"]] == [2, 3, 1]   # 4 already started
    assert out["goals"]["count"] == 0 and out["strength"]["count"] == 0


def test_same_window_as_standouts():
    data = [fx(1, "2026-10-14T00:00:00Z", discover_sections={"goals": {"list": "goals", "score": .2}})]
    out = select_rolling_sections(data, now=NOW)
    assert out["end_exclusive"] == "2026-10-14T00:00:00+00:00" and out["goals"]["count"] == 0
