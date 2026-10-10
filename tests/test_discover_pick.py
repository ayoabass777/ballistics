"""D71 Discover pick: one card per fixture, scored on one scale across lists."""
import pytest

from engine.eval.consensus_slips import (
    _build_elo_gap_shortlist,
    aggregate_elo_gap_group,
    league_big_dogs_trusted,
    wilson_lower,
    attach_discover_picks,
    elo_group_win_rate,
    league_win_rate,
)

LG = "Czech Liga (Czech-Republic)"
RATES = {(LG, "win", "all"): {"rate": 0.394, "rows": 288},
         (LG, "win", "home"): {"rate": 0.45, "rows": 144}}
GROUP = {"win_rate": 0.753, "matches": 2943, "bands": [
    {"band": "250-300", "min": 250, "max": 300, "matches": 1200, "win_rate": 0.70},
    {"band": "300-400", "min": 300, "max": 400, "matches": 1300, "win_rate": 0.76},
    {"band": "400+", "min": 400, "max": None, "matches": 60, "win_rate": 0.90},
], "by_league": [
    {"league": "Czech Liga", "country": "Czech-Republic", "matches": 200, "won": 160,
     "win_rate": 0.80, "win_rate_low": 0.75, "meets_min": True},
    {"league": "Super League", "country": "China", "matches": 12, "won": 11,
     "win_rate": 0.917, "win_rate_low": 0.70, "meets_min": False},
]}


def fixture(**kw):
    base = {"fixture_id": 1, "home": "Zlin", "away": "Slavia Praha",
            "league": "Czech Liga", "country": "Czech-Republic",
            "dixon_coles": [], "bayesian": [], "mismatch": []}
    return {**base, **kw}


def pattern(team, event, k, n, base, comparison="well_above", eligible=True):
    return {"team": team, "event": event, "context": "all", "k": k, "n": n,
            "base_rate": base, "league_comparison": comparison,
            "display_eligible": eligible}


def test_band_rate_used_and_thin_band_falls_back():
    assert elo_group_win_rate(320, GROUP) == (0.76, "band 300-400")
    assert elo_group_win_rate(501, GROUP) == (0.753, "all big dogs")  # 60 < 100
    assert elo_group_win_rate(300, GROUP) == (0.70, "band 250-300")   # upper inclusive
    assert elo_group_win_rate(320, None) == (None, "unavailable")


def test_league_win_rate_falls_back_to_all_games():
    fx = fixture()
    assert league_win_rate(fx, "Zlin", RATES)["context"] == "home"
    away = league_win_rate(fx, "Slavia Praha", RATES)
    assert (away["context"], away["rate"]) == ("all", 0.394)
    assert league_win_rate(fx, "Nobody", {}) is None


def test_strength_beats_goals_when_its_gap_is_larger():
    fx = fixture(dixon_coles=[{"market": "Slavia Praha Over 1.5", "probability": 0.72,
                               "league_gap": 0.312, "league_comparison": "well_above"}])
    short = [{"fixture_id": 1, "favourite": "Slavia Praha", "underdog": "Zlin",
              "elo_gap": 501}]
    attach_discover_picks([fx], short, GROUP, RATES)
    pick = fx["discover_pick"]
    # scored on the Czech Liga's own record (lower bound 0.75), not the pooled 0.753
    assert pick["list"] == "strength" and pick["score"] == pytest.approx(0.75 - 0.394)
    assert short[0]["league_big_dog_matches"] == 200 and short[0]["league_record_ok"]
    assert pick["big_dog"] == "Slavia Praha"
    assert pick["runner_up"] == {"list": "goals", "score": round(0.312 * 0.76, 4)}
    assert short[0]["discover_score"] == pick["score"]
    assert short[0]["league_win_context"] == "all"


def test_small_pattern_samples_are_shrunk_below_larger_records():
    fx = fixture(bayesian=[pattern("Zlin", "winless", 9, 9, 0.6056),
                           pattern("Zlin", "concede_1", 28, 30, 0.60)])
    attach_discover_picks([fx], [], None, RATES)
    # 15-match prior: 9/9 raw gap +39 → +14.8; 28/30 raw +33 → +22.2
    assert fx["discover_pick"]["event"] == "concede_1"
    assert fx["discover_pick"]["score"] == pytest.approx((28 + 9) / 45 - 0.60, abs=1e-4)


def test_mirror_tie_goes_to_the_favourite():
    fx = fixture(bayesian=[pattern("Zlin", "winless", 9, 9, 0.59),
                           pattern("Slavia Praha", "unbeaten", 9, 9, 0.59)],
                 mismatch=[{"market": "Slavia Praha Win", "gap": 501}])
    attach_discover_picks([fx], [], None, RATES)
    assert fx["discover_pick"]["team"] == "Slavia Praha"


def test_ineligible_rows_are_skipped_and_no_standout_gives_no_card():
    fx = fixture(
        dixon_coles=[
            {"market": "Slavia Praha Win", "probability": 0.73, "league_gap": 0.39,
             "league_comparison": "well_above"},           # DC win never shown
            {"market": "Match Over 2.5", "probability": 0.60, "league_gap": 0.10,
             "league_comparison": "above"},                # below 65% bar
            {"market": "Match Over 1.5", "probability": 0.80, "league_gap": -0.02,
             "league_comparison": "typical"}],
        bayesian=[pattern("Zlin", "loss", 5, 5, 0.3, "too_few_matches"),
                  pattern("Zlin", "draw", 1, 9, 0.2, eligible=False)])
    attach_discover_picks([fx], [], None, RATES)
    assert fx["discover_pick"] is None


def test_group_rows_aggregate_per_band_and_skip_volatile_leagues():
    rows = [("A", "X", 100, 70, 20, "250-300"), ("A", "X", 50, 40, 5, "400+"),
            ("B", "Y", 999, 1, 1, "300-400"), ("C", "Z", 10, 9, 0, "250-300")]
    g = aggregate_elo_gap_group(rows, {("A", "X"), ("C", "Z")}, 250, 0.27)
    assert (g["matches"], g["won"], g["drew"], g["lost"]) == (160, 119, 25, 16)
    lg = {r["league"]: r for r in g["by_league"]}
    assert set(lg) == {"A", "C"}                       # volatile league B excluded
    assert lg["A"]["matches"] == 150 and lg["A"]["meets_min"]
    assert lg["C"]["win_rate"] == 0.9 and not lg["C"]["meets_min"]
    assert lg["C"]["win_rate_low"] < lg["C"]["win_rate"]
    bands = {b["band"]: b for b in g["bands"]}
    assert bands["250-300"]["matches"] == 110 and bands["300-400"]["matches"] == 0
    assert bands["400+"]["win_rate"] == 0.8


def test_thin_league_record_gets_no_strength_score():
    fx = fixture(league="Super League", country="China", home="Qingdao", away="Beijing")
    short = [{"fixture_id": 1, "favourite": "Beijing", "underdog": "Qingdao", "elo_gap": 320}]
    rates = {("Super League (China)", "win", "away"): {"rate": 0.27, "rows": 200}}
    attach_discover_picks([fx], short, GROUP, rates)
    assert fx["discover_pick"] is None               # 11 of 12 is not enough history
    assert short[0]["league_big_dog_matches"] == 12 and not short[0]["league_record_ok"]
    assert short[0]["discover_score"] is None


def test_wilson_lower_is_pessimistic_and_tightens_with_sample():
    assert wilson_lower(9, 10) < 0.9
    assert wilson_lower(90, 100) > wilson_lower(9, 10)
    assert wilson_lower(0, 0) == 0.0


def test_big_dog_label_needs_sixty_percent_lower_bound():
    assert league_big_dogs_trusted({"meets_min": True, "win_rate_low": 0.68})
    assert not league_big_dogs_trusted({"meets_min": True, "win_rate_low": 0.466})  # Belgium
    assert not league_big_dogs_trusted({"meets_min": False, "win_rate_low": 0.72})
    assert not league_big_dogs_trusted(None)


def test_shortlist_drops_untrusted_leagues_only_when_record_is_loaded():
    def fx(fid, league, country):
        return {"fixture_id": fid, "home": f"H{fid}", "away": f"F{fid}", "league": league,
                "country": country, "kickoff": "2026-10-10 12:00:00+00:00",
                "mismatch": [{"market": f"F{fid} Win", "gap": 320, "probability": 0.8}]}
    vol = {("Czech Liga", "Czech-Republic"): {"matches": 500, "upset_rate": 0.24},
           ("Jupiler Pro League", "Belgium"): {"matches": 500, "upset_rate": 0.25}}
    group = {"by_league": [
        {"league": "Czech Liga", "country": "Czech-Republic", "meets_min": True, "win_rate_low": 0.68},
        {"league": "Jupiler Pro League", "country": "Belgium", "meets_min": True, "win_rate_low": 0.466}]}
    fixtures = [fx(1, "Czech Liga", "Czech-Republic"), fx(2, "Jupiler Pro League", "Belgium")]
    assert [p["fixture_id"] for p in _build_elo_gap_shortlist(fixtures, vol, group=group)] == [1]
    assert len(_build_elo_gap_shortlist(fixtures, vol)) == 2   # no record → old rule


def test_historical_pattern_does_not_need_prediction_display_gate():
    fx = fixture(bayesian=[pattern("Zlin", "score_2plus", 7, 7, 0.4, eligible=False)])
    attach_discover_picks([fx], [], None, RATES)
    assert fx["discover_pick"]["event"] == "score_2plus"
    assert fx["discover_pick"]["score"] == pytest.approx(0.6 * 7 / 22, abs=1e-4)


def test_historical_pattern_still_requires_sample_and_baseline():
    fx = fixture(bayesian=[pattern("Zlin", "score_2plus", 5, 5, 0.1, eligible=False),
                           pattern("Zlin", "unbeaten", 9, 9, None, eligible=False)])
    attach_discover_picks([fx], [], None, RATES)
    assert fx["discover_pick"] is None


@pytest.mark.parametrize("section", ["strength", "goals", "patterns"])
@pytest.mark.parametrize("score,qualifies", [(0.14999, False), (0.15, True), (0.15001, True)])
def test_standout_cutoff_is_inclusive_before_rounding(monkeypatch, section, score, qualifies):
    monkeypatch.setattr("engine.eval.consensus_slips.discover_candidates",
                        lambda *args: [{"list": section, "score": score}])
    fixtures = [fixture(fixture_id=i) for i in range(7)]
    attach_discover_picks(fixtures, [], None, RATES)
    # Seven qualifying fixtures in the same section are all retained: no quota/cap.
    assert sum(f["discover_pick"] is not None for f in fixtures) == (7 if qualifies else 0)


def test_below_cutoff_pattern_remains_a_section_candidate():
    from engine.eval.consensus_slips import discover_candidates
    fx = fixture(bayesian=[pattern("Zlin", "winless", 9, 9, 0.6056)])
    assert discover_candidates(fx, None, None, RATES)[0]["score"] > 0
    attach_discover_picks([fx], [], None, RATES)
    assert fx["discover_pick"] is None


# ── D81: no upset-rate gate, buffer, top flight only ─────────────────────
from engine.eval.consensus_slips import (  # noqa: E402
    big_dog_league_in, classify_big_dog_leagues, load_big_dog_state, save_big_dog_state)


def _rec(league, country, low, n=60):
    return {"league": league, "country": country, "matches": n, "won": int(n * 0.75),
            "win_rate": 0.75, "win_rate_low": low, "meets_min": n >= 30}


def test_classify_buffer_tier_and_floor():
    recs = [_rec("Eliteserien", "Norway", 0.656), _rec("Liga MX", "Mexico", 0.59),
            _rec("Premier League", "Belarus", 0.58), _rec("Premier League", "Belarus2", 0.55),
            _rec("Eerste Divisie", "Netherlands", 0.615), _rec("Virsliga", "Latvia", 0.80),
            _rec("Nowhere League", "Atlantis", 0.9), _rec("Superliga", "Denmark", 0.7, n=19)]
    tiers = {("Eliteserien", "Norway"): 1, ("Liga MX", "Mexico"): 1,
             ("Premier League", "Belarus"): 1, ("Premier League", "Belarus2"): 1,
             ("Eerste Divisie", "Netherlands"): 2}
    prev = {"Premier League (Belarus)", "Premier League (Belarus2)"}
    got = {(r["league"], r["country"]): r for r in classify_big_dog_leagues(recs, tiers, prev)}
    assert got[("Eliteserien", "Norway")]["status"] == "in"
    assert got[("Liga MX", "Mexico")]["status"] == "out"            # 59% can't join
    assert got[("Premier League", "Belarus")]["held"]               # 58% stays (was in)
    assert got[("Premier League", "Belarus2")]["status"] == "out"   # 55% < 57% leaves
    assert "tier 2" in got[("Eerste Divisie", "Netherlands")]["reason"]
    assert got[("Virsliga", "Latvia")]["status"] == "in"            # tier from overrides
    assert got[("Nowhere League", "Atlantis")]["reason"] == "tier unknown"
    assert got[("Superliga", "Denmark")]["status"] == "out"


def test_shortlist_ignores_upset_rate_when_classified():
    fx = {"fixture_id": 7, "home": "Kristiansund", "away": "Bodo/Glimt", "league": "Eliteserien",
          "country": "Norway", "kickoff": "2026-10-10 14:00:00+00:00",
          "mismatch": [{"market": "Bodo/Glimt Win", "gap": 332, "probability": 0.8}]}
    vol = {("Eliteserien", "Norway"): {"matches": 1517, "upset_rate": 0.271}}
    group = {"by_league": [], "big_dog_leagues": [
        {"league": "Eliteserien", "country": "Norway", "status": "in"}]}
    assert [p["favourite"] for p in _build_elo_gap_shortlist([fx], vol, group=group)] == ["Bodo/Glimt"]
    assert _build_elo_gap_shortlist([fx], vol) == []        # fallback keeps the 27% rule
    assert big_dog_league_in(group, "Eliteserien", "Norway")


def test_state_round_trip(tmp_path):
    path = tmp_path / "state.json"
    assert load_big_dog_state(path) == set()
    save_big_dog_state({"big_dog_leagues": [
        {"league": "Ligue 1", "country": "France", "status": "in"},
        {"league": "Liga MX", "country": "Mexico", "status": "out"}]}, path)
    assert load_big_dog_state(path) == {"Ligue 1 (France)"}


def test_goals_standout_needs_seventy_percent():
    fx = fixture(dixon_coles=[
        {"market": "Kryvbas KR Over 1.5", "probability": 0.6686, "league_gap": 0.3856,
         "league_comparison": "well_above"},
        {"market": "Match Over 1.5", "probability": 0.70, "league_gap": 0.21,
         "league_comparison": "well_above"}])
    attach_discover_picks([fx], [], None, RATES)
    assert fx["discover_pick"]["market"] == "Match Over 1.5"   # 67% row is below the bar
