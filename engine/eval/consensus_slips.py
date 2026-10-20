"""
Consensus engine — multi-model pick pool, shortlists and the daily feed.

Runs three engines (Mismatch Elo, Bayesian Streaks, Dixon-Coles; the Poisson
GLM was retired 2026-10-11), maps every qualifying pick to a unified market,
scores each pick by how many models agree, and publishes the pool, the curated
shortlists and per-fixture analysis (Discover, match pages) as
consensus_slips_<ds>.json. Betting slips, SportyBet odds and EV were removed
on 2026-10-20; the module and file names are kept for the existing readers.

Model-to-market mapping:
    Mismatch fav_to_score                →  Team Over 0.5 (fav side)
    Mismatch over_1.5 / over_2.5         →  Match Over 1.5 / Over 2.5
    Bayesian Over_1_5 / Over_2_5 / etc.  →  Match Over 1.5 / Over 2.5
    Bayesian score_1goal                 →  Team Over 0.5
    Bayesian concede_1                   →  Opponent Over 0.5
    Bayesian Under_2_5 / Under_3_5       →  Match Under 2.5 / Under 3.5
    Dixon-Coles (markets already unified) →  Match/Team O/U, BTTS Yes, Win

Consensus tiers (n = distinct models agreeing, 1-4):
    3+ models →  backbone (trio+ — three or more agree)
    2-model  →  strong (two independent signals)
    1-model  →  solo (only if P >= solo_threshold)

Goal-model rules (walk-forward validated, folds 5-9):
    - Match Over 1.5 requires Dixon-Coles (previously DC AND Poisson),
      with calibrated consensus P >= 78%.
    - Match Over 2.5 has no model-combination restriction; any contributing
      model can qualify it through the normal consensus thresholds.
    - Under selections require Dixon-Coles (previously DC AND Poisson).
    - Other solo goal-model match-total picks are dropped (DC solo unders 53.3%).
    - BTTS Yes is sourced from Dixon-Coles only; Bayesian BTTS is ignored.

Usage:
    python -m engine.eval.consensus_slips
    python -m engine.eval.consensus_slips --days 10
    python -m engine.eval.consensus_slips --from-picks --run-ds 2026-10-20
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone, timedelta, time
from zoneinfo import ZoneInfo
from pathlib import Path

import numpy as np
import pandas as pd

from engine.eval.team_patterns import attach_team_patterns
from engine.eval.predict_upcoming import _tier
from engine.eval.goals_league_blend import blend_probability
from engine.eval.predict_mismatch import predict_mismatch, _market_label
from engine.eval.predict_dixon_coles import predict_dixon_coles
from engine.eval.db import get_conn
from engine.eval.team_strength import load_team_df, compute_team_strength
from engine.eval.bayesian_streaks import (
    get_fixture_beliefs,
    load_event_history,
    compute_league_base_rates,
    compute_beliefs,
    BETTING_EVENTS,
    PRIOR_STRENGTH,
    MIN_POSTERIOR_INTERESTING,
)

SOLO_THRESHOLD = 0.80
BOUNDS_SOLO_THRESHOLD = 0.78
BOUNDS_NEAR_THRESHOLD_FLOOR = 0.70
DUO_THRESHOLD = 0.72
TRIO_THRESHOLD = 0.70
OVER15_DC_POISSON_THRESHOLD = 0.78
UNDER35_MAX_AVG_ATTACK_PCT = 55.0
UNDER35_MAX_SINGLE_ATTACK_PCT = 75.0
UNDER35_MIN_AVG_DEFENSE_PCT = 55.0
UNDER35_MIN_SINGLE_DEFENSE_PCT = 35.0
UNDER35_TIER_A_THRESHOLD = 0.80
UNDER35_TIER_B_THRESHOLD = 0.75
UNDER35_TIER_C_THRESHOLD = 0.72

# Conservative ranking-only priors for the most dependable Goal Bounds 1-4
# leagues in the 2020-2025 history.  These do not alter model probability
# or eligibility gates: they only break ties/order already-qualified picks.
# The score bonus is the league's excess hit rate over the global rate, shrunk
# toward zero with a 1,000-match prior so thin or volatile samples cannot
# dominate the model consensus.
BOUNDS_1_4_GLOBAL_RATE = 0.7839
BOUNDS_1_4_PRIOR_STRENGTH = 1_000
BOUNDS_1_4_MAX_SCORE_BOOST = 0.04
BOUNDS_1_4_MIN_HISTORY = 50
BOUNDS_1_4_TOP_RATE = 0.80
BOUNDS_1_4_SHORTLIST_LIMIT = 40
BOUNDS_1_4_PER_LEAGUE_LIMIT = 7
ELO_HUGE_GAP_MIN = 250  # Big dog: gap strictly above this (D27/D44, 2026-10-02; was >= 300)
# D137: the Big dog gap is venue-adjusted: (home Elo + BIG_DOG_HOME_EDGE) - away Elo.
# Flat gaps treated home and away favourites alike although away favourites won
# 10 pts less (71% vs 81%, 3 seasons top flight). With +65 both win ~77% and Big
# dogs rise 2,189 -> 2,628 (engine/eval/diagnostics/strength_home_adv.py).
BIG_DOG_HOME_EDGE = 65
# D138: slight favourites (venue-adjusted gap 100 < g <= 250) split in two levels.
# 3 seasons top flight: 100-175 won 51% (drew 26%), 175-250 won 62% (drew 23%).
# Shown as a label only (owner: no "x in 10" on strength).
SLIGHT_FAV_MIN_GAP = 100          # at or below: "close" (flat gap, D27)
SLIGHT_EDGE_MAX_GAP = 175         # 100-175 "slight edge", 175-250 "favourite"
# D138: league tiers (Top / Middle / Bottom) as match-page context only. k-means
# k=3 on current flat Elo of the league's latest season; at the same gap, tier
# matchups changed slight-favourite win rates by <= 5 pts, so no numbers of
# their own (engine/eval/diagnostics/tier_slight_fav.py, tier_context.py).
TIER_MIN_TEAMS = 9
TIER_MIN_GAMES = 3
TIER_NAMES = ("Bottom", "Middle", "Top")
ELO_LOW_VOLATILITY_MAX = 0.27
ELO_VOLATILITY_MIN_HISTORY = 50
ELO_GAP_SHORTLIST_LIMIT = 40
ELO_GAP_PER_LEAGUE_LIMIT = 7
BOUNDS_1_4_LEAGUE_PRIORS = {
    ("Serie B", "Brazil"): {"rate": 0.8460, "matches": 1_156},
    ("Serie A", "Brazil"): {"rate": 0.8298, "matches": 2_280},
    ("Premier Division", "Ireland"): {"rate": 0.8270, "matches": 994},
    ("Championship", "England"): {"rate": 0.8206, "matches": 2_469},
    ("Premiership", "Scotland"): {"rate": 0.8036, "matches": 1_110},
}

# In high-scoring leagues Poisson overpredicts unders (+4.4% walk-forward gap,
# because its per-cluster fit pools lower-scoring leagues) while Dixon-Coles —
# fit per league — stays calibrated (+0.6%). So Match Under markets in these
# leagues are priced off DC alone instead of the equal DC+Poisson blend.
HIGH_SCORING_AVG_GOALS = 3.1     # league goals/game above which to favour DC on unders
UNDER_HIGH_DC_WEIGHT = 1.0       # weight on DC's prob there (1.0 = DC-only)

def _load_bounds_1_4_league_priors(
        conn, min_matches: int = BOUNDS_1_4_MIN_HISTORY) -> dict:
    """Fresh 1–4 hit rates by league, using one home row per played fixture."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                f.league_name AS league,
                COALESCE(rf.country_name, '') AS country,
                COUNT(*) AS matches,
                AVG(CASE
                    WHEN f.goals_for + f.goals_against BETWEEN 1 AND 4
                    THEN 1.0 ELSE 0.0
                END) AS hit_rate
            FROM raw.engine_head_to_head f
            JOIN raw.fact_fixtures rf ON rf.fixture_id = f.fixture_id
            WHERE f.is_played AND f.is_home
            GROUP BY f.league_name, rf.country_name
            HAVING COUNT(*) >= %s
        """, (min_matches,))
        return {
            (league, country or ""): {
                "rate": float(rate),
                "matches": int(matches),
            }
            for league, country, matches, rate in cur.fetchall()
        }


def _load_league_elo_volatility(
        conn, min_matches: int = ELO_VOLATILITY_MIN_HISTORY) -> dict:
    """League upset rates keyed by league and country using pre-match flat Elo."""
    with conn.cursor() as cur:
        cur.execute("""
            WITH matches AS (
                SELECT
                    f.league_name AS league,
                    COALESCE(rf.country_name, '') AS country,
                    f.goals_for AS home_goals,
                    f.goals_against AS away_goals,
                    COALESCE(eh.rating_before_flat, eh.rating_before) AS home_elo,
                    COALESCE(ea.rating_before_flat, ea.rating_before) AS away_elo
                FROM raw.engine_head_to_head f
                JOIN raw.fact_fixtures rf ON rf.fixture_id = f.fixture_id
                JOIN elo.team_elo_history eh
                  ON eh.fixture_id = f.fixture_id AND eh.team_id = f.team_id
                JOIN elo.team_elo_history ea
                  ON ea.fixture_id = f.fixture_id AND ea.team_id = f.opponent_id
                WHERE f.is_played AND f.is_home
            )
            SELECT
                league,
                country,
                COUNT(*) AS matches,
                AVG(CASE
                    WHEN home_elo < away_elo AND home_goals > away_goals THEN 1.0
                    WHEN away_elo < home_elo AND away_goals > home_goals THEN 1.0
                    ELSE 0.0
                END) AS upset_rate
            FROM matches
            WHERE home_elo != away_elo
            GROUP BY league, country
            HAVING COUNT(*) >= %s
        """, (min_matches,))
        return {
            (league, country or ""): {
                "upset_rate": float(upset_rate),
                "matches": int(matches),
            }
            for league, country, matches, upset_rate in cur.fetchall()
        }


def _bounds_1_4_league_prior(unified_market: str, league: str,
                             country: str = "",
                             league_priors: dict | None = None) -> dict | None:
    """Return a transparent, reliability-shrunk ranking prior when eligible."""
    if unified_market != "Match Goals 1-4":
        return None
    prior = (league_priors or BOUNDS_1_4_LEAGUE_PRIORS).get(
        (league, country or ""))
    if not prior:
        return None
    excess = max(0.0, prior["rate"] - BOUNDS_1_4_GLOBAL_RATE)
    reliability = prior["matches"] / (prior["matches"] + BOUNDS_1_4_PRIOR_STRENGTH)
    boost = min(excess * reliability, BOUNDS_1_4_MAX_SCORE_BOOST)
    return {
        "rate": prior["rate"],
        "matches": prior["matches"],
        "boost": boost,
    }

# ── League comparison (D57) ─────────────────────────────────────────────
# Each user-facing number is compared with how often the same event happened
# in the same league this season. Rates come from the Bayesian league table
# (raw.fact_head_to_head_events, current season). For match events every
# fixture appears once per team in context "all", so fixtures = rows // 2.
LEAGUE_RATE_MIN_MATCHES = 20      # fewer fixtures → "too_few_matches"
PATTERN_COMPARE_MIN_N = 6         # team record needs this many matches
LEAGUE_GAP_WELL = 0.15            # ≥ +15 pts → well_above
LEAGUE_GAP_SOME = 0.05            # ≥ +5 pts → above; within ±5 → typical

# D71 Discover pick: one card per fixture. Every candidate from the three
# lists is scored in the same unit, points above what is normal in its league:
#   goals    = (calibrated DC probability − league rate) × DISCOVER_GOALS_WEIGHT
#   patterns = record shrunk toward the league rate with a 15-match prior,
#              minus the league rate (so 9 of 9 counts for less than 28 of 30)
# Weights come from the walk-forward check (diagnostics/discover_weight_check.py,
# Jun–Oct 2026, 4,828 fixtures): goals gaps came true at 0.76 of their claimed
# size; patterns with a 10-match prior at 0.47, and with 20 the top decile
# claimed +11.7 pts against +11.1 realised. D77 subsequently selects 15;
# that earlier validation does not establish performance for the new setting.
#   strength = Big dog win rate for its gap band − league win rate for the
#              favourite's venue (all-games rate when the venue split is missing)
DISCOVER_DC_MIN_PROB = 0.70  # standouts only (owner, 2026-10-07)
DISCOVER_SECTION_DC_MIN_PROB = 0.75  # Owner, 2026-10-17: Discover Goals section
DISCOVER_PATTERN_PRIOR_MATCHES = 15  # D77: historical ranking; separate from prediction rating
# D129: the prior is set per (league, event, venue context) from how much that
# league's teams really differ on the event (empirical Bayes, method of
# moments): M = p(1-p)/tau^2 - 1, tau^2 = observed variance of team rates minus
# luck variance. Clamped to [FLOOR, CAP]; DISCOVER_PATTERN_PRIOR_MATCHES is the
# fallback when too few teams. Walk-forward: standout-size claims realised 0.80
# of their size (fixed 15: 0.74); engine/eval/diagnostics/pattern_shrink_variable.py.
PATTERN_PRIOR_FLOOR = 15
PATTERN_PRIOR_CAP = 30
PATTERN_PRIOR_MIN_TEAMS = 10
PATTERN_PRIOR_MIN_GAMES = 5
DISCOVER_GOALS_WEIGHT = 1.0  # D128: was 0.76 (D71); the D127 league blend now makes goals gaps honest (re-measured 1.01 at 70%+)
STANDOUT_MIN_SCORE = 0.15  # D78: inclusive adjusted percentage-point gap
ELO_GAP_BANDS = ((250, 300), (300, 400), (400, None))   # (lower excl., upper incl.)
ELO_BAND_MIN_MATCHES = 100        # thinner band → fall back to the overall group rate
# D136 (shadow, not user-facing): Dixon-Coles cross-check on Big dogs. Backtest
# Jun-Oct 2026 (engine/eval/diagnostics/strength_dc_check.py): favourites DC
# also backs at >= 65% won 76% vs 69% for the rest; ranges overlap, so it is
# logged to raw.big_dog_watch and reported in the daily data email until proven.
BIG_DOG_DC_CONFIRM = 0.65
# Strength trust (2026-10-05 trial): each league is scored on its OWN past Big dog
# record (gap > 250, all seasons in the DB), never the pooled 30-league rate. A
# league needs this many past Big dog matches before its Big dogs get a score,
# and the score uses the pessimistic end of its win rate (Wilson 90% lower bound)
# so a thin record cannot take the top slot.
ELO_LEAGUE_MIN_MATCHES = 30
ELO_LEAGUE_Z = 1.645
# A "Big dog" label is a claim the user sees, so a league only gets Big dogs
# (Strength section and standouts) when its own record's lower bound is at least
# this. Owner decision 2026-10-05; drops Belgium, Finland, Austria, Ireland
# Premier Division on the Oct 5 records.
ELO_LEAGUE_MIN_LOW = 0.60
# D81 (trial): the 27% league upset-rate rule no longer gates Big dogs; each league's
# own Big dog record decides, with a buffer so leagues on the line don't flip daily:
# a league JOINS at lower bound ≥ 60% and, once in, only LEAVES below 57%.
# Big dogs are top-flight only (tier 1). Membership is remembered between runs in
# BIG_DOG_STATE_PATH.
ELO_LEAGUE_STAY_LOW = 0.57
BIG_DOG_STATE_PATH = Path(__file__).parent / "big_dog_leagues_state.json"
# Tier for leagues whose dim.dim_leagues.tier is empty (not in data/metadata.yaml).
# Unknown tier → no Big dogs.
LEAGUE_TIER_OVERRIDES = {
    ("Virsliga", "Latvia"): 1, ("Meistriliiga", "Estonia"): 1,
    ("A Lyga", "Lithuania"): 1, ("Super League", "China"): 1,
    ("Premier Division", "Ireland"): 1, ("Veikkausliiga", "Finland"): 1,
    ("Superliga", "Denmark"): 1, ("Liga I", "Romania"): 1,
    ("Erovnuli Liga", "Georgia"): 1, ("Premier League", "Kazakhstan"): 1,
    ("Bundesliga", "Austria"): 1, ("Jupiler Pro League", "Belgium"): 1,
    ("Allsvenskan", "Sweden"): 1, ("Major League Soccer", "USA"): 1,
    ("Primera División", "Chile"): 1, ("Liga Profesional Argentina", "Argentina"): 1,
    ("Ekstraklasa", "Poland"): 1, ("Super League", "Switzerland"): 1,
    ("1 Lyga", "Lithuania"): 2, ("First Division", "Ireland"): 2,
    ("1. Division", "Norway"): 2, ("1. Deild", "Iceland"): 2,
    ("League One", "China"): 2, ("USL Championship", "USA"): 2,
    ("USL League One", "USA"): 3, ("Division 2 - Södra Svealand", "Sweden"): 4,
    ("League Two", "China"): 3, ("Capixaba B", "Brazil"): 5,
}
_DISCOVER_PRIORITY = {"strength": 0, "goals": 1, "patterns": 2}  # tie-break order
_NO_COMPARISON = {None, "unavailable", "too_few_matches"}

_DC_MATCH_EVENTS = {
    "Match Over 1.5": "Over_1_5",
    "Match Over 2.5": "Over_2_5",
    "Match Over 3.5": "Over_3_5",
    "Match Under 2.5": "Under_2_5",
    "Match Under 3.5": "Under_3_5",
    "BTTS Yes": "BTTS",
}
_DC_TEAM_EVENTS = (
    (" Over 0.5", "score_1goal"),
    (" Over 1.5", "score_2goals"),
    (" Win", "win"),
)


def _load_league_event_rates(conn) -> dict:
    """{(league_key, event_name, context): {rate, rows}} for the current season."""
    rates = {}
    for context in ("all", "home", "away"):
        df = compute_league_base_rates(conn, context=context,
                                       current_season_only=True)
        for r in df.itertuples(index=False):
            rates[(r.league, r.event_name, context)] = {
                "rate": float(r.base_rate), "rows": int(r.n)}
    # Goals 1–4 has no event flag ("goalless" means a team failed to score,
    # not 0-0), so count it from final scores: one home row per fixture.
    with conn.cursor() as cur:
        cur.execute("""
            SELECT f.league_name, COALESCE(rf.country_name, ''), COUNT(*),
                   AVG(CASE WHEN f.goals_for + f.goals_against BETWEEN 1 AND 4
                            THEN 1.0 ELSE 0.0 END)
            FROM raw.engine_head_to_head f
            JOIN raw.fact_fixtures rf ON rf.fixture_id = f.fixture_id
            WHERE f.is_played AND f.is_home AND f.is_current
            GROUP BY 1, 2
        """)
        for league, country, n, rate in cur.fetchall():
            rates[(f"{league} ({country or ''})", "goals_1_4", "fixtures")] = {
                "rate": float(rate), "rows": int(n)}
    return rates


def league_comparison_label(gap: float | None) -> str:
    """Plain-language label for a gap (team or estimate minus league rate)."""
    if gap is None:
        return "unavailable"
    if gap >= LEAGUE_GAP_WELL:
        return "well_above"
    if gap >= LEAGUE_GAP_SOME:
        return "above"
    if gap > -LEAGUE_GAP_SOME:
        return "typical"
    if gap > -LEAGUE_GAP_WELL:
        return "below"
    return "well_below"


def _league_key(fixture: dict) -> str:
    return f"{fixture.get('league')} ({fixture.get('country') or ''})"


def dc_league_rate(market: str, fixture: dict, rates: dict) -> dict | None:
    """League rate for the same event as a DC market, or None if unsupported."""
    league = _league_key(fixture)
    if market == "Match Goals 1-4":
        row = rates.get((league, "goals_1_4", "fixtures"))  # one row per fixture
        return row and {"event": "goals_1_4", "context": "all",
                        "rate": row["rate"], "matches": row["rows"]}
    if market in _DC_MATCH_EVENTS:
        event = _DC_MATCH_EVENTS[market]
        row = rates.get((league, event, "all"))
        return row and {"event": event, "context": "all",
                        "rate": row["rate"], "matches": row["rows"] // 2}
    for suffix, event in _DC_TEAM_EVENTS:
        if not market.endswith(suffix):
            continue
        team = market[: -len(suffix)]
        context = ("home" if team == fixture.get("home")
                   else "away" if team == fixture.get("away") else None)
        if context is None:
            return None
        row = rates.get((league, event, context))
        return row and {"event": event, "context": context,
                        "rate": row["rate"], "matches": row["rows"]}
    return None


def pattern_league_matches(row: dict, rates: dict) -> int | None:
    """Fixtures behind a pattern row's league base rate (current season).

    Rates come from the same query as the beliefs' base_rate. Home/away
    contexts have one row per fixture; "all" has one per team, so halve it.
    None when the league rate is not available.
    """
    context = row.get("context") or "all"
    ref = rates.get((row.get("league"), row.get("event"), context))
    if not ref:
        return None
    return ref["rows"] // 2 if context == "all" else ref["rows"]


def attach_league_comparisons(analysis: list[dict], rates: dict) -> None:
    """Add league_rate / league_gap / league_comparison to DC and pattern rows."""
    for fixture in analysis:
        for row in fixture.get("dixon_coles", []):
            ref = dc_league_rate(row.get("market", ""), fixture, rates)
            if not ref:
                row.update(league_rate=None, league_rate_matches=None,
                           league_rate_context=None, league_gap=None,
                           league_comparison="unavailable")
                continue
            # League blend (2026-10-17): for markets where high-gap chances
            # proved overconfident, the shown chance leans toward the league
            # rate. The pre-blend chance is kept for audit; gap, comparison and
            # Discover ranking all use the blended chance.
            blended = (blend_probability(row.get("market", ""),
                                         row.get("probability"), ref["rate"])
                       if ref["matches"] >= LEAGUE_RATE_MIN_MATCHES else None)
            if blended is not None:
                row["probability_pre_blend"] = row["probability"]
                row["probability"] = round(blended, 4)
                row["league_blend"] = True
                if "tier" in row:
                    row["tier"] = _tier(row["probability"])
            gap = float(row["probability"]) - ref["rate"]
            row.update(
                league_rate=round(ref["rate"], 4),
                league_rate_matches=ref["matches"],
                league_rate_context=ref["context"],
                league_gap=round(gap, 4),
                league_comparison=(
                    league_comparison_label(gap)
                    if ref["matches"] >= LEAGUE_RATE_MIN_MATCHES
                    else "too_few_matches"),
            )
        for row in fixture.get("bayesian", []):
            k, n, base = row.get("k"), row.get("n"), row.get("base_rate")
            league_n = pattern_league_matches(row, rates)
            if not n or k is None or base is None or league_n is None:
                row.update(league_gap=None, league_rate_matches=league_n,
                           league_comparison="unavailable")
                continue
            gap = k / n - float(base)
            # Same floors as goals: the team needs PATTERN_COMPARE_MIN_N
            # matches and the league rate LEAGUE_RATE_MIN_MATCHES fixtures,
            # or the comparison is noise.
            enough = (n >= PATTERN_COMPARE_MIN_N
                      and league_n >= LEAGUE_RATE_MIN_MATCHES)
            row.update(
                league_gap=round(gap, 4),
                league_rate_matches=league_n,
                league_comparison=(
                    league_comparison_label(gap) if enough else "too_few_matches"),
            )


def _load_comparison_context(conn, league_volatility: dict) -> tuple[dict, dict | None]:
    """League event rates + Strength group record, never fatal to the export.

    These feed display copy only. A failure leaves comparisons "unavailable"
    rather than stopping the daily run.
    """
    try:
        rates = _load_league_event_rates(conn)
    except Exception as exc:  # pragma: no cover - logged, export continues
        conn.rollback()
        print(f"  WARN league event rates unavailable: {exc}")
        rates = {}
    try:
        # D81: every league's own record (no upset-rate pre-filter), then classify.
        group = _load_elo_gap_group_record(conn, league_volatility, max_upset=1.0)
        group["big_dog_leagues"] = classify_big_dog_leagues(
            group["by_league"], _load_league_tiers(conn), load_big_dog_state())
        n_in = sum(r["status"] == "in" for r in group["big_dog_leagues"])
        n_held = sum(r["held"] for r in group["big_dog_leagues"])
        print(f"  Big dog leagues: {n_in} in ({n_held} held by buffer)")
    except Exception as exc:  # pragma: no cover
        conn.rollback()
        print(f"  WARN elo gap group record unavailable: {exc}")
        group = None
    return rates, group


def slight_favourite_level(gap: float) -> str:
    """D138: 'slight_edge' (about 5 in 10) or 'favourite' (about 6 in 10)."""
    return "slight_edge" if gap <= SLIGHT_EDGE_MAX_GAP else "favourite"


TEAM_TIERS_SQL = """
    WITH seasons AS (
        SELECT f.league_name, COALESCE(rf.country_name, '') AS country, f.league_season_id,
               ROW_NUMBER() OVER (PARTITION BY f.league_name, COALESCE(rf.country_name, '')
                                  ORDER BY MAX(f.kickoff_utc) DESC) AS rn
        FROM raw.engine_head_to_head f
        JOIN raw.fact_fixtures rf ON rf.fixture_id = f.fixture_id
        WHERE f.is_played
        GROUP BY 1, 2, 3
    ), teams AS (
        SELECT s.league_name, s.country, f.team_id
        FROM seasons s
        JOIN raw.engine_head_to_head f
          ON f.league_season_id = s.league_season_id AND f.is_played
        WHERE s.rn = 1
        GROUP BY 1, 2, 3
        HAVING COUNT(*) >= %s
    )
    SELECT t.league_name, t.country, t.team_id, e.rating
    FROM teams t
    JOIN LATERAL (
        SELECT COALESCE(h.rating_after_flat, h.rating_after) AS rating
        FROM elo.team_elo_history h
        WHERE h.team_id = t.team_id
        ORDER BY h.kickoff_utc DESC
        LIMIT 1
    ) e ON TRUE
"""


def tier_labels(ratings: dict) -> dict:
    """{team_id: 'Top'|'Middle'|'Bottom'} by 1-D k-means (k=3) on Elo; {} if too few."""
    if len(ratings) < TIER_MIN_TEAMS:
        return {}
    teams = list(ratings)
    x = np.array([float(ratings[t]) for t in teams])
    centres = np.quantile(x, [1 / 6, 1 / 2, 5 / 6])
    for _ in range(50):
        lab = np.abs(x[:, None] - centres[None]).argmin(1)
        new = np.array([x[lab == i].mean() if (lab == i).any() else centres[i] for i in range(3)])
        if np.allclose(new, centres):
            break
        centres = new
    rank = np.argsort(np.argsort(centres))
    return {t: TIER_NAMES[int(rank[l])] for t, l in zip(teams, lab)}


def _load_team_tiers(conn) -> dict:
    """{(league, country): {team_id: tier}}; empty on failure (context only)."""
    try:
        with conn.cursor() as cur:
            cur.execute(TEAM_TIERS_SQL, (TIER_MIN_GAMES,))
            rows = cur.fetchall()
    except Exception as exc:  # pragma: no cover
        conn.rollback()
        print(f"  WARN team tiers unavailable: {exc}")
        return {}
    by_league = defaultdict(dict)
    for league, country, team_id, rating in rows:
        if rating is not None:
            by_league[(league, country or "")][int(team_id)] = rating
    return {k: tier_labels(v) for k, v in by_league.items()}


def attach_team_tiers(analysis: list[dict], tiers: dict) -> None:
    """fixture["tiers"] = {"home": tier, "away": tier} when both teams have one."""
    for f in analysis:
        league = tiers.get((f.get("league"), f.get("country") or "")) or {}
        h, a = league.get(f.get("home_team_id")), league.get(f.get("away_team_id"))
        f["tiers"] = {"home": h, "away": a} if h and a else None


# ── Strength group record ────────────────────────────────────────────────
def _load_elo_gap_group_record(
        conn, league_volatility: dict, gap_min: int | None = None,
        max_upset: float | None = None) -> dict:
    """How favourites like the Strength shortlist actually finished.

    Same population rules as _build_elo_gap_shortlist: pre-match flat Elo gap,
    venue-adjusted by BIG_DOG_HOME_EDGE (D137), strictly above gap_min, in leagues whose upset rate is ≤ max_upset over
    enough history. Used for "favourites like this won X in 10" copy instead
    of a per-match win %, which did not separate stronger from weaker
    favourites (numbers audit §5).
    """
    gap_min = ELO_HUGE_GAP_MIN if gap_min is None else gap_min
    max_upset = ELO_LOW_VOLATILITY_MAX if max_upset is None else max_upset
    eligible = {k for k, v in league_volatility.items()
                if v["matches"] >= ELO_VOLATILITY_MIN_HISTORY
                and v["upset_rate"] <= max_upset}
    with conn.cursor() as cur:
        cur.execute("""
            WITH matches AS (
                SELECT
                    f.league_name AS league,
                    COALESCE(rf.country_name, '') AS country,
                    f.goals_for AS home_goals,
                    f.goals_against AS away_goals,
                    COALESCE(eh.rating_before_flat, eh.rating_before) + %s
                      - COALESCE(ea.rating_before_flat, ea.rating_before) AS diff
                FROM raw.engine_head_to_head f
                JOIN raw.fact_fixtures rf ON rf.fixture_id = f.fixture_id
                JOIN elo.team_elo_history eh
                  ON eh.fixture_id = f.fixture_id AND eh.team_id = f.team_id
                JOIN elo.team_elo_history ea
                  ON ea.fixture_id = f.fixture_id AND ea.team_id = f.opponent_id
                WHERE f.is_played AND f.is_home
            )
            SELECT league, country,
                   COUNT(*) AS matches,
                   SUM(CASE WHEN (diff > 0 AND home_goals > away_goals)
                              OR (diff < 0 AND away_goals > home_goals)
                            THEN 1 ELSE 0 END) AS won,
                   SUM(CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END) AS drew,
                   _ELO_BAND_SQL AS band
            FROM matches
            WHERE ABS(diff) > %s
            GROUP BY league, country, band
        """.replace("_ELO_BAND_SQL", _elo_band_sql()), (BIG_DOG_HOME_EDGE, gap_min))
        rows = cur.fetchall()
    return aggregate_elo_gap_group(rows, eligible, gap_min, max_upset)


def _elo_band_label(lo: int, hi: int | None) -> str:
    return f"{lo}-{hi}" if hi is not None else f"{lo}+"


def _elo_band_sql() -> str:
    parts = [f"WHEN ABS(diff) <= {hi} THEN '{_elo_band_label(lo, hi)}'"
             for lo, hi in ELO_GAP_BANDS if hi is not None]
    lo, hi = ELO_GAP_BANDS[-1]
    return f"CASE {' '.join(parts)} ELSE '{_elo_band_label(lo, hi)}' END"


def aggregate_elo_gap_group(rows, eligible: set, gap_min: int,
                            max_upset: float) -> dict:
    """Overall + per-gap-band record from (league, country, n, won, drew, band)."""
    def rec(n, w, d):
        return {"matches": n, "won": w, "drew": d, "lost": n - w - d,
                "win_rate": round(w / n, 4) if n else None}
    total = [0, 0, 0]
    bands = {_elo_band_label(lo, hi): [0, 0, 0] for lo, hi in ELO_GAP_BANDS}
    for league, country, n, w, d, band in rows:
        if (league, country or "") not in eligible:
            continue
        for acc in (total, bands.setdefault(band, [0, 0, 0])):
            acc[0] += int(n); acc[1] += int(w); acc[2] += int(d)
    out = {"gap_min": gap_min, "max_league_upset_rate": max_upset,
           "leagues": len(eligible), **rec(*total)}
    out["bands"] = [dict(band=_elo_band_label(lo, hi), min=lo, max=hi,
                         **rec(*bands[_elo_band_label(lo, hi)]))
                    for lo, hi in ELO_GAP_BANDS]
    per = defaultdict(lambda: [0, 0, 0])
    for league, country, n, w, d, _band in rows:
        key = (league, country or "")
        if key in eligible:
            acc = per[key]; acc[0] += int(n); acc[1] += int(w); acc[2] += int(d)
    out["by_league"] = sorted((
        dict(league=lg, country=ct, **rec(*v),
             win_rate_low=round(wilson_lower(v[1], v[0]), 4) if v[0] else None,
             meets_min=v[0] >= ELO_LEAGUE_MIN_MATCHES)
        for (lg, ct), v in per.items()), key=lambda r: -r["matches"])
    return out


def wilson_lower(won: int, n: int, z: float = ELO_LEAGUE_Z) -> float:
    """Pessimistic end of a win rate's range (Wilson score interval)."""
    if n <= 0:
        return 0.0
    p = won / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return max(0.0, (centre - margin) / (1 + z * z / n))


def elo_league_record(group: dict | None, league: str | None,
                      country: str | None) -> dict | None:
    """This league's own past Big dog record from the group export, or None."""
    for r in (group or {}).get("by_league") or []:
        if r["league"] == league and r["country"] == (country or ""):
            return r
    return None


def league_big_dogs_trusted(rec: dict | None) -> bool:
    """Enough past Big dog matches AND favourites reliably won them (join bar)."""
    return bool(rec and rec.get("meets_min")
                and (rec.get("win_rate_low") or 0) >= ELO_LEAGUE_MIN_LOW)


def classify_big_dog_leagues(by_league: list[dict], tiers: dict,
                             previously_in: set) -> list[dict]:
    """D81: which leagues get Big dogs this run, and why (in / out, held by buffer)."""
    out = []
    for r in by_league:
        key = (r["league"], r["country"])
        tier = tiers.get(key) or LEAGUE_TIER_OVERRIDES.get(key)
        low = r.get("win_rate_low") or 0
        was_in = f"{key[0]} ({key[1]})" in previously_in
        held = False
        if tier is None:
            status, reason = "out", "tier unknown"
        elif tier != 1:
            status, reason = "out", f"tier {tier} (top flight only)"
        elif not r.get("meets_min"):
            status, reason = "out", f"< {ELO_LEAGUE_MIN_MATCHES} Big dog matches"
        elif low >= ELO_LEAGUE_MIN_LOW:
            status, reason = "in", f"low ≥ {ELO_LEAGUE_MIN_LOW:.0%}"
        elif was_in and low >= ELO_LEAGUE_STAY_LOW:
            status, reason, held = "in", f"held by buffer (low ≥ {ELO_LEAGUE_STAY_LOW:.0%})", True
        else:
            status = "out"
            reason = (f"fell below {ELO_LEAGUE_STAY_LOW:.0%}" if was_in
                      else f"low < {ELO_LEAGUE_MIN_LOW:.0%}")
        out.append({"league": key[0], "country": key[1], "tier": tier,
                    "status": status, "reason": reason, "held": held,
                    "was_in": was_in, "matches": r.get("matches"),
                    "won": r.get("won"), "win_rate": r.get("win_rate"),
                    "win_rate_low": r.get("win_rate_low")})
    return out


def big_dog_league_in(group: dict | None, league: str | None,
                      country: str | None) -> bool:
    """League gets Big dogs this run (D81 classification; D74 rule if absent)."""
    rows = (group or {}).get("big_dog_leagues")
    if rows is None:
        return league_big_dogs_trusted(elo_league_record(group, league, country))
    return any(r["league"] == league and r["country"] == (country or "")
               and r["status"] == "in" for r in rows)


def load_big_dog_state(path: Path = BIG_DOG_STATE_PATH) -> set:
    try:
        return set(json.loads(path.read_text()).get("leagues", []))
    except (OSError, ValueError):
        return set()


def save_big_dog_state(group: dict | None, path: Path = BIG_DOG_STATE_PATH) -> None:
    rows = (group or {}).get("big_dog_leagues")
    if rows is None:
        return
    leagues = sorted(f"{r['league']} ({r['country']})" for r in rows if r["status"] == "in")
    path.write_text(json.dumps({
        "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "join_low": ELO_LEAGUE_MIN_LOW, "stay_low": ELO_LEAGUE_STAY_LOW,
        "leagues": leagues}, indent=2))


def _load_league_tiers(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT dl.league_name, COALESCE(dc.country_name, ''), dl.tier
            FROM dim.dim_leagues dl
            LEFT JOIN dim.dim_countries dc ON dc.country_id = dl.country_id
            WHERE dl.tier IS NOT NULL
        """)
        return {(lg, ct): int(t) for lg, ct, t in cur.fetchall()}


def elo_group_win_rate(gap: float, group: dict | None) -> tuple[float | None, str]:
    """Win rate for favourites in this gap's band; overall rate if the band is thin."""
    if not group:
        return None, "unavailable"
    for b in group.get("bands") or []:
        if gap > b["min"] and (b["max"] is None or gap <= b["max"]):
            if b["matches"] >= ELO_BAND_MIN_MATCHES and b["win_rate"] is not None:
                return b["win_rate"], f"band {b['band']}"
            break
    return group.get("win_rate"), "all big dogs"


def league_win_rate(fixture: dict, team: str, rates: dict) -> dict | None:
    """League win rate at the team's venue; all-games rate if the split is missing."""
    league = _league_key(fixture)
    side = ("home" if team == fixture.get("home")
            else "away" if team == fixture.get("away") else None)
    for ctx in ([side] if side else []) + ["all"]:
        row = rates.get((league, "win", ctx))
        if row and row.get("rows"):
            return {"rate": row["rate"], "context": ctx, "rows": row["rows"]}
    return None


def _fixture_favourite(fixture: dict, strength: dict | None) -> str | None:
    if strength:
        return strength.get("favourite")
    for m in fixture.get("mismatch") or []:
        for team in (fixture.get("home"), fixture.get("away")):
            if team and str(m.get("market", "")).startswith(f"{team} "):
                return team
    return None


def pattern_prior_matches(analysis: list[dict]) -> dict:
    """D129: {(league, event, context): prior matches} from the run's team records.

    Uses every team record carried in the run's `bayesian` rows (one per team,
    league, event and venue context). Keys with fewer than
    PATTERN_PRIOR_MIN_TEAMS teams on PATTERN_PRIOR_MIN_GAMES+ games are left
    out, so callers fall back to DISCOVER_PATTERN_PRIOR_MATCHES.
    """
    teams: dict = defaultdict(dict)
    for fixture in analysis:
        for b in fixture.get("bayesian") or []:
            k, n = b.get("k"), b.get("n")
            if k is None or not n or not b.get("team"):
                continue
            key = (b.get("league"), b.get("event"), b.get("context") or "all")
            teams[key][b["team"]] = (int(k), int(n))
    out = {}
    for key, recs in teams.items():
        rows = [(k, n) for k, n in recs.values() if n >= PATTERN_PRIOR_MIN_GAMES]
        if len(rows) < PATTERN_PRIOR_MIN_TEAMS:
            continue
        k_all = sum(k for k, _ in rows); n_all = sum(n for _, n in rows)
        p = k_all / n_all
        if p <= 0 or p >= 1:
            continue
        rates_ = [k / n for k, n in rows]
        mean = sum(rates_) / len(rates_)
        observed = sum((r - mean) ** 2 for r in rates_) / (len(rates_) - 1)
        luck = sum(p * (1 - p) / n for _, n in rows) / len(rows)
        tau2 = observed - luck
        m = PATTERN_PRIOR_CAP if tau2 <= 0 else p * (1 - p) / tau2 - 1
        out[key] = round(min(max(m, PATTERN_PRIOR_FLOOR), PATTERN_PRIOR_CAP), 2)
    return out


def discover_candidates(fixture: dict, strength: dict | None,
                        group: dict | None, rates: dict,
                        goals_min_prob: float = DISCOVER_DC_MIN_PROB,
                        pattern_priors: dict | None = None) -> list[dict]:
    """Every Discover-eligible event for one fixture, scored on one scale (D71).

    goals_min_prob: 0.70 for standouts (D101), 0.75 for the Goals section (2026-10-17).
    """
    out = []
    for r in fixture.get("dixon_coles") or []:
        market = r.get("market", "")
        if (market.endswith(" Win") or r.get("league_gap") is None
                or (r.get("probability") or 0) < goals_min_prob
                or r.get("league_comparison") in _NO_COMPARISON):
            continue
        out.append({"list": "goals",
                    "score": float(r["league_gap"]) * DISCOVER_GOALS_WEIGHT,
                    "market": market, "probability": r["probability"],
                    "league_rate": r.get("league_rate"),
                    "league_comparison": r.get("league_comparison")})
    for p in fixture.get("bayesian") or []:
        k, n, base = p.get("k"), p.get("n"), p.get("base_rate")
        if (not n or k is None or base is None
                or n < PATTERN_COMPARE_MIN_N
                or p.get("league_comparison") in _NO_COMPARISON):
            continue
        m = (pattern_priors or {}).get(
            (p.get("league"), p.get("event"), p.get("context") or "all"),
            DISCOVER_PATTERN_PRIOR_MATCHES)
        shrunk = (k + m * float(base)) / (n + m)
        out.append({"list": "patterns", "score": shrunk - float(base),
                    "team": p.get("team"), "event": p.get("event"),
                    "context": p.get("context"), "k": k, "n": n,
                    "base_rate": base, "shrunk_rate": round(shrunk, 4),
                    "prior_matches": m,
                    "league_comparison": p.get("league_comparison")})
    if strength:
        rec = elo_league_record(group, fixture.get("league"), fixture.get("country"))
        league = league_win_rate(fixture, strength.get("favourite"), rates)
        if big_dog_league_in(group, fixture.get("league"), fixture.get("country")) \
                and rec and league:
            group_rate = rec["win_rate_low"]
            source = f"league record {rec['won']}/{rec['matches']} (lower bound)"
            out.append({"list": "strength", "score": group_rate - league["rate"],
                        "favourite": strength.get("favourite"),
                        "underdog": strength.get("underdog"),
                        "elo_gap": strength.get("elo_gap"),
                        "group_win_rate": group_rate, "group_rate_source": source,
                        "league_win_rate": round(league["rate"], 4),
                        "league_win_context": league["context"]})
    return out


def attach_discover_picks(analysis: list[dict], shortlist: list[dict],
                          group: dict | None, rates: dict) -> None:
    """Pick one Discover card per fixture: the highest score above its league.

    Ties go strength > goals > patterns, then to the favourite's side, so a
    mirrored pair (Zlin winless 9/9 vs Slavia unbeaten 9/9) is told about the
    stronger team. No standout below 15 adjusted percentage points. A shortlisted
    favourite is carried as `big_dog` so any winning card can show the badge.
    """
    by_fid = {s["fixture_id"]: s for s in shortlist}
    priors = pattern_prior_matches(analysis)   # D129
    for fixture in analysis:
        strength = by_fid.get(fixture.get("fixture_id"))
        fav = _fixture_favourite(fixture, strength)
        cands = discover_candidates(fixture, strength, group, rates,
                                    pattern_priors=priors)
        if strength:
            s = next((c for c in cands if c["list"] == "strength"), None)
            rec = elo_league_record(group, fixture.get("league"),
                                    fixture.get("country"))
            strength.update(
                league_big_dog_matches=rec and rec["matches"],
                league_big_dog_won=rec and rec["won"],
                league_big_dog_win_rate=rec and rec["win_rate"],
                league_big_dog_win_rate_low=rec and rec["win_rate_low"],
                league_record_ok=big_dog_league_in(
                    group, fixture.get("league"), fixture.get("country")),
                group_win_rate=s and s["group_win_rate"],
                group_rate_source=s and s["group_rate_source"],
                league_win_rate=s and s["league_win_rate"],
                league_win_context=s and s["league_win_context"],
                discover_score=round(s["score"], 4) if s else None)
        ranked = sorted(cands, key=lambda c: (
            c["score"], -_DISCOVER_PRIORITY[c["list"]],
            c.get("team") == fav), reverse=True)
        # Apply the threshold before presentation rounding; tolerate only float noise.
        best = ranked[0] if ranked and (
            ranked[0]["score"] >= STANDOUT_MIN_SCORE
            or math.isclose(ranked[0]["score"], STANDOUT_MIN_SCORE,
                            rel_tol=0, abs_tol=1e-12)) else None
        fixture["discover_pick"] = best and {
            **best, "score": round(best["score"], 4), "big_dog": strength.get("favourite") if strength else None,
            "runner_up": ({"list": ranked[1]["list"], "score": round(ranked[1]["score"], 4)}
                          if len(ranked) > 1 else None),
            "candidates": len(ranked)}
        fixture["discover_sections"] = section_picks(
            discover_candidates(fixture, strength, group, rates,
                                goals_min_prob=DISCOVER_SECTION_DC_MIN_PROB,
                                pattern_priors=priors),
            fav, strength.get("favourite") if strength else None)


def section_picks(cands: list[dict], fav: str | None,
                  big_dog: str | None) -> dict:
    """D121: each Discover section shows a fixture once, on its strongest item
    in that section, independent of the other sections (D71 revised).

    Same scores as the standout pick. A fixture is in a section when its best
    item there scores above 0 (above its league); no 15-point standout floor.
    Ties go to the favourite's side, as for the standout pick.
    """
    out = {}
    for lst in _DISCOVER_PRIORITY:
        mine = [c for c in cands if c["list"] == lst and c["score"] > 0
                and (lst != "goals" or c.get("league_comparison") != "typical")]
        if not mine:
            continue
        best = max(mine, key=lambda c: (c["score"], c.get("team") == fav))
        out[lst] = {**best, "score": round(best["score"], 4), "big_dog": big_dog,
                    "candidates": len(mine)}
    return out


def _rolling_window(now: datetime | None, timezone_name: str):
    """Today through the end of the sixth following local day (calendar days)."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must include a timezone")
    zone = ZoneInfo(timezone_name)
    first_day = now.astimezone(zone).date()
    end = datetime.combine(first_day + timedelta(days=7), time.min, tzinfo=zone)
    return now, zone, first_day, end


def _window_row(fixture: dict, pick: dict, now: datetime, zone, end: datetime):
    """One list row for an upcoming fixture inside the window, else None."""
    try:
        kickoff = datetime.fromisoformat(str(fixture.get("kickoff", "")).replace("Z", "+00:00"))
    except ValueError:
        return None
    if kickoff.tzinfo is None or kickoff.utcoffset() is None:
        return None
    if not now < kickoff < end or fixture.get("fixture_id") is None:
        return None
    local = kickoff.astimezone(zone)
    return {**{key: fixture.get(key) for key in
               ("fixture_id", "home", "away", "league", "country", "kickoff")},
            "fixture_date": local.date().isoformat(),
            "kickoff_local": local.isoformat(), "pick": dict(pick)}


def select_rolling_sections(analysis: list[dict], *,
                            now: datetime | None = None,
                            timezone_name: str = "UTC") -> dict:
    """D121: the three Discover section lists for the same window as standouts.

    Each list has a fixture at most once, on its best item for that section
    (`discover_sections`), ranked by that section's score, highest first; ties
    by earlier kickoff, then fixture id. No cap: the client pages the list.
    Call on read as well as export to drop started fixtures between refreshes.
    """
    now, zone, first_day, end = _rolling_window(now, timezone_name)
    lists = {lst: {} for lst in _DISCOVER_PRIORITY}
    for fixture in analysis:
        for lst, pick in (fixture.get("discover_sections") or {}).items():
            if lst not in lists or not pick or pick.get("score", 0) <= 0:
                continue
            row = _window_row(fixture, pick, now, zone, end)
            if row is None:
                continue
            old = lists[lst].get(row["fixture_id"])
            if old is None or pick["score"] > old["pick"]["score"]:
                lists[lst][row["fixture_id"]] = row
    out = {"timezone": timezone_name, "as_of": now.isoformat(),
           "start_date": first_day.isoformat(),
           "end_date": (first_day + timedelta(days=6)).isoformat(),
           "end_exclusive": end.isoformat()}
    for lst, rows in lists.items():
        items = sorted(rows.values(), key=lambda r: (
            -r["pick"]["score"], r["kickoff"], str(r["fixture_id"])))
        out[lst] = {"count": len(items), "items": items}
    return out


def select_rolling_standouts(analysis: list[dict], *,
                            now: datetime | None = None,
                            timezone_name: str = "UTC") -> dict:
    """Upcoming standouts through the end of the sixth following local day.

    Call on read as well as export to remove started fixtures between refreshes.
    The export defaults to UTC; user-facing callers pass their IANA timezone.
    """
    now, zone, first_day, end = _rolling_window(now, timezone_name)
    unique = {}
    for fixture in analysis:
        pick = fixture.get("discover_pick")
        if not pick or pick.get("score", 0) < STANDOUT_MIN_SCORE:
            continue
        row = _window_row(fixture, pick, now, zone, end)
        if row is None:
            continue
        old = unique.get(row["fixture_id"])
        if old is None or pick["score"] > old["pick"]["score"]:
            unique[row["fixture_id"]] = row
    items = sorted(unique.values(), key=lambda r: (
        -r["pick"]["score"], -1 * (r["pick"]["list"] == "strength"),
        -1 * (r["pick"]["list"] == "goals"), r["kickoff"], str(r["fixture_id"])))
    return {"timezone": timezone_name, "as_of": now.isoformat(),
            "start_date": first_day.isoformat(),
            "end_date": (first_day + timedelta(days=6)).isoformat(),
            "end_exclusive": end.isoformat(), "minimum_score": STANDOUT_MIN_SCORE,
            "count": len(items), "items": items}


def _normalise_market(model: str, raw_market: str, side: str = "",
                      team: str = "", favourite: str = "") -> str | None:
    """Map model-specific market labels to a unified SportyBet market key."""
    if model == "poisson":
        parts = raw_market.split()
        direction = parts[0].capitalize()
        line = parts[1]
        return f"Match {direction} {line}"

    if model == "mismatch":
        if "over_1.5" in raw_market or "Over 1.5" in raw_market:
            return "Match Over 1.5"
        if "over_2.5" in raw_market or "Over 2.5" in raw_market:
            return "Match Over 2.5"
        if raw_market == "fav_to_score":
            return f"{favourite} Over 0.5"
        if raw_market == "fav_over_1.5":
            return f"{favourite} Over 1.5"
        if raw_market == "fav_win":
            return f"{favourite} Win"
        return None

    if model == "bayesian":
        event = raw_market.split()[-1] if " " in raw_market else raw_market
        if event == "Over_1_5":
            return "Match Over 1.5"
        if event == "Over_2_5":
            return "Match Over 2.5"
        if event == "Over_3_5":
            return "Match Over 3.5"
        if event == "Under_2_5":
            return "Match Under 2.5"
        if event == "Under_3_5":
            return "Match Under 3.5"
        if event == "score_1goal":
            return f"{team} Over 0.5"
        if event == "score_2goals":
            return f"{team} Over 1.5"
        if event == "win":
            return f"{team} Win"
        if event == "unbeaten":
            return f"{team} Double Chance"
        if event == "concede_1":
            opp_side = "away" if side == "home" else "home"
            return f"OPP_{opp_side} Over 0.5"
        if event == "BTTS":
            return "BTTS Yes"
        if event == "clean_sheet":
            return f"{team} Clean Sheet"
        return None

    if model == "dixon_coles":
        return raw_market

    return None


def _sportify_market(unified: str, home: str, away: str) -> dict:
    """Convert a unified market key into SportyBet-bookable description."""
    if unified == "Match Goals 1-4":
        return {"market": "Goal Bounds", "selection": "1-4", "line": None}
    if unified.startswith("Match Over"):
        line = unified.split()[-1]
        return {"market": "Over/Under", "selection": f"Over {line}", "line": float(line)}
    if unified.startswith("Match Under"):
        line = unified.split()[-1]
        return {"market": "Over/Under", "selection": f"Under {line}", "line": float(line)}
    if "Over 0.5" in unified:
        if unified.startswith("OPP_home"):
            return {"market": "Home Team Over/Under", "selection": f"{home} Over 0.5", "line": 0.5}
        if unified.startswith("OPP_away"):
            return {"market": "Away Team Over/Under", "selection": f"{away} Over 0.5", "line": 0.5}
        for team_name in (home, away):
            if team_name.lower() in unified.lower() or unified.startswith(team_name.split()[0]):
                is_home = team_name == home
                mkt = "Home Team Over/Under" if is_home else "Away Team Over/Under"
                return {"market": mkt, "selection": f"{team_name} Over 0.5", "line": 0.5}
        return {"market": "Team Over/Under", "selection": unified, "line": 0.5}
    if "Over 1.5" in unified and not unified.startswith("Match"):
        for team_name in (home, away):
            if team_name.lower() in unified.lower():
                is_home = team_name == home
                mkt = "Home Team Over/Under" if is_home else "Away Team Over/Under"
                return {"market": mkt, "selection": f"{team_name} Over 1.5", "line": 1.5}
    if "Win" in unified:
        return {"market": "1X2", "selection": unified, "line": None}
    if "Double Chance" in unified:
        for team_name in (home, away):
            if team_name.lower() in unified.lower():
                if team_name == home:
                    return {"market": "Double Chance", "selection": "1X", "line": None}
                else:
                    return {"market": "Double Chance", "selection": "X2", "line": None}
        return {"market": "Double Chance", "selection": unified, "line": None}
    if "BTTS" in unified:
        return {"market": "Both Teams to Score", "selection": "Yes", "line": None}
    return {"market": "Other", "selection": unified, "line": None}


def _is_match_total_market(unified: str) -> bool:
    return unified.startswith("Match Over") or unified.startswith("Match Under")


def _passes_goal_model_rules(unified: str, models: set[str]) -> bool:
    # Poisson retired 2026-10-11: Over 1.5 and unders now need Dixon-Coles
    # alone (they used to need DC and Poisson to agree).
    if unified == "Match Over 1.5":
        return "dixon_coles" in models
    if unified == "Match Over 2.5":
        return True
    if unified.startswith("Match Under"):
        return "dixon_coles" in models
    if _is_match_total_market(unified) and len(models) == 1:
        return False
    if unified == "BTTS Yes":
        return "dixon_coles" in models
    return True


def _strength_profile(row: pd.Series) -> dict:
    profile = {
        "overall": {
            "attack_rate": round(float(row["attack_rate"]), 2),
            "defense_rate": round(float(row["defense_rate"]), 2),
            "attack_shrunk": round(float(row["attack_shrunk"]), 3),
            "defense_shrunk": round(float(row["defense_shrunk"]), 3),
            "attack_pct": round(float(row["attack_percentile"]), 1),
            "defense_pct": round(float(row["defense_percentile"]), 1),
            "attack_rank": int(row["attack_rank"]),
            "defense_rank": int(row["defense_rank"]),
            "league_size": int(row["league_size"]),
        },
    }
    for ctx in ("home", "away"):
        col_atk = f"{ctx}_attack_shrunk"
        if col_atk in row.index and pd.notna(row.get(col_atk)):
            profile[ctx] = {
                "attack_rate": round(float(row[f"{ctx}_attack_rate"]), 2) if pd.notna(row.get(f"{ctx}_attack_rate")) else None,
                "defense_rate": round(float(row[f"{ctx}_defense_rate"]), 2) if pd.notna(row.get(f"{ctx}_defense_rate")) else None,
                "attack_shrunk": round(float(row[f"{ctx}_attack_shrunk"]), 3),
                "defense_shrunk": round(float(row[f"{ctx}_defense_shrunk"]), 3),
                "attack_pct": round(float(row[f"{ctx}_attack_percentile"]), 1),
                "defense_pct": round(float(row[f"{ctx}_defense_percentile"]), 1),
                "attack_rank": int(row[f"{ctx}_attack_rank"]) if pd.notna(row.get(f"{ctx}_attack_rank")) else None,
                "defense_rank": int(row[f"{ctx}_defense_rank"]) if pd.notna(row.get(f"{ctx}_defense_rank")) else None,
                "league_size": int(row[f"{ctx}_league_size"]) if pd.notna(row.get(f"{ctx}_league_size")) else None,
            }
        else:
            profile[ctx] = None
    return profile


def _load_fixture_strength_profiles(conn) -> dict[int, dict]:
    strength_df = load_team_df(conn)
    strength_all = compute_team_strength(strength_df)
    strength_by_h2h = {
        row["team_id"]: _strength_profile(row)
        for _, row in strength_all.iterrows()
    }

    # fact_fixtures carries warehouse team ids, the same space as the strength
    # table, so no API -> warehouse translation is needed.
    fixtures = pd.read_sql("""
        SELECT fixture_id, home_team_id, away_team_id
        FROM raw.fact_fixtures
    """, conn)
    if fixtures.empty:
        return {}

    fixture_strength = {}
    for fid, home_id, away_id in fixtures.itertuples(index=False):
        fixture_strength[int(fid)] = {
            "home_team": (strength_by_h2h.get(int(home_id))
                          if pd.notna(home_id) else None),
            "away_team": (strength_by_h2h.get(int(away_id))
                          if pd.notna(away_id) else None),
        }
    return fixture_strength


def _ctx_strength(team_profile: dict | None, ctx: str) -> dict | None:
    if not team_profile:
        return None
    return team_profile.get(ctx) or team_profile.get("overall")


def _rank_summary(ctx: dict | None) -> dict | None:
    if not ctx:
        return None
    return {
        "attack_rank": ctx.get("attack_rank"),
        "defense_rank": ctx.get("defense_rank"),
        "league_size": ctx.get("league_size"),
        "attack_pct": ctx.get("attack_pct"),
        "defense_pct": ctx.get("defense_pct"),
    }


def _fixture_strength_rank(strength: dict | None) -> dict | None:
    if not strength:
        return None
    home_profile = strength.get("home_team")
    away_profile = strength.get("away_team")
    home_ctx = _ctx_strength(home_profile, "home")
    away_ctx = _ctx_strength(away_profile, "away")
    home_overall = _ctx_strength(home_profile, "overall")
    away_overall = _ctx_strength(away_profile, "overall")
    if not home_ctx and not away_ctx and not home_overall and not away_overall:
        return None
    return {
        "home": {
            "context": "home",
            "split": _rank_summary(home_ctx),
            "overall": _rank_summary(home_overall),
        },
        "away": {
            "context": "away",
            "split": _rank_summary(away_ctx),
            "overall": _rank_summary(away_overall),
        },
    }


def _under35_strength_gate(strength: dict | None) -> tuple[bool, dict | None]:
    if not strength:
        return False, None
    home = _ctx_strength(strength.get("home_team"), "home")
    away = _ctx_strength(strength.get("away_team"), "away")
    if not home or not away:
        return False, None

    attack_pcts = [home["attack_pct"], away["attack_pct"]]
    defense_pcts = [home["defense_pct"], away["defense_pct"]]
    avg_attack = sum(attack_pcts) / 2
    avg_defense = sum(defense_pcts) / 2
    detail = {
        "avg_attack_pct": round(avg_attack, 1),
        "max_attack_pct": round(max(attack_pcts), 1),
        "avg_defense_pct": round(avg_defense, 1),
        "min_defense_pct": round(min(defense_pcts), 1),
    }
    ok = (
        avg_attack <= UNDER35_MAX_AVG_ATTACK_PCT
        and max(attack_pcts) <= UNDER35_MAX_SINGLE_ATTACK_PCT
        and avg_defense >= UNDER35_MIN_AVG_DEFENSE_PCT
        and min(defense_pcts) >= UNDER35_MIN_SINGLE_DEFENSE_PCT
    )
    return ok, detail


def _market_tier(unified_market: str, avg_confidence: float,
                 strength_gate: dict | None = None) -> str | None:
    """Market-specific quality tier from walk-forward calibration."""
    if unified_market != "Match Under 3.5" or not strength_gate:
        return None
    if avg_confidence >= UNDER35_TIER_A_THRESHOLD:
        return "A"
    if avg_confidence >= UNDER35_TIER_B_THRESHOLD:
        return "B"
    if avg_confidence >= UNDER35_TIER_C_THRESHOLD:
        return "C"
    return None


def _f(v):
    """Coerce numpy/NaN/None to a plain float or None (JSON/DB safe)."""
    if v is None:
        return None
    try:
        return float(v) if pd.notna(v) else None
    except (TypeError, ValueError):
        return None


# ── per-model signal extraction (shared by run_consensus and run_model) ───────
# Each returns a list of normalised picks:
#   {fixture_id, model, unified_market, probability, raw_probability, detail}

def poisson_signals(poisson_picks) -> list[dict]:
    out = []
    if poisson_picks is None or poisson_picks.empty:
        return out
    for _, row in poisson_picks.iterrows():
        unified = _normalise_market("poisson", f"{row['direction']} {row['line']}")
        if unified:
            out.append({"fixture_id": int(row["fixture_id"]), "model": "poisson",
                        "unified_market": unified, "probability": _f(row["probability"]),
                        "raw_probability": _f(row.get("raw_probability")),
                        "detail": {"tier": _tier(row["probability"]),
                                   "raw": _f(row.get("raw_probability"))}})
    return out


def mismatch_signals(mismatch_picks) -> list[dict]:
    out = []
    for p in mismatch_picks:
        unified = _normalise_market("mismatch", p.market, favourite=p.favourite)
        if unified:
            out.append({"fixture_id": int(p.fixture_id), "model": "mismatch",
                        "unified_market": unified, "probability": _f(p.confidence),
                        "raw_probability": None,
                        "detail": {"gap": p.gap, "gap_bin": p.gap_bin,
                                   # league ids live on the fixture (league_id,
                                   # warehouse); no API-Football ids in exports
                                   "country": p.country}})
    return out


def _bayesian_signal_decision(b: dict, side: str,
                              home_away: tuple[str, str] | None) -> tuple[str | None, str]:
    """Return (unified_market, eligibility_reason) for one Bayesian belief.

    The reason is persisted with fixture analysis so rejected beliefs remain
    auditable without entering the betting consensus pool.
    """
    if not b.get("display_eligible", True):
        return None, "below_display_threshold"
    if b.get("primary") is False:
        return None, "secondary_context"
    if b["event"] == "BTTS":
        return None, "btts_disabled"
    if b["event"] not in BETTING_EVENTS:
        return None, "not_betting_allowlist"

    unified = _normalise_market(
        "bayesian", f"{b['team']} {b['event']}", side=side, team=b["team"]
    )
    if not unified:
        return None, "no_market_mapping"
    if unified.startswith("OPP_"):
        if not home_away:
            return None, "missing_fixture_mapping"
        opp_side = "home" if side == "away" else "away"
        unified = f"{home_away[0] if opp_side == 'home' else home_away[1]} Over 0.5"
    return unified, "included"


def annotate_bayesian_analysis(bayesian_by_fixture: dict,
                               home_away: dict) -> dict:
    """Add stable consensus eligibility metadata to every Bayesian belief."""
    annotated = {}
    for fid, sides in bayesian_by_fixture.items():
        fixture_entry = {"home": [], "away": []}
        for side in ("home", "away"):
            for belief in sides.get(side, []):
                market, reason = _bayesian_signal_decision(
                    belief, side, home_away.get(fid)
                )
                row = {
                    **belief,
                    "side": side,
                    "consensus_eligible": reason == "included",
                    "eligibility_reason": reason,
                    "unified_market": market,
                }
                fixture_entry[side].append(row)
        if fixture_entry["home"] or fixture_entry["away"]:
            annotated[int(fid)] = fixture_entry
    return annotated


def bayesian_signals(bayesian_by_fixture, home_away: dict) -> list[dict]:
    """home_away: {fixture_id: (home_name, away_name)} — for the concede→OPP remap."""
    out = []
    for fid, sides in bayesian_by_fixture.items():
        ha = home_away.get(fid)
        for side in ("home", "away"):
            for b in sides.get(side, []):
                unified, reason = _bayesian_signal_decision(b, side, ha)
                if reason != "included":
                    continue
                out.append({"fixture_id": int(fid), "model": "bayesian",
                            "unified_market": unified, "probability": _f(b["posterior"]),
                            "raw_probability": None,
                            "detail": {"streak": b["streak"], "trend": b["trend"],
                                       "n": b["n"], "k": b.get("k"),
                                       "context": b.get("context"),
                                       "boost": b.get("boost"),
                                       "base_rate": b["base_rate"],
                                       "team": b["team"], "side": side,
                                       "event": b["event"]}})
    return out


def dixon_signals(dixon_picks) -> list[dict]:
    out = []
    if dixon_picks is None or dixon_picks.empty:
        return out
    for _, row in dixon_picks.iterrows():
        unified = _normalise_market("dixon_coles", row["market"])
        if unified:
            out.append({"fixture_id": int(row["fixture_id"]), "model": "dixon_coles",
                        "unified_market": unified, "probability": _f(row["probability"]),
                        "raw_probability": _f(row.get("raw_probability")),
                        "detail": {"raw": _f(row.get("raw_probability")),
                                   "global_calibrated": _f(row.get("global_calibrated_probability")),
                                   "calibration_scope": row.get("calibration_scope", "global"),
                                   "calibration_map": row.get("calibration_map"),
                                   "league_adjustment": _f(row.get("league_adjustment")),
                                   "lambda_home": _f(row.get("lambda_home")),
                                   "lambda_away": _f(row.get("lambda_away"))}})
    return out


def _high_scoring_fixture_ids(fixture_ids, conn=None) -> set:
    """fixture_ids whose league averages > HIGH_SCORING_AVG_GOALS goals/game."""
    ids = [int(f) for f in fixture_ids]
    if not ids:
        return set()
    own = conn is None
    conn = conn or get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT rf.api_league_id, avg(f.goals_for + f.goals_against)
            FROM raw.engine_head_to_head f
            JOIN raw.fact_fixtures rf ON rf.fixture_id = f.fixture_id
            WHERE f.is_home AND f.is_played AND rf.season IN (2025, 2026)
            GROUP BY rf.api_league_id
        """)
        high_lids = {lid for lid, ag in cur.fetchall()
                     if ag is not None and float(ag) > HIGH_SCORING_AVG_GOALS}
        cur.execute("SELECT fixture_id, api_league_id FROM raw.fact_fixtures "
                    "WHERE fixture_id = ANY(%s)", (ids,))
        return {int(fid) for fid, lid in cur.fetchall() if lid in high_lids}
    except Exception:  # noqa: BLE001
        return set()
    finally:
        if own:
            conn.close()


def _score_pool(consensus: dict, fixture_meta: dict,
                fixture_strength_all: dict,
                high_scoring_fids: set = frozenset(),
                bounds_league_priors: dict | None = None
                ) -> tuple[list[dict], dict, list[dict]]:
    """Apply gates and tiers and rank the consensus dict into a pool.

    Pure function of its inputs — shared by the in-process `run_consensus`
    and the fan-in `build_consensus_from_signals` (DAG) path.
    """
    ranked = []
    near_threshold = []
    gate_status = {}   # (fid, unified_market) -> {"in_pool": bool, "reason": str}
    for key, pick in consensus.items():
        fid = pick["fixture_id"]
        um = pick["unified_market"]
        meta = fixture_meta.get(fid)
        if not meta:
            continue

        def _reject(reason):
            gate_status[key] = {"in_pool": False, "reason": reason}

        models = set(pick["models"])
        n_models = len(models)
        max_conf = max(pick["confidences"])
        avg_conf = sum(pick["confidences"]) / len(pick["confidences"])
        solo_threshold = (
            BOUNDS_SOLO_THRESHOLD
            if um == "Match Goals 1-4"
            else SOLO_THRESHOLD
        )

        # DC-favoured unders in high-scoring leagues: Poisson overpredicts unders
        # there while DC stays calibrated, so price off DC's probability.
        if (um.startswith("Match Under")
                and fid in high_scoring_fids and "dixon_coles" in pick["models"]):
            dc_conf = pick["confidences"][pick["models"].index("dixon_coles")]
            avg_conf = UNDER_HIGH_DC_WEIGHT * dc_conf + (1 - UNDER_HIGH_DC_WEIGHT) * avg_conf

        strength_rank = _fixture_strength_rank(fixture_strength_all.get(fid))

        if not _passes_goal_model_rules(um, models):
            if um == "BTTS Yes":
                _reject("needs Dixon-Coles")
            elif um == "Match Over 1.5" or um.startswith("Match Under"):
                _reject("needs Dixon-Coles")
            else:
                _reject("solo goal-model pick")
            continue
        if um == "Match Over 1.5" and avg_conf < OVER15_DC_POISSON_THRESHOLD:
            _reject(f"below Over 1.5 floor ({avg_conf:.0%} < {OVER15_DC_POISSON_THRESHOLD:.0%})")
            continue
        strength_gate = None
        if um == "Match Under 3.5":
            passes_strength, strength_gate = _under35_strength_gate(
                fixture_strength_all.get(fid))
            if not passes_strength:
                _reject("fails Under 3.5 strength gate"); continue
        market_tier = _market_tier(um, avg_conf, strength_gate)
        if um == "Match Under 3.5" and not market_tier:
            _reject("below Under 3.5 tier"); continue

        # Filter by tier thresholds
        if n_models >= 3 and avg_conf < TRIO_THRESHOLD:
            _reject(f"below trio threshold ({avg_conf:.0%} < {TRIO_THRESHOLD:.0%})"); continue
        if n_models == 2 and avg_conf < DUO_THRESHOLD:
            _reject(f"below duo threshold ({avg_conf:.0%} < {DUO_THRESHOLD:.0%})"); continue
        if n_models == 1 and max_conf < solo_threshold:
            if (um == "Match Goals 1-4"
                    and max_conf >= BOUNDS_NEAR_THRESHOLD_FLOOR):
                sportify = _sportify_market(um, meta["home"], meta["away"])
                detail = next(
                    (d for d in pick.get("details", [])
                     if isinstance(d, dict) and "calibration_scope" in d),
                    {},
                )
                near_threshold.append({
                    "fixture_id": fid,
                    "match": f"{meta['home']} vs {meta['away']}",
                    "home": meta["home"],
                    "away": meta["away"],
                    "league": meta["league"],
                    "kickoff": meta["kickoff"],
                    "unified_market": um,
                    "sportybet_market": sportify["market"],
                    "sportybet_selection": sportify["selection"],
                    "line": sportify.get("line"),
                    "n_models": n_models,
                    "models": sorted(models),
                    "max_confidence": round(max_conf, 4),
                    "avg_confidence": round(avg_conf, 4),
                    "country": meta.get("country", ""),
                    "strength_rank": strength_rank,
                    "market_tier": None,
                    "decision_status": "watch",
                    "gate_reason": (
                        f"{max_conf:.2%} is within "
                        f"{solo_threshold - max_conf:.2%} of the {solo_threshold:.0%} gate"
                    ),
                    "global_calibrated_probability": detail.get("global_calibrated"),
                    "calibration_scope": detail.get("calibration_scope", "global"),
                    "league_adjustment": detail.get("league_adjustment"),
                })
            _reject(
                f"below solo threshold ({max_conf:.0%} < {solo_threshold:.0%})")
            continue

        gate_status[key] = {"in_pool": True, "reason": "in pool"}
        sportify = _sportify_market(um, meta["home"], meta["away"])

        # Score by consensus strength: models agreeing, then their probability.
        base_score = n_models * 1.0 + avg_conf
        league_prior = _bounds_1_4_league_prior(
            um, meta["league"], meta.get("country", ""),
            bounds_league_priors)
        league_prior_boost = league_prior["boost"] if league_prior else 0.0
        score = base_score + league_prior_boost

        ranked.append({
            "fixture_id": fid,
            "match": f"{meta['home']} vs {meta['away']}",
            "home": meta["home"],
            "away": meta["away"],
            "league": meta["league"],
            "kickoff": meta["kickoff"],
            "unified_market": pick["unified_market"],
            "sportybet_market": sportify["market"],
            "sportybet_selection": sportify["selection"],
            "line": sportify.get("line"),
            "n_models": n_models,
            "models": sorted(set(pick["models"])),
            "max_confidence": round(max_conf, 4),
            "avg_confidence": round(avg_conf, 4),
            "consensus_base_score": round(base_score, 4),
            "consensus_score": round(score, 4),
            "league_prior_boost": round(league_prior_boost, 4),
            "league_prior_rate": (
                round(league_prior["rate"], 4) if league_prior else None),
            "league_prior_matches": (
                league_prior["matches"] if league_prior else None),
            "country": meta.get("country", ""),
            "details": pick["details"],
            "strength_gate": strength_gate,
            "strength_rank": strength_rank,
            "market_tier": market_tier,
        })

    ranked.sort(key=lambda x: (-x["n_models"], -x["consensus_score"]))

    print(f"  {len(ranked)} consensus picks after filtering")
    by_tier = {4: 0, 3: 0, 2: 0, 1: 0}
    for r in ranked:
        by_tier[r["n_models"]] = by_tier.get(r["n_models"], 0) + 1
    print(f"  4-model: {by_tier.get(4,0)} | 3-model: {by_tier.get(3,0)} | "
          f"2-model: {by_tier.get(2,0)} | "
          f"1-model qualified: {by_tier.get(1,0)}")
    near_threshold.sort(key=lambda x: -x["avg_confidence"])
    return ranked, gate_status, near_threshold


def _build_bounds_1_4_shortlist(
        ranked: list[dict], limit: int = BOUNDS_1_4_SHORTLIST_LIMIT,
        per_league_limit: int = BOUNDS_1_4_PER_LEAGUE_LIMIT,
        end_date: str | None = None) -> list[dict]:
    """Top 1–4 events, capped so one league cannot dominate."""
    candidates = [dict(p) for p in ranked if (
        p.get("sportybet_market") == "Goal Bounds"
        and p.get("sportybet_selection") == "1-4"
        and (p.get("league_prior_matches") or 0) >= BOUNDS_1_4_MIN_HISTORY
        and (p.get("league_prior_rate") or 0) >= BOUNDS_1_4_TOP_RATE
        and _within_shortlist_end_date(p.get("kickoff"), end_date)
    )]
    candidates.sort(key=lambda p: (
        -p["league_prior_rate"],
        -p["avg_confidence"],
        -p["n_models"],
        p.get("kickoff", ""),
    ))
    shortlist = []
    league_counts = defaultdict(int)
    for pick in candidates:
        league_key = (pick.get("league"), pick.get("country"))
        if league_counts[league_key] >= per_league_limit:
            continue
        league_counts[league_key] += 1
        shortlist.append(pick)
        if len(shortlist) >= limit:
            break

    for rank, pick in enumerate(shortlist, 1):
        pick["bounds_rank"] = rank
    return shortlist


def dc_favourite_win(fixture: dict, favourite: str | None) -> float | None:
    """Dixon-Coles win chance for `favourite` from the fixture's DC goal rates
    (independent Poissons on lambda_home/lambda_away, no low-score correction)."""
    rows = [r for r in fixture.get("dixon_coles") or []
            if r.get("lambda_home") is not None and r.get("lambda_away") is not None]
    if not rows or favourite not in (fixture.get("home"), fixture.get("away")):
        return None
    lh, la = float(rows[0]["lambda_home"]), float(rows[0]["lambda_away"])
    if favourite != fixture.get("home"):
        lh, la = la, lh
    pf = [math.exp(-lh) * lh ** i / math.factorial(i) for i in range(11)]
    pd_ = [math.exp(-la) * la ** j / math.factorial(j) for j in range(11)]
    return round(sum(pf[i] * pd_[j] for i in range(11) for j in range(i)), 4)


def venue_gap(flat_gap: float, favourite_is_home: bool) -> float:
    """D137 Big dog gap: the home side counts BIG_DOG_HOME_EDGE Elo points stronger."""
    return flat_gap + BIG_DOG_HOME_EDGE if favourite_is_home else flat_gap - BIG_DOG_HOME_EDGE


def _build_elo_gap_shortlist(
        fixture_analysis: list[dict], league_volatility: dict,
        limit: int | None = ELO_GAP_SHORTLIST_LIMIT,
        per_league_limit: int = ELO_GAP_PER_LEAGUE_LIMIT,
        end_date: str | None = None,
        group: dict | None = None) -> list[dict]:
    """Large-Elo-gap fixtures from leagues whose historical upset rate is low.

    With `group` (the Big dog record), a league also needs its own trusted
    record (league_big_dogs_trusted). Without it (record failed to load) the
    old volatility-only rule applies and a warning is printed by the caller.
    """
    candidates = []
    for fixture in fixture_analysis:
        if group is not None and not big_dog_league_in(
                group, fixture.get("league"), fixture.get("country")):
            continue
        if not _within_shortlist_end_date(fixture.get("kickoff"), end_date):
            continue
        league_key = (fixture.get("league"), fixture.get("country") or "")
        volatility = league_volatility.get(league_key)
        if not volatility:
            continue
        if group is None:   # D81: upset rule only as fallback when no record loaded
            if volatility["matches"] < ELO_VOLATILITY_MIN_HISTORY:
                continue
            if volatility["upset_rate"] > ELO_LOW_VOLATILITY_MAX:
                continue

        # D137: a home favourite can qualify from a flat gap down to 250 - edge.
        mismatch = [m for m in fixture.get("mismatch", [])
                    if (m.get("gap") or 0) > ELO_HUGE_GAP_MIN - BIG_DOG_HOME_EDGE]
        if not mismatch:
            continue
        flat_gap = max(m["gap"] for m in mismatch)
        favourite = None
        favourite_rows = []
        for team in (fixture.get("home", ""), fixture.get("away", "")):
            team_rows = [m for m in mismatch
                         if str(m.get("market", "")).startswith(f"{team} ")]
            if team_rows:
                favourite = team
                favourite_rows = team_rows
                break
        if not favourite:
            continue
        gap = round(venue_gap(flat_gap, favourite == fixture.get("home")))
        if gap <= ELO_HUGE_GAP_MIN:
            continue
        underdog = (fixture.get("away") if favourite == fixture.get("home")
                    else fixture.get("home"))
        best_signal = max(favourite_rows, key=lambda m: m.get("probability", 0))
        win_signal = next(
            (m for m in favourite_rows
             if m.get("market") == f"{favourite} Win"),
            None,
        )
        candidates.append({
            "fixture_id": fixture["fixture_id"],
            "match": f"{fixture.get('home')} vs {fixture.get('away')}",
            "home": fixture.get("home"),
            "away": fixture.get("away"),
            "kickoff": fixture.get("kickoff"),
            "league": fixture.get("league"),
            "country": fixture.get("country", ""),
            "favourite": favourite,
            "underdog": underdog,
            "elo_gap": gap,                 # D137 venue-adjusted
            "elo_gap_flat": flat_gap,
            "gap_bin": f">{ELO_HUGE_GAP_MIN}",
            "league_upset_rate": volatility["upset_rate"],
            "league_matches": volatility["matches"],
            "best_signal": best_signal.get("market"),
            "best_signal_confidence": best_signal.get("probability"),
            "win_confidence": win_signal.get("probability") if win_signal else None,
        })
        dc_win = dc_favourite_win(fixture, favourite)
        candidates[-1]["dc_win_prob"] = dc_win
        candidates[-1]["dc_confirms"] = None if dc_win is None else dc_win >= BIG_DOG_DC_CONFIRM

    candidates.sort(key=lambda p: (
        -p["elo_gap"],
        p["league_upset_rate"],
        -(p["best_signal_confidence"] or 0),
        p.get("kickoff", ""),
    ))
    if limit is None:   # every qualifying Big dog fixture, uncapped and unranked
        return candidates
    return _cap_elo_gap_shortlist(candidates, limit, per_league_limit)


def _cap_elo_gap_shortlist(candidates: list[dict],
                           limit: int = ELO_GAP_SHORTLIST_LIMIT,
                           per_league_limit: int = ELO_GAP_PER_LEAGUE_LIMIT) -> list[dict]:
    """The published shortlist: top `limit` by gap, at most `per_league_limit` per league."""
    shortlist = []
    league_counts = defaultdict(int)
    for pick in candidates:
        league_key = (pick["league"], pick["country"])
        if league_counts[league_key] >= per_league_limit:
            continue
        league_counts[league_key] += 1
        shortlist.append(pick)
        if len(shortlist) >= limit:
            break
    for rank, pick in enumerate(shortlist, 1):
        pick["elo_gap_rank"] = rank
    return shortlist


def _within_shortlist_end_date(kickoff, end_date: str | None) -> bool:
    """Inclusive Vancouver-local date cutoff for curated report sections."""
    if not end_date:
        return True
    if not kickoff:
        return False
    try:
        timestamp = pd.Timestamp(kickoff)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        local_date = timestamp.tz_convert("America/Vancouver").date().isoformat()
        return local_date <= end_date
    except (TypeError, ValueError):
        return False


def _league_id_country_map(conn) -> tuple[dict, dict]:
    """(api_league_id→league_name, api_league_id→country_name) from the dim
    tables — dim.dim_leagues and dim.dim_countries are the source of truth
    (replaces the old metadata.yaml name-matching + hardcoded overrides)."""
    df = pd.read_sql("""
        SELECT l.api_league_id, l.league_name, c.country_name
        FROM dim.dim_leagues l
        JOIN dim.dim_countries c ON c.country_id = l.country_id
    """, conn)
    lmap = dict(zip(df["api_league_id"], df["league_name"]))
    lid_country = dict(zip(df["api_league_id"], df["country_name"]))
    return lmap, lid_country


def _warehouse_ids(row) -> dict:
    """Warehouse team/league ids for the export (None when not yet mapped).

    The frontend joins standings, H2H and crests on these. API-Football ids
    never leave the warehouse.
    """
    def _id(v):
        return None if pd.isna(v) else int(v)
    return {"home_team_id": _id(row["home_team_id"]),
            "away_team_id": _id(row["away_team_id"]),
            "league_id": _id(row["league_id"])}


def fixture_meta_from_db(fixture_ids, conn=None) -> dict:
    """{fid: {fixture_id, home, away, league, kickoff, country}} for given fixtures."""
    own = conn is None
    conn = conn or get_conn()
    try:
        ids = [int(f) for f in fixture_ids]
        if not ids:
            return {}
        lmap, lid_country = _league_id_country_map(conn)
        df = pd.read_sql("""
            SELECT fixture_id, home_team_name AS home, away_team_name AS away,
                   kickoff_utc AS kickoff, api_league_id,
                   home_team_id, away_team_id, league_id
            FROM raw.fact_fixtures WHERE fixture_id = ANY(%(ids)s)
        """, conn, params={"ids": ids})
        meta = {}
        for _, r in df.iterrows():
            fid = int(r["fixture_id"])
            lid = r["api_league_id"]
            meta[fid] = {"fixture_id": fid, "home": r["home"], "away": r["away"],
                         "league": lmap.get(lid, "Unknown"), "kickoff": str(r["kickoff"]),
                         "country": lid_country.get(lid, "") or "",
                         **_warehouse_ids(r)}
        return meta
    finally:
        if own:
            conn.close()


def _gates_by_fixture(gate_status: dict) -> dict:
    """(fid, market)->status  ==>  {fid: [{market, in_pool, reason}, ...]}."""
    out = {}
    for (fid, market), gs in gate_status.items():
        out.setdefault(fid, []).append({"market": market, **gs})
    for lst in out.values():
        lst.sort(key=lambda g: (not g["in_pool"], g["market"]))  # in-pool first
    return out


def _analysis_from_signals(signals: list[dict], fixture_meta: dict,
                           fixture_strength_all: dict,
                           gate_status: dict | None = None,
                           bayesian_analysis: dict[int, dict] | None = None) -> list[dict]:
    """Best-effort per-fixture analysis rebuilt from stored signals (for the JSON feed)."""
    gates = _gates_by_fixture(gate_status or {})
    fa = {}
    for fid, meta in fixture_meta.items():
        fa[fid] = {**meta,
                          "poisson": [], "mismatch": [], "bayesian": [], "dixon_coles": [],
                   "gates": gates.get(fid, []),
                   "strength": fixture_strength_all.get(fid)}
    for s in signals:
        entry = fa.get(s["fixture_id"])
        if not entry or s["model"] not in entry:
            continue
        entry[s["model"]].append({"market": s["unified_market"],
                                  "probability": s["probability"],
                                  **(s.get("detail") or {})})

    # The scheduled fan-in report is normally reconstructed from model_picks,
    # which intentionally contains only consensus-eligible rows. Replace that
    # lean Bayesian signal view with the full persisted belief audit when the
    # Bayesian task supplied it.
    for fid, sides in (bayesian_analysis or {}).items():
        entry = fa.get(int(fid))
        if not entry:
            continue
        entry["bayesian"] = []
        for side in ("home", "away"):
            for belief in sides.get(side, []):
                entry["bayesian"].append({"side": side, **belief})
    out = [v for v in fa.values()
           if v["poisson"] or v["mismatch"] or v["bayesian"] or v["dixon_coles"]]
    out.sort(key=lambda x: x.get("kickoff", ""))
    return out


def build_consensus_from_signals(signals: list[dict],
                                 run_ds: str | None = None,
                                 bayesian_analysis: dict[int, dict] | None = None,
                                 shortlist_end_date: str | None = None) -> dict:
    """Fan-in path: build the consensus pool and shortlists from already-computed model
    signals (read from raw.model_picks), with no model imports. Writes
    raw.consensus_pool when run_ds is given. See docs/engine_orchestration.md.
    """
    consensus = {}
    for s in signals:
        if s.get("probability") is None:
            continue
        key = (s["fixture_id"], s["unified_market"])
        c = consensus.setdefault(key, {"fixture_id": s["fixture_id"],
                                       "unified_market": s["unified_market"],
                                       "models": [], "confidences": [], "details": []})
        c["models"].append(s["model"])
        c["confidences"].append(s["probability"])
        if s.get("detail"):
            c["details"].append(s["detail"])

    fids = {s["fixture_id"] for s in signals}
    fids.update(int(fid) for fid in (bayesian_analysis or {}))
    conn = get_conn()
    try:
        fixture_meta = fixture_meta_from_db(fids, conn)
        fixture_strength_all = _load_fixture_strength_profiles(conn)
        bounds_league_priors = _load_bounds_1_4_league_priors(conn)
        league_elo_volatility = _load_league_elo_volatility(conn)
        league_event_rates, elo_gap_group = _load_comparison_context(
            conn, league_elo_volatility)
        team_tiers = _load_team_tiers(conn)
    finally:
        conn.close()

    high_scoring_fids = _high_scoring_fixture_ids(fixture_meta.keys())
    ranked, gate_status, near_threshold = _score_pool(
        consensus, fixture_meta, fixture_strength_all,
        high_scoring_fids, bounds_league_priors)
    analysis = _analysis_from_signals(signals, fixture_meta, fixture_strength_all,
                                      gate_status, bayesian_analysis)
    attach_league_comparisons(analysis, league_event_rates)
    attach_team_patterns(analysis)  # contract §3.4, D122
    attach_team_tiers(analysis, team_tiers)  # D138 context
    bounds_shortlist = _build_bounds_1_4_shortlist(
        ranked, end_date=shortlist_end_date)
    if elo_gap_group is None:
        print("  WARN Big dog record unavailable: Strength uses volatility rule only")
    # Strength standouts consider every qualifying Big dog fixture; the 40 /
    # 7-per-league caps only shape the published shortlist (D-fix 2026-10-17:
    # Forest v Arsenal, gap 252, fell off a capped list and lost its standout).
    big_dog_fixtures = _build_elo_gap_shortlist(
        analysis, league_elo_volatility, limit=None, group=elo_gap_group)
    attach_discover_picks(analysis, big_dog_fixtures, elo_gap_group,
                          league_event_rates)
    elo_gap_shortlist = _cap_elo_gap_shortlist(
        [p for p in big_dog_fixtures
         if _within_shortlist_end_date(p.get("kickoff"), shortlist_end_date)])

    if run_ds:
        from engine.eval.picks_store import write_consensus_pool
        n = write_consensus_pool(run_ds, ranked)
        print(f"  wrote {n} rows to raw.consensus_pool (run_ds={run_ds})")

    return {"pool": ranked, "near_threshold": near_threshold,
            "shortlist_end_date": shortlist_end_date,
            "bounds_1_4_shortlist": bounds_shortlist,
            "elo_gap_shortlist": elo_gap_shortlist,
            "elo_gap_group": elo_gap_group,
            "fixture_analysis": analysis}


def run_consensus(days: int = 10,
                  shortlist_end_date: str | None = None) -> dict:
    """Run all four models and build consensus picks."""

    # Poisson GLM retired 2026-10-11; empty frame keeps the merge code simple.
    poisson_picks = pd.DataFrame()

    # ── 2. Mismatch Elo ─────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  [2/4] MISMATCH ELO")
    print("=" * 60)
    mismatch_picks, fixture_strength = predict_mismatch(days=days)

    # ── 3. Bayesian Streaks ──────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  [3/4] BAYESIAN STREAKS")
    print("=" * 60)
    from engine.eval.bayesian_streaks import _get_conn
    conn = _get_conn()
    bayesian_by_fixture = get_fixture_beliefs(
        conn, days=days, include_below_threshold=True
    )
    conn.close()
    print(f"  {len(bayesian_by_fixture)} fixtures with Bayesian beliefs")

    # ── 4. Dixon-Coles ───────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  [4/4] DIXON-COLES")
    print("=" * 60)
    dixon_coles_picks = predict_dixon_coles(days=days)
    print(f"  {len(dixon_coles_picks)} Dixon-Coles picks")

    # ── 5. Build unified pick pool ───────────────────────────────────────
    print()
    print("=" * 60)
    print("  BUILDING CONSENSUS")
    print("=" * 60)

    # Collect fixture metadata
    fixture_meta = {}

    # From Poisson
    if not poisson_picks.empty:
        for _, row in poisson_picks.iterrows():
            fid = int(row["fixture_id"])
            if fid not in fixture_meta:
                fixture_meta[fid] = {
                    "fixture_id": fid,
                    "home": row["home"],
                    "away": row["away"],
                    "league": row["league"],
                    "kickoff": str(row["kickoff"]),
                }

    # From Mismatch
    for p in mismatch_picks:
        fid = p.fixture_id
        if fid not in fixture_meta:
            fixture_meta[fid] = {
                "fixture_id": fid,
                "home": p.home_team,
                "away": p.away_team,
                "league": p.league,
                "kickoff": str(p.kickoff),
            }

    # From Dixon-Coles
    if not dixon_coles_picks.empty:
        for _, row in dixon_coles_picks.iterrows():
            fid = int(row["fixture_id"])
            if fid not in fixture_meta:
                fixture_meta[fid] = {
                    "fixture_id": fid,
                    "home": row["home"],
                    "away": row["away"],
                    "league": row["league"],
                    "kickoff": str(row["kickoff"]),
                }

    # From Bayesian (need fixture info from DB)
    conn = _get_conn()
    if bayesian_by_fixture:
        fids = list(bayesian_by_fixture.keys())
        fid_str = ",".join(str(f) for f in fids)
        fix_df = pd.read_sql(f"""
            SELECT fixture_id, team_name AS home, opponent_name AS away,
                   league_name AS league, kickoff_utc AS kickoff
            FROM raw.engine_head_to_head
            WHERE fixture_id IN ({fid_str}) AND is_home
        """, conn)
        for _, row in fix_df.iterrows():
            fid = int(row["fixture_id"])
            if fid not in fixture_meta:
                fixture_meta[fid] = {
                    "fixture_id": fid,
                    "home": row["home"],
                    "away": row["away"],
                    "league": row["league"],
                    "kickoff": str(row["kickoff"]),
                }

    # Also load upcoming unplayed fixtures for Bayesian-only matches
    upcoming_df = pd.read_sql("""
        SELECT fixture_id, home_team_name AS home, away_team_name AS away,
               kickoff_utc AS kickoff, api_league_id
        FROM raw.engine_fixtures
        WHERE fixture_status = 'NS'
        ORDER BY kickoff_utc
    """, conn)
    # league name + country from the dim tables (single source of truth)
    lmap, _league_id_country = _league_id_country_map(conn)

    # Map fixture_id -> api_league_id for country lookup
    fid_league_id = dict(zip(upcoming_df["fixture_id"].astype(int),
                             upcoming_df["api_league_id"]))

    for _, row in upcoming_df.iterrows():
        fid = int(row["fixture_id"])
        if fid not in fixture_meta and fid in bayesian_by_fixture:
            fixture_meta[fid] = {
                "fixture_id": fid,
                "home": row["home"],
                "away": row["away"],
                "league": lmap.get(row.get("api_league_id", ""), "Unknown"),
                "kickoff": str(row["kickoff"]),
            }

    # Attach country and warehouse ids to all fixture metadata
    # (fixtures already in meta came from Poisson/Mismatch/Dixon-Coles).
    meta_ids = [int(f) for f in fixture_meta]
    all_ff = pd.read_sql("""
        SELECT fixture_id, api_league_id, home_team_id, away_team_id, league_id
        FROM raw.fact_fixtures WHERE fixture_id = ANY(%(ids)s)
    """, conn, params={"ids": meta_ids})
    ff_by_fid = {int(r["fixture_id"]): r for _, r in all_ff.iterrows()}
    for fid, meta in fixture_meta.items():
        r = ff_by_fid.get(fid)
        lid = r["api_league_id"] if r is not None else None
        meta["country"] = _league_id_country.get(lid, "") if lid else ""
        meta.update(_warehouse_ids(r) if r is not None else
                    {"home_team_id": None, "away_team_id": None, "league_id": None})

    conn.close()

    conn = _get_conn()
    try:
        fixture_strength_all = _load_fixture_strength_profiles(conn)
        bounds_league_priors = _load_bounds_1_4_league_priors(conn)
        league_elo_volatility = _load_league_elo_volatility(conn)
        league_event_rates, elo_gap_group = _load_comparison_context(
            conn, league_elo_volatility)
        team_tiers = _load_team_tiers(conn)
    finally:
        conn.close()

    # ── Build per-fixture, per-market pick objects ───────────────────────
    # Key: (fixture_id, unified_market) → {models: [...], max_conf, ...}
    consensus = {}

    def _add_pick(fid, unified, model, confidence, detail=None):
        key = (fid, unified)
        if key not in consensus:
            consensus[key] = {
                "fixture_id": fid,
                "unified_market": unified,
                "models": [],
                "confidences": [],
                "details": [],
            }
        consensus[key]["models"].append(model)
        consensus[key]["confidences"].append(confidence)
        if detail:
            consensus[key]["details"].append(detail)

    # Collect normalised signals from every model (shared with run_model)
    _home_away = {fid: (m["home"], m["away"]) for fid, m in fixture_meta.items()}
    bayesian_by_fixture = annotate_bayesian_analysis(
        bayesian_by_fixture, _home_away
    )
    _signals = (poisson_signals(poisson_picks)
                + mismatch_signals(mismatch_picks)
                + bayesian_signals(bayesian_by_fixture, _home_away)
                + dixon_signals(dixon_coles_picks))
    for sig in _signals:
        _add_pick(sig["fixture_id"], sig["unified_market"], sig["model"],
                  sig["probability"], sig["detail"])

    # ── Score and rank ──────────────────────────────────────────────────
    high_scoring_fids = _high_scoring_fixture_ids(fixture_meta.keys())
    ranked, gate_status, near_threshold = _score_pool(
        consensus, fixture_meta, fixture_strength_all,
        high_scoring_fids, bounds_league_priors)
    gates_by_fid = _gates_by_fixture(gate_status)

    # ── Per-fixture analysis (all raw model signals) ──────────────────
    fixture_analysis = {}
    for fid, meta in fixture_meta.items():
        fixture_analysis[fid] = {
            **meta,
            "gates": gates_by_fid.get(fid, []),
            "poisson": [],
            "mismatch": [],
            "bayesian": [],
            "dixon_coles": [],
        }

    if not poisson_picks.empty:
        for _, row in poisson_picks.iterrows():
            fid = int(row["fixture_id"])
            if fid in fixture_analysis:
                fixture_analysis[fid]["poisson"].append({
                    "market": f"{row['direction']} {row['line']}",
                    "probability": round(float(row["probability"]), 4),
                    "tier": _tier(row["probability"]),
                })

    for p in mismatch_picks:
        fid = p.fixture_id
        if fid in fixture_analysis:
            entry = {
                "market": p.market,
                "label": _market_label(p.market, p.favourite),
                "confidence": round(p.confidence, 4),
                "favourite": p.favourite,
                "gap": p.gap,
                "gap_bin": p.gap_bin,
            }
            if hasattr(p, "fav_attack_pct") and p.fav_attack_pct:
                entry["fav_atk"] = p.fav_attack_pct
            if hasattr(p, "dog_defense_pct") and p.dog_defense_pct:
                entry["dog_def"] = p.dog_defense_pct
            fixture_analysis[fid]["mismatch"].append(entry)

    for fid, sides in bayesian_by_fixture.items():
        if fid not in fixture_analysis:
            continue
        for side in ("home", "away"):
            for b in sides.get(side, []):
                fixture_analysis[fid]["bayesian"].append({
                    **b,
                    "side": side,
                    "posterior": round(b["posterior"], 4),
                    "posterior_raw": round(b["posterior_raw"], 4),
                    "posterior_calibrated": round(
                        b["posterior_calibrated"], 4
                    ),
                    "base_rate": round(b["base_rate"], 4),
                })

    if not dixon_coles_picks.empty:
        for _, row in dixon_coles_picks.iterrows():
            fid = int(row["fixture_id"])
            if fid in fixture_analysis:
                fixture_analysis[fid]["dixon_coles"].append({
                    "market": row["market"],
                    "probability": round(float(row["probability"]), 4),
                    "raw_probability": round(float(row["raw_probability"]), 4),
                    "tier": _tier(row["probability"]),
                    "lambda_home": row["lambda_home"],
                    "lambda_away": row["lambda_away"],
                })

    for fid, fa in fixture_analysis.items():
        fa["strength"] = fixture_strength_all.get(fid)

    analysis_list = [v for v in fixture_analysis.values()
                     if v["poisson"] or v["mismatch"] or v["bayesian"]
                     or v["dixon_coles"]]
    analysis_list.sort(key=lambda x: x.get("kickoff", ""))
    attach_league_comparisons(analysis_list, league_event_rates)
    attach_team_patterns(analysis_list)  # contract §3.4, D122
    attach_team_tiers(analysis_list, team_tiers)  # D138 context

    # ── Shortlists ──────────────────────────────────────────────────
    bounds_shortlist = _build_bounds_1_4_shortlist(
        ranked, end_date=shortlist_end_date)
    if elo_gap_group is None:
        print("  WARN Big dog record unavailable: Strength uses volatility rule only")
    # Strength standouts consider every qualifying Big dog fixture; the 40 /
    # 7-per-league caps only shape the published shortlist (D-fix 2026-10-17:
    # Forest v Arsenal, gap 252, fell off a capped list and lost its standout).
    big_dog_fixtures = _build_elo_gap_shortlist(
        analysis_list, league_elo_volatility, limit=None, group=elo_gap_group)
    attach_discover_picks(analysis_list, big_dog_fixtures, elo_gap_group,
                          league_event_rates)
    elo_gap_shortlist = _cap_elo_gap_shortlist(
        [p for p in big_dog_fixtures
         if _within_shortlist_end_date(p.get("kickoff"), shortlist_end_date)])
    return {"pool": ranked, "near_threshold": near_threshold,
            "shortlist_end_date": shortlist_end_date,
            "bounds_1_4_shortlist": bounds_shortlist,
            "elo_gap_shortlist": elo_gap_shortlist,
            "elo_gap_group": elo_gap_group,
            "fixture_analysis": analysis_list}


def save_slips(
    result: dict,
    run_ds: str | None = None,
    output_dir: Path | None = None,
    record_history: bool = False,
    standout_timezone: str = "UTC",
) -> str:
    # Name by an explicit run date fixed at the start of the run, not now(), so a
    # single run that straddles UTC midnight can't split output across two files.
    ds = run_ds or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = (output_dir or Path(__file__).parent) / f"consensus_slips_{ds}.json"

    pool_items = []
    for p in result["pool"]:
        pool_items.append({
            "fixture_id": p["fixture_id"],
            "match": p["match"],
            "home": p["home"],
            "away": p["away"],
            "kickoff": p["kickoff"],
            "league": p["league"],
            "sportybet_market": p["sportybet_market"],
            "sportybet_selection": p["sportybet_selection"],
            "line": p.get("line"),
            "n_models": p["n_models"],
            "models": p["models"],
            "avg_confidence": p["avg_confidence"],
            "max_confidence": p["max_confidence"],
            "country": p.get("country", ""),
            "strength_gate": p.get("strength_gate"),
            "strength_rank": p.get("strength_rank"),
            "market_tier": p.get("market_tier"),
            "consensus_base_score": p.get("consensus_base_score"),
            "consensus_score": p.get("consensus_score"),
            "league_prior_boost": p.get("league_prior_boost", 0.0),
            "league_prior_rate": p.get("league_prior_rate"),
            "league_prior_matches": p.get("league_prior_matches"),
        })

    near_threshold_items = []
    for p in result.get("near_threshold", []):
        near_threshold_items.append({
            "fixture_id": p["fixture_id"],
            "match": p["match"],
            "home": p["home"],
            "away": p["away"],
            "kickoff": p["kickoff"],
            "league": p["league"],
            "country": p.get("country", ""),
            "sportybet_market": p["sportybet_market"],
            "sportybet_selection": p["sportybet_selection"],
            "line": p.get("line"),
            "n_models": p["n_models"],
            "models": p["models"],
            "avg_confidence": p["avg_confidence"],
            "max_confidence": p["max_confidence"],
            "strength_rank": p.get("strength_rank"),
            "decision_status": "watch",
            "gate_reason": p.get("gate_reason"),
            "global_calibrated_probability": p.get("global_calibrated_probability"),
            "calibration_scope": p.get("calibration_scope", "global"),
            "league_adjustment": p.get("league_adjustment"),
        })

    generated_at = datetime.now(timezone.utc)
    generated_at_iso = generated_at.isoformat().replace("+00:00", "Z")
    output = {
        "run_ds": ds,
        "generated": generated_at.strftime("%Y-%m-%d %H:%M UTC"),
        "shortlist_end_date": result.get("shortlist_end_date"),
        "models": ["poisson_glm", "mismatch_elo", "bayesian_streaks",
                   "dixon_coles"],
        "thresholds": {
            "solo": SOLO_THRESHOLD,
            "bounds_solo": BOUNDS_SOLO_THRESHOLD,
            "bounds_near_threshold_floor": BOUNDS_NEAR_THRESHOLD_FLOOR,
            "elo_huge_gap_min": ELO_HUGE_GAP_MIN,
            "big_dog_home_edge": BIG_DOG_HOME_EDGE,
            "elo_low_volatility_max": ELO_LOW_VOLATILITY_MAX,
            "duo": DUO_THRESHOLD,
            "trio": TRIO_THRESHOLD,
            "over15_dc_poisson": OVER15_DC_POISSON_THRESHOLD,
            "under35_strength": {
                "max_avg_attack_pct": UNDER35_MAX_AVG_ATTACK_PCT,
                "max_single_attack_pct": UNDER35_MAX_SINGLE_ATTACK_PCT,
                "min_avg_defense_pct": UNDER35_MIN_AVG_DEFENSE_PCT,
                "min_single_defense_pct": UNDER35_MIN_SINGLE_DEFENSE_PCT,
            },
            "under35_tiers": {
                "A": UNDER35_TIER_A_THRESHOLD,
                "B": UNDER35_TIER_B_THRESHOLD,
                "C": UNDER35_TIER_C_THRESHOLD,
            },
        },
        "pool_size": len(result["pool"]),
        "pool": pool_items,
        "near_threshold_size": len(near_threshold_items),
        "near_threshold": near_threshold_items,
        "bounds_1_4_shortlist": [{
            "bounds_rank": p["bounds_rank"],
            "fixture_id": p["fixture_id"],
            "match": p["match"],
            "home": p["home"],
            "away": p["away"],
            "kickoff": p["kickoff"],
            "league": p["league"],
            "country": p.get("country", ""),
            "sportybet_market": p["sportybet_market"],
            "sportybet_selection": p["sportybet_selection"],
            "n_models": p["n_models"],
            "models": p["models"],
            "avg_confidence": p["avg_confidence"],
            "max_confidence": p["max_confidence"],
            "league_hit_rate": p["league_prior_rate"],
            "league_matches": p["league_prior_matches"],
        } for p in result.get("bounds_1_4_shortlist", [])],
        "elo_gap_shortlist": [{
            "elo_gap_rank": p["elo_gap_rank"],
            "fixture_id": p["fixture_id"],
            "match": p["match"],
            "home": p["home"],
            "away": p["away"],
            "kickoff": p["kickoff"],
            "league": p["league"],
            "country": p.get("country", ""),
            "favourite": p["favourite"],
            "underdog": p["underdog"],
            "elo_gap": p["elo_gap"],
            "elo_gap_flat": p.get("elo_gap_flat"),
            "gap_bin": p["gap_bin"],
            "league_upset_rate": p["league_upset_rate"],
            "league_matches": p["league_matches"],
            "best_signal": p["best_signal"],
            "best_signal_confidence": p["best_signal_confidence"],
            "win_confidence": p.get("win_confidence"),
            "dc_win_prob": p.get("dc_win_prob"),      # D136 shadow
            "dc_confirms": p.get("dc_confirms"),
            "group_win_rate": p.get("group_win_rate"),
            "group_rate_source": p.get("group_rate_source"),
            "league_win_rate": p.get("league_win_rate"),
            "league_win_context": p.get("league_win_context"),
            "discover_score": p.get("discover_score"),
            "league_big_dog_matches": p.get("league_big_dog_matches"),
            "league_big_dog_won": p.get("league_big_dog_won"),
            "league_big_dog_win_rate": p.get("league_big_dog_win_rate"),
            "league_big_dog_win_rate_low": p.get("league_big_dog_win_rate_low"),
            "league_record_ok": p.get("league_record_ok"),
        } for p in result.get("elo_gap_shortlist", [])],
        "elo_gap_group": result.get("elo_gap_group"),
    }

    output["fixture_analysis"] = result.get("fixture_analysis", [])
    output["standouts"] = select_rolling_standouts(
        output["fixture_analysis"], now=generated_at, timezone_name=standout_timezone)
    output["discover_sections"] = select_rolling_sections(
        output["fixture_analysis"], now=generated_at, timezone_name=standout_timezone)
    # Each curated section is rebuilt from the current run's ranked pool and
    # fixture analysis. Persist an auditable refresh marker even when a section
    # legitimately has zero qualifying events.
    output["shortlist_refresh"] = {
        "run_ds": ds,
        "generated_at": generated_at_iso,
        "sections": {
            key: {
                "count": len(output[key]),
                "updated_at": generated_at_iso,
            }
            for key in (
                "bounds_1_4_shortlist",
                "elo_gap_shortlist",
            )
        },
    }

    # CLI publications are captured before the mutable dashboard feed is replaced.
    # Tests/offline callers opt in explicitly; archive failures stop publication.
    if record_history:
        from engine.eval.forecast_history import capture, init_schema
        init_schema()
        capture(output)
        # Page-ready match sections for the API (raw.match_analysis), written
        # atomically and marked complete before the JSON file is replaced.
        from engine.eval.match_store import write_match_analysis
        n_match = write_match_analysis(ds, output)
        print(f"  wrote {n_match} match pages to raw.match_analysis (run_ds={ds})")
        try:   # D136 shadow log; monitoring must never block publication
            from engine.eval.big_dog_watch import write_big_dog_watch
            n_bd = write_big_dog_watch(ds, output.get("elo_gap_shortlist") or [])
            print(f"  logged {n_bd} Big dogs to raw.big_dog_watch")
        except Exception as exc:
            print(f"  WARN Big dog watch not logged: {type(exc).__name__}: {exc}")

    # Publish atomically so the dashboard never reads a half-written daily file.
    temp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(temp_path, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, default=str)
    temp_path.replace(out_path)

    print(f"\nConsensus feed saved to {out_path}")
    return str(out_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-model consensus pool, shortlists and daily feed")
    parser.add_argument("--days", type=int, default=10, help="Days ahead to scan")
    parser.add_argument("--from-picks", action="store_true",
                        help="Fan-in mode: build from raw.model_picks (set by run_model) "
                             "instead of running the 4 models in-process")
    parser.add_argument("--run-ds", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        help="Run date; used for --from-picks and the output filename "
                             "(fixed once, so a midnight rollover can't split output)")
    parser.add_argument("--shortlist-end-date", default=None,
                        help="Inclusive Vancouver-local cutoff for curated lists (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.from_picks:
        from engine.eval.picks_store import read_model_analysis, read_model_picks
        signals = read_model_picks(args.run_ds)
        bayesian_analysis = read_model_analysis(args.run_ds, "bayesian")
        print(f"Fan-in: {len(signals)} signals from raw.model_picks (run_ds={args.run_ds})")
        print(f"Fan-in: {len(bayesian_analysis)} Bayesian fixture audit payloads")
        result = build_consensus_from_signals(
            signals, run_ds=args.run_ds, bayesian_analysis=bayesian_analysis,
            shortlist_end_date=args.shortlist_end_date)
    else:
        result = run_consensus(days=args.days,
                               shortlist_end_date=args.shortlist_end_date)
    print(f"Pool: {len(result['pool'])} picks, {len(result['fixture_analysis'])} fixtures analysed")
    save_slips(result, run_ds=args.run_ds, record_history=True)
    save_big_dog_state(result.get("elo_gap_group"))
