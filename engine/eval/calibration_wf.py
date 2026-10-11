"""
Walk-forward calibration backtest — no data leakage.

Splits 6 months of history into 2-week test folds. For each fold,
trains models ONLY on data before that fold's start date, then predicts
that fold's completed fixtures. De-duplicates to one prediction per
(fixture, market) and compares to actual outcomes.

Usage:
    python -m engine.eval.calibration_wf
    python -m engine.eval.calibration_wf --months 6 --fold-days 14
"""

from __future__ import annotations

import argparse
import json
import warnings
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

from engine.eval.bayesian_streaks import (
    BETTING_EVENTS,
    MIN_POSTERIOR_INTERESTING,
    PRIOR_STRENGTH,
    apply_streak_boost,
    compute_beliefs,
    compute_league_base_rates,
)
from engine.eval.calibration_maps import (
    DC_MARKET_KEYS,
    DC_POOLED_KEY,
    MAPS_PATH,
    dc_calibration_key,
    apply_logit_adjustment,
    apply_points,
    fit_league_logit_adjustments,
    fit_pava_map,
)
from etl.src.logger import get_logger

logger = get_logger(__name__)

EVENTS_TABLE = "raw.fact_head_to_head_events"
H2H_TABLE = "raw.fact_head_to_head"
ELO_TABLE = "elo.team_elo_history"
from engine.eval.db import FIXTURES_TABLE  # noqa: E402  (warehouse ids)

# Folds record RAW model probabilities down to these floors (below the live
# betting gates) so the calibration fit has support around the thresholds.
# Live gates are applied later, at evaluation time only.
RECORD_FLOOR_BAYESIAN = 0.60
RECORD_FLOOR_POISSON = 0.55
RECORD_FLOOR_MISMATCH = 0.60
RECORD_FLOOR_DC = 0.55

MODELS = ("bayesian", "poisson", "mismatch", "dixon_coles")
DC_BOUNDS_1_4_MARKET = "Match Goals 1-4"
DC_BOUNDS_1_4_CALIBRATION_KEY = "dixon_coles_bounds_1_4"
CALIBRATION_KEYS = (*MODELS, *DC_MARKET_KEYS)
LEAGUE_ADJUSTED_CALIBRATION_KEYS = (
    "mismatch",
    DC_BOUNDS_1_4_CALIBRATION_KEY,
)

# Fraction of folds used to FIT calibration maps; the rest are held out
# for evaluation so the report never grades a map on the data that fit it.
CAL_FRAC_DEFAULT = 0.4
LEAGUE_CALIBRATION_MIN_N = 30
# Per-market DC maps fit on smaller samples; a 150-prediction minimum per bin
# kept held-out overstatement in thin markets (BTTS, team win, 2.5 lines) to
# ~5 pts vs ~8 with the default 50 (numbers audit, 2026-10-03).
DC_MARKET_MIN_BIN_N = 150
LEAGUE_CALIBRATION_PRIOR_STRENGTH = 200


def _get_conn():
    from engine.eval.db import get_conn
    return get_conn()


def _actual_outcome(row: pd.Series, market: str) -> bool | None:
    hg = row.get("goals_home_ft")
    ag = row.get("goals_away_ft")
    if hg is None or ag is None:
        return None
    hg, ag = int(hg), int(ag)
    total = hg + ag

    if market == DC_BOUNDS_1_4_MARKET:
        return 1 <= total <= 4
    if "Match Over" in market:
        return total > float(market.split()[-1])
    if "Match Under" in market:
        return total < float(market.split()[-1])
    if "Over 0.5" in market:
        team = market.replace(" Over 0.5", "").strip()
        if team == row.get("home_team_name", ""):
            return hg >= 1
        if team == row.get("away_team_name", ""):
            return ag >= 1
        return None
    if "Over 1.5" in market and "Match" not in market:
        team = market.replace(" Over 1.5", "").strip()
        if team == row.get("home_team_name", ""):
            return hg >= 2
        if team == row.get("away_team_name", ""):
            return ag >= 2
        return None
    if "Win" in market:
        team = market.replace(" Win", "").strip()
        if team == row.get("home_team_name", ""):
            return hg > ag
        if team == row.get("away_team_name", ""):
            return ag > hg
        return None
    if "BTTS" in market:
        return hg >= 1 and ag >= 1
    return None


def _calibration_key(prediction: dict) -> str:
    """Return the map fitted/applied to one raw prediction."""
    if prediction["model"] == "dixon_coles":
        return dc_calibration_key(prediction["market"])
    return prediction["model"]


def _rows_for_key(predictions: list[dict], calibration_key: str) -> list[dict]:
    """Predictions used to fit one map.

    The pooled "dixon_coles" map is fitted on every DC prediction except
    Goals 1-4 (which has always had its own map), so it stays a meaningful
    fallback for market families with too little data of their own.
    """
    if calibration_key == DC_POOLED_KEY:
        return [p for p in predictions if p["model"] == "dixon_coles"
                and _calibration_key(p) != DC_BOUNDS_1_4_CALIBRATION_KEY]
    return [p for p in predictions if _calibration_key(p) == calibration_key]


def _fit_map(rows: list[dict], calibration_key: str) -> list[list[float]]:
    """Fit one calibration map with the bin-size rule for its key."""
    min_bin_n = (DC_MARKET_MIN_BIN_N
                 if calibration_key in DC_MARKET_KEYS
                 and calibration_key != DC_BOUNDS_1_4_CALIBRATION_KEY
                 else 50)
    return fit_pava_map([p["predicted_prob_raw"] for p in rows],
                        [float(p["actual"]) for p in rows],
                        min_bin_n=min_bin_n)


def _points_for(maps: dict, calibration_key: str) -> list[list[float]]:
    """Map points for a key, falling back to the pooled DC map for DC keys."""
    points = maps.get(calibration_key) or []
    if not points and calibration_key in DC_MARKET_KEYS:
        points = maps.get(DC_POOLED_KEY) or []
    return points


# ── Data loaders with date cutoff ──────────────────────────────────────

def load_all_fixtures(conn, months: int = 6) -> pd.DataFrame:
    cutoff = datetime.now(timezone.utc) - timedelta(days=months * 30)
    # Warehouse team ids. Fixtures whose teams are not in the dimensions are
    # excluded: no model can score them (previously their API ids were looked
    # up as warehouse ids and picked up unrelated clubs' history).
    df = pd.read_sql(f"""
        SELECT fixture_id, home_team_id, home_team_name,
               away_team_id, away_team_name,
               home_team_fulltime_goal AS goals_home_ft,
               away_team_fulltime_goal AS goals_away_ft,
               kickoff_utc, api_league_id, fixture_status
        FROM {FIXTURES_TABLE}
        WHERE fixture_status = 'FT'
          AND kickoff_utc >= %(cutoff)s
          AND home_team_id IS NOT NULL AND away_team_id IS NOT NULL
        ORDER BY kickoff_utc
    """, conn, params={"cutoff": cutoff})
    for col in ("home_team_id", "away_team_id"):
        df[col] = df[col].astype(int)
    return df


# Season-at-cutoff scoping: the live model filters events to is_current,
# but is_current reflects "now" — for a historical fold the leak-free
# equivalent is each league's season in progress at the fold's cutoff
# (most recent dim_league_seasons.start_date <= cutoff).
_SEASON_AT_CUTOFF_CTE = """
    WITH ls_at_cutoff AS (
        SELECT DISTINCT ON (league_id) league_season_id, league_id
        FROM dim.dim_league_seasons
        WHERE start_date <= %(cutoff)s
        ORDER BY league_id, start_date DESC
    )
"""


def _load_event_history_before(conn, cutoff_dt, context: str) -> pd.DataFrame:
    """Bayesian event history BEFORE cutoff, scoped to the season in
    progress at the cutoff (mirrors the live current-season-only model)."""
    return pd.read_sql(f"""
        {_SEASON_AT_CUTOFF_CTE}
        SELECT dl.league_name || ' (' || dc.country_name || ')' AS league,
               e.team_id, e.team_name,
               e.event_name, e.event_flag, e.kickoff_utc, e.fixture_id
        FROM {EVENTS_TABLE} e
        JOIN ls_at_cutoff la ON la.league_season_id = e.league_season_id
        JOIN dim.dim_leagues dl ON dl.league_id = la.league_id
        JOIN dim.dim_countries dc ON dc.country_id = dl.country_id
        WHERE e.h2h_context_scope = %(context)s
          AND e.kickoff_utc < %(cutoff)s
        ORDER BY e.team_id, e.event_name, e.kickoff_utc, e.fixture_id
    """, conn, params={"context": context, "cutoff": cutoff_dt})


def _load_base_rates_before(conn, cutoff_dt, context: str) -> pd.DataFrame:
    """League base rates from season-at-cutoff events BEFORE cutoff only."""
    return pd.read_sql(f"""
        {_SEASON_AT_CUTOFF_CTE}
        SELECT dl.league_name || ' (' || dc.country_name || ')' AS league,
               e.event_name,
               AVG(e.event_flag::float) AS base_rate, COUNT(*) AS n
        FROM {EVENTS_TABLE} e
        JOIN ls_at_cutoff la ON la.league_season_id = e.league_season_id
        JOIN dim.dim_leagues dl ON dl.league_id = la.league_id
        JOIN dim.dim_countries dc ON dc.country_id = dl.country_id
        WHERE e.h2h_context_scope = %(context)s
          AND e.kickoff_utc < %(cutoff)s
        GROUP BY 1, e.event_name
    """, conn, params={"context": context, "cutoff": cutoff_dt})


def _load_league_map(conn) -> dict:
    """api_league_id -> league_name for leagues with played history."""
    league_map_df = pd.read_sql(f"""
        SELECT DISTINCT api_league_id, league_name AS league
        FROM {FIXTURES_TABLE}
        WHERE is_played AND league_name IS NOT NULL
    """, conn)
    return dict(zip(league_map_df["api_league_id"], league_map_df["league"]))


def _load_historical_h2h_before(conn, cutoff_dt) -> pd.DataFrame:
    """Load fact_head_to_head with Elo for played fixtures BEFORE cutoff."""
    return pd.read_sql(f"""
        SELECT f.fixture_id, f.kickoff_utc, f.season, f.league_name AS league,
               rf.api_league_id AS league_id,
               f.team_id AS home_team_id, f.opponent_id AS away_team_id,
               f.team_name AS home_team, f.opponent_name AS away_team,
               f.goals_for AS home_goals, f.goals_against AS away_goals,
               COALESCE(eh.rating_before_flat, eh.rating_before) AS home_elo,
               COALESCE(ea.rating_before_flat, ea.rating_before) AS away_elo
        FROM {H2H_TABLE} f
        JOIN {FIXTURES_TABLE} rf ON rf.fixture_id = f.fixture_id
        JOIN {ELO_TABLE} eh ON eh.team_id = f.team_id AND eh.fixture_id = f.fixture_id
        JOIN {ELO_TABLE} ea ON ea.team_id = f.opponent_id AND ea.fixture_id = f.fixture_id
        WHERE f.is_played AND f.is_home
          AND f.kickoff_utc < %(cutoff)s
        ORDER BY f.kickoff_utc
    """, conn, params={"cutoff": cutoff_dt})


def _load_elo_before(conn, cutoff_dt, team_ids: list) -> dict:
    """Latest Elo per warehouse team_id using only data before cutoff."""
    mapped = [int(t) for t in team_ids]
    if not mapped:
        return {}
    placeholders = ",".join(["%s"] * len(mapped))
    params = mapped + [cutoff_dt]
    df = pd.read_sql(f"""
        SELECT DISTINCT ON (team_id)
            team_id, COALESCE(rating_after_flat, rating_after) AS elo
        FROM {ELO_TABLE}
        WHERE team_id IN ({placeholders})
          AND kickoff_utc < %s
        ORDER BY team_id, kickoff_utc DESC
    """, conn, params=params)

    elo_map = dict(zip(df["team_id"], df["elo"]))
    return {t: float(elo_map[t]) for t in mapped if t in elo_map}


def _build_mismatch_hit_rates_before(conn, cutoff_dt) -> pd.DataFrame:
    """Build mismatch hit rates from fixtures BEFORE cutoff only."""
    from engine.eval.predict_mismatch import GAP_BINS, MIN_SAMPLE

    sql = f"""
        WITH matches AS (
            SELECT f.fixture_id,
                   rf.api_league_id,
                   f.league_name,
                   rf.country_name,
                   f.goals_for + f.goals_against AS total,
                   ABS(COALESCE(eh.rating_before_flat, eh.rating_before)
                     - COALESCE(ea.rating_before_flat, ea.rating_before)) AS gap,
                   CASE WHEN COALESCE(eh.rating_before_flat, eh.rating_before)
                           > COALESCE(ea.rating_before_flat, ea.rating_before)
                        THEN f.goals_for ELSE f.goals_against END AS fav_goals,
                   CASE WHEN COALESCE(eh.rating_before_flat, eh.rating_before)
                           > COALESCE(ea.rating_before_flat, ea.rating_before)
                        THEN f.goals_against ELSE f.goals_for END AS dog_goals
            FROM {H2H_TABLE} f
            JOIN {FIXTURES_TABLE} rf
              ON rf.fixture_id = f.fixture_id AND rf.country_name IS NOT NULL
            JOIN {ELO_TABLE} eh ON eh.team_id = f.team_id AND eh.fixture_id = f.fixture_id
            JOIN {ELO_TABLE} ea ON ea.team_id = f.opponent_id AND ea.fixture_id = f.fixture_id
            WHERE f.is_played AND f.is_home
              AND f.kickoff_utc < %(cutoff)s
        )
        SELECT api_league_id, league_name, country_name,
               gap, fav_goals, dog_goals, total
        FROM matches WHERE gap >= 100
    """
    df = pd.read_sql(sql, conn, params={"cutoff": cutoff_dt})
    if df.empty:
        return pd.DataFrame()

    rows = []
    for (api_league_id, league, country), grp in df.groupby(
        ["api_league_id", "league_name", "country_name"]
    ):
        for lo, hi, label in GAP_BINS:
            bin_df = grp[(grp["gap"] >= lo) & (grp["gap"] < hi)]
            n = len(bin_df)
            if n < MIN_SAMPLE:
                continue
            rows.append({
                "api_league_id": int(api_league_id),
                "league": league, "country": country,
                "gap_bin": label, "n": n,
                "fav_scores_pct": (bin_df["fav_goals"] >= 1).mean(),
                "fav_over15_pct": (bin_df["fav_goals"] >= 2).mean(),
                "over15_pct": (bin_df["total"] > 1.5).mean(),
                "over25_pct": (bin_df["total"] > 2.5).mean(),
                "fav_win_pct": (bin_df["fav_goals"] > bin_df["dog_goals"]).mean(),
                "draw_pct": (bin_df["fav_goals"] == bin_df["dog_goals"]).mean(),
                "avg_fav_goals": bin_df["fav_goals"].mean(),
                "avg_total": bin_df["total"].mean(),
            })

    for lo, hi, label in GAP_BINS:
        bin_df = df[(df["gap"] >= lo) & (df["gap"] < hi)]
        n = len(bin_df)
        if n < MIN_SAMPLE:
            continue
        rows.append({
            "api_league_id": None,
            "league": "__global__", "country": "__global__",
            "gap_bin": label, "n": n,
            "fav_scores_pct": (bin_df["fav_goals"] >= 1).mean(),
            "fav_over15_pct": (bin_df["fav_goals"] >= 2).mean(),
            "over15_pct": (bin_df["total"] > 1.5).mean(),
            "over25_pct": (bin_df["total"] > 2.5).mean(),
            "fav_win_pct": (bin_df["fav_goals"] > bin_df["dog_goals"]).mean(),
            "draw_pct": (bin_df["fav_goals"] == bin_df["dog_goals"]).mean(),
            "avg_fav_goals": bin_df["fav_goals"].mean(),
            "avg_total": bin_df["total"].mean(),
        })

    return pd.DataFrame(rows)


# ── Per-fold model runners ─────────────────────────────────────────────

def _bayesian_fold(conn, test_fixtures: pd.DataFrame, cutoff_dt) -> list[dict]:
    """Bayesian predictions using only events before cutoff.

    Mirrors the live model: season-at-cutoff history, per-event selection of
    the venue-context belief when mature (n >= VENUE_PREFER_MIN) else the
    overall belief, primaries only. Records RAW posteriors plus the streak so
    the eval gate can apply the live calibrate-then-boost order downstream.
    """
    from engine.eval.bayesian_streaks import _index_latest_by_event, VENUE_PREFER_MIN

    results = []
    all_fh_ids = set(test_fixtures["home_team_id"].tolist() +
                     test_fixtures["away_team_id"].tolist())

    idx_by_ctx: dict[str, dict] = {}
    for ctx in ("home", "away", "all"):
        event_df = _load_event_history_before(conn, cutoff_dt, ctx)
        event_df = event_df[
            event_df["team_id"].isin(all_fh_ids)
            & event_df["event_name"].isin(BETTING_EVENTS)
        ]
        if event_df.empty:
            idx_by_ctx[ctx] = {}
            continue
        base_rates = _load_base_rates_before(conn, cutoff_dt, ctx)
        beliefs = compute_beliefs(event_df, base_rates, context=ctx)
        idx_by_ctx[ctx] = _index_latest_by_event(beliefs)

    for _, fix in test_fixtures.iterrows():
        fid = int(fix["fixture_id"])
        for side in ("home", "away"):
            if side == "home":
                target_rf = fix["home_team_id"]
                team_name = fix["home_team_name"]
                opp_name = fix["away_team_name"]
            else:
                target_rf = fix["away_team_id"]
                team_name = fix["away_team_name"]
                opp_name = fix["home_team_name"]

            target_fh = target_rf   # warehouse id
            venue = idx_by_ctx.get(side, {}).get(target_fh, {})
            overall = idx_by_ctx.get("all", {}).get(target_fh, {})

            for event_name in set(venue) | set(overall):
                venue_b = venue.get(event_name)
                overall_b = overall.get(event_name)
                if venue_b is not None and venue_b.n_matches >= VENUE_PREFER_MIN:
                    b = venue_b
                else:
                    b = overall_b or venue_b
                if b.posterior < RECORD_FLOOR_BAYESIAN:
                    continue
                market = None
                if b.event_name == "Over_1_5":
                    market = "Match Over 1.5"
                elif b.event_name == "Over_2_5":
                    market = "Match Over 2.5"
                elif b.event_name == "Under_2_5":
                    market = "Match Under 2.5"
                elif b.event_name == "Under_3_5":
                    market = "Match Under 3.5"
                elif b.event_name == "score_1goal":
                    market = f"{team_name} Over 0.5"
                elif b.event_name == "score_2goals":
                    market = f"{team_name} Over 1.5"
                elif b.event_name == "concede_1":
                    market = f"{opp_name} Over 0.5"
                elif b.event_name == "BTTS":
                    market = "BTTS Yes"

                if market:
                    outcome = _actual_outcome(fix, market)
                    if outcome is not None:
                        results.append({
                            "fixture_id": fid,
                            "model": "bayesian",
                            "market": market,
                            "predicted_prob_raw": round(float(b.posterior), 4),
                            "streak": int(b.recent_streak),
                            "actual": outcome,
                        })
    return results


def _poisson_fold(conn, test_fixtures: pd.DataFrame, cutoff_dt,
                  league_map: dict) -> list[dict]:
    """Poisson predictions trained only on fixtures before cutoff.

    Mirrors live predict_upcoming exactly: per-cluster Poisson on OVER_COLS
    for overs, a separate global Poisson on UNDER_COLS for unders, and the
    live lines [1.5, 2.5, 3.5]. Records RAW probabilities — calibration is
    fitted/applied downstream, never inside the fold.
    """
    from engine.eval.predict_upcoming import (
        prepare_features, compute_form_features, compute_strength_features,
        compute_rest_days, assign_clusters_seasonal, _fit_poisson_lambda,
        _predict_poisson_over, OVER_COLS, UNDER_COLS, MIN_TRAIN_PER_CLUSTER,
    )
    from engine.eval.cluster_backtest import ELO_GAP_SENSITIVITY, _MEDIAN_SENSITIVITY

    results = []
    hist_raw = _load_historical_h2h_before(conn, cutoff_dt)
    if len(hist_raw) < 100:
        return results
    hist = compute_form_features(prepare_features(hist_raw))

    test_df = test_fixtures.copy()
    test_df["league"] = test_df["api_league_id"].map(league_map)
    test_df["league_id"] = test_df["api_league_id"]   # unique key for league clustering
    test_df = test_df[test_df["league"].notna()].copy()
    if test_df.empty:
        return results

    all_teams = set(test_df["home_team_id"].tolist() +
                    test_df["away_team_id"].tolist())
    elo_map = _load_elo_before(conn, cutoff_dt, list(all_teams))

    test_df["home_elo"] = test_df["home_team_id"].map(elo_map).fillna(1500.0)
    test_df["away_elo"] = test_df["away_team_id"].map(elo_map).fillna(1500.0)
    test_df["abs_gap"] = (test_df["home_elo"] - test_df["away_elo"]).abs()
    test_df["max_elo"] = test_df[["home_elo", "away_elo"]].max(axis=1)
    test_df["min_elo"] = test_df[["home_elo", "away_elo"]].min(axis=1)
    test_df["elo_sum"] = test_df["home_elo"] + test_df["away_elo"]
    test_df["home_elo_gap"] = test_df["home_elo"] - test_df["away_elo"]
    test_df["kickoff_utc"] = pd.to_datetime(test_df["kickoff_utc"], utc=True)

    sensitivity = test_df["league"].map(ELO_GAP_SENSITIVITY).fillna(_MEDIAN_SENSITIVITY)
    test_df["elo_gap_scaled"] = test_df["abs_gap"] * sensitivity

    test_s = compute_strength_features(hist, test_df)
    test_s = compute_rest_days(test_s)
    train_s = compute_strength_features(hist, hist)
    train_s = compute_rest_days(train_s)
    train_s["cluster"] = assign_clusters_seasonal(train_s, train_s)
    test_s["cluster"] = assign_clusters_seasonal(train_s, test_s)

    for line in [1.5, 2.5, 3.5]:
        # ── OVER: per-cluster Poisson on OVER_COLS (as live) ──
        clusters = sorted(train_s["cluster"].unique())
        over_probs = np.full(len(test_s), np.nan)

        for cl in clusters:
            tr_cl = train_s[train_s["cluster"] == cl]
            te_cl = test_s[test_s["cluster"] == cl]
            if len(tr_cl) < MIN_TRAIN_PER_CLUSTER or len(te_cl) == 0:
                continue
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                params = _fit_poisson_lambda(tr_cl["total_goals"].values,
                                             tr_cl[OVER_COLS].values)
            idx = te_cl.index
            probs = _predict_poisson_over(params, te_cl[OVER_COLS].values, line)
            over_probs[test_s.index.get_indexer(idx)] = probs

        nan_mask = np.isnan(over_probs)
        if nan_mask.any():
            over_probs[nan_mask] = (train_s["total_goals"] > line).mean()

        # ── UNDER: separate global Poisson on UNDER_COLS (as live) ──
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            params_u = _fit_poisson_lambda(train_s["total_goals"].values,
                                           train_s[UNDER_COLS].values)
        under_probs = 1.0 - _predict_poisson_over(
            params_u, test_s[UNDER_COLS].values, line)

        for i, (_, row) in enumerate(test_s.iterrows()):
            fid = int(row["fixture_id"])
            fix_row = test_fixtures[test_fixtures["fixture_id"] == fid]
            if fix_row.empty:
                continue
            fix_row = fix_row.iloc[0]

            for mkt, prob in [(f"Match Over {line}", float(over_probs[i])),
                              (f"Match Under {line}", float(under_probs[i]))]:
                if prob < RECORD_FLOOR_POISSON:
                    continue
                outcome = _actual_outcome(fix_row, mkt)
                if outcome is not None:
                    results.append({
                        "fixture_id": fid, "model": "poisson",
                        "market": mkt, "predicted_prob_raw": round(prob, 4),
                        "actual": outcome,
                    })
    return results


def _mismatch_fold(conn, test_fixtures: pd.DataFrame, cutoff_dt,
                   league_map: dict) -> list[dict]:
    """Mismatch predictions using hit rates computed only from pre-cutoff data."""
    from engine.eval.predict_mismatch import (
        _get_gap_bin, _load_h2h_records, _load_team_strength,
        _h2h_adjustment, _lookup_hit_rate, _strength_adjustment,
        MIN_CONFIDENCE, MIN_SAMPLE,
    )

    results = []
    hit_rates = _build_mismatch_hit_rates_before(conn, cutoff_dt)
    if hit_rates.empty:
        return results

    test_df = test_fixtures.copy()
    test_df["league"] = test_df["api_league_id"].map(league_map)
    test_df["league_id"] = test_df["api_league_id"]   # unique key for league clustering
    test_df = test_df[test_df["league"].notna()].copy()
    if test_df.empty:
        return results

    all_teams = set(test_df["home_team_id"].tolist() +
                    test_df["away_team_id"].tolist())
    elo_map = _load_elo_before(conn, cutoff_dt, list(all_teams))

    fixture_info = []
    for _, row in test_df.iterrows():
        h_elo = elo_map.get(row["home_team_id"], 1500.0)
        a_elo = elo_map.get(row["away_team_id"], 1500.0)
        gap = abs(h_elo - a_elo)
        if gap < 100:
            continue
        bin_info = _get_gap_bin(gap)
        if bin_info is None:
            continue
        fav_is_home = h_elo >= a_elo
        fav_rf = row["home_team_id"] if fav_is_home else row["away_team_id"]
        dog_rf = row["away_team_id"] if fav_is_home else row["home_team_id"]
        fixture_info.append((row, h_elo, a_elo, gap, bin_info,
                             fav_is_home, fav_rf, dog_rf))

    if not fixture_info:
        return results

    matchups = [(f[6], f[7]) for f in fixture_info]
    h2h_map = _load_h2h_records(conn, matchups, cutoff=cutoff_dt)
    strength_map = _load_team_strength(conn, cutoff=cutoff_dt)

    for row, h_elo, a_elo, gap, bin_info, fav_is_home, fav_rf, dog_rf in fixture_info:
        gap_bin, _, _ = bin_info
        fav_name = row["home_team_name"] if fav_is_home else row["away_team_name"]
        dog_name = row["away_team_name"] if fav_is_home else row["home_team_name"]

        h2h_w, h2h_d, h2h_l = h2h_map.get((fav_rf, dog_rf), (0, 0, 0))
        h2h_boost, _ = _h2h_adjustment(h2h_w, h2h_d, h2h_l)
        fav_strength = strength_map.get(fav_rf)
        dog_strength = strength_map.get(dog_rf)

        fix_row = test_fixtures[test_fixtures["fixture_id"] == row["fixture_id"]]
        if fix_row.empty:
            continue
        fix_row = fix_row.iloc[0]

        for mkt in ["fav_to_score", "fav_over_1.5", "over_1.5", "over_2.5"]:
            conf, n = _lookup_hit_rate(
                hit_rates,
                row.get("league", ""),
                gap_bin,
                mkt,
                api_league_id=int(row["api_league_id"]),
            )
            str_adj = _strength_adjustment(mkt, fav_strength, dog_strength)
            adjusted = max(min(conf + str_adj, 0.99), 0.0)
            if adjusted < RECORD_FLOOR_MISMATCH or n < MIN_SAMPLE:
                continue
            if mkt == "fav_to_score":
                market = f"{fav_name} Over 0.5"
            elif mkt == "fav_over_1.5":
                market = f"{fav_name} Over 1.5"
            elif mkt == "over_1.5":
                market = "Match Over 1.5"
            elif mkt == "over_2.5":
                market = "Match Over 2.5"
            else:
                continue

            outcome = _actual_outcome(fix_row, market)
            if outcome is not None:
                results.append({
                    "fixture_id": int(row["fixture_id"]),
                    "model": "mismatch", "market": market,
                    "predicted_prob_raw": round(adjusted, 4),
                    "actual": outcome,
                })

        win_conf, win_n = _lookup_hit_rate(
            hit_rates,
            row.get("league", ""),
            gap_bin,
            "fav_win",
            api_league_id=int(row["api_league_id"]),
        )
        adjusted_win = min(win_conf + h2h_boost, 0.95)
        if adjusted_win >= RECORD_FLOOR_MISMATCH and win_n >= MIN_SAMPLE:
            market = f"{fav_name} Win"
            outcome = _actual_outcome(fix_row, market)
            if outcome is not None:
                results.append({
                    "fixture_id": int(row["fixture_id"]),
                    "model": "mismatch", "market": market,
                    "predicted_prob_raw": round(adjusted_win, 4),
                    "actual": outcome,
                })

    return results


def _dixon_coles_fold(conn, test_fixtures: pd.DataFrame, cutoff_dt,
                      league_map: dict) -> list[dict]:
    """Dixon-Coles predictions from per-league fits on pre-cutoff matches."""
    from engine.eval.predict_dixon_coles import fit_league_dc, fixture_markets, blend_xg

    results = []
    hist = _load_historical_h2h_before(conn, cutoff_dt)
    if hist.empty:
        return results
    hist["kickoff_utc"] = pd.to_datetime(hist["kickoff_utc"], utc=True)
    hist = blend_xg(hist, conn)   # xG only for pre-cutoff matches → no leak

    test_df = test_fixtures.copy()
    test_df["league"] = test_df["api_league_id"].map(league_map)
    test_df["league_id"] = test_df["api_league_id"]   # unique key for league clustering
    test_df = test_df[test_df["league"].notna()].copy()
    if test_df.empty:
        return results

    ref_time = pd.Timestamp(cutoff_dt)
    # Match the live predictor exactly: league names are not unique across
    # countries (for example, several competitions are named Premier League).
    for api_league_id, te_lg in test_df.groupby("league_id"):
        lg_hist = hist[hist["league_id"] == api_league_id]
        params = fit_league_dc(lg_hist, ref_time)
        if params is None:
            continue
        for _, fix in te_lg.iterrows():
            home_h2h, away_h2h = fix["home_team_id"], fix["away_team_id"]
            fm = fixture_markets(params, home_h2h, away_h2h,
                                 fix["home_team_name"], fix["away_team_name"])
            if fm is None:
                continue
            for market, raw in fm["markets"].items():
                if raw < RECORD_FLOOR_DC:
                    continue
                outcome = _actual_outcome(fix, market)
                if outcome is not None:
                    results.append({
                        "fixture_id": int(fix["fixture_id"]),
                        "model": "dixon_coles",
                        "market": market,
                        "predicted_prob_raw": round(float(raw), 4),
                        "actual": outcome,
                    })
    return results


# ── De-duplication ─────────────────────────────────────────────────────

def deduplicate_predictions(preds: list[dict]) -> list[dict]:
    """One prediction per (fixture_id, market). Average confidence across models."""
    grouped = defaultdict(list)
    for p in preds:
        grouped[(p["fixture_id"], p["market"])].append(p)

    deduped = []
    for (fid, market), group in grouped.items():
        avg_prob = np.mean([p["predicted_prob"] for p in group])
        models = sorted(set(p["model"] for p in group))
        deduped.append({
            "fixture_id": fid,
            "market": market,
            "predicted_prob": round(float(avg_prob), 4),
            "actual": group[0]["actual"],
            "n_models": len(models),
            "models": ",".join(models),
            "home": group[0].get("home", ""),
            "away": group[0].get("away", ""),
            "score": group[0].get("score", ""),
            "kickoff": group[0].get("kickoff", ""),
            "api_league_id": group[0].get("api_league_id"),
            "league": group[0].get("league", ""),
            "calibration_scope": group[0].get("calibration_scope", "global"),
        })
    return deduped


# ── Calibration metrics ────────────────────────────────────────────────

def compute_calibration(predictions: list[dict], n_bins: int = 10) -> dict:
    df = pd.DataFrame(predictions)
    if df.empty:
        return {"n": 0}

    df["actual_int"] = df["actual"].astype(int)
    brier = float(((df["predicted_prob"] - df["actual_int"]) ** 2).mean())

    bins = np.linspace(0.5, 1.0, n_bins + 1)
    bin_data = []
    for i in range(len(bins) - 1):
        lo, hi = bins[i], bins[i + 1]
        mask = (df["predicted_prob"] >= lo) & (df["predicted_prob"] < hi)
        subset = df[mask]
        if len(subset) == 0:
            continue
        bin_data.append({
            "bin_lo": round(lo, 3), "bin_hi": round(hi, 3),
            "bin_mid": round((lo + hi) / 2, 3), "n": len(subset),
            "predicted_avg": round(float(subset["predicted_prob"].mean()), 4),
            "actual_rate": round(float(subset["actual_int"].mean()), 4),
            "gap": round(float(subset["predicted_prob"].mean() -
                               subset["actual_int"].mean()), 4),
        })

    per_model = {}
    if "models" in df.columns:
        # Explode to per-model stats
        all_models = set()
        for m in df["models"]:
            all_models.update(m.split(","))
        for model in sorted(all_models):
            grp = df[df["models"].str.contains(model)]
            if len(grp) == 0:
                continue
            model_brier = float(((grp["predicted_prob"] - grp["actual_int"]) ** 2).mean())
            per_model[model] = {
                "n": len(grp),
                "brier": round(model_brier, 4),
                "avg_predicted": round(float(grp["predicted_prob"].mean()), 4),
                "actual_hit_rate": round(float(grp["actual_int"].mean()), 4),
                "overconfidence": round(float(grp["predicted_prob"].mean() -
                                              grp["actual_int"].mean()), 4),
            }

    # Consensus tiers
    consensus_stats = {}
    if "n_models" in df.columns:
        for label, mask in [("trio", df["n_models"] >= 3),
                            ("duo", df["n_models"] == 2),
                            ("solo", df["n_models"] == 1)]:
            sub = df[mask]
            if len(sub) == 0:
                continue
            consensus_stats[label] = {
                "n": len(sub),
                "avg_predicted": round(float(sub["predicted_prob"].mean()), 4),
                "actual_hit_rate": round(float(sub["actual_int"].mean()), 4),
                "brier": round(float(((sub["predicted_prob"] -
                                       sub["actual_int"]) ** 2).mean()), 4),
            }

    # Market type breakdown
    def _market_type(m):
        if m == DC_BOUNDS_1_4_MARKET:
            return "bounds_1_4"
        if "Under" in m:
            return "under"
        if "Over 0.5" in m:
            return "team_scores"
        if "Over" in m:
            return "over_match"
        if "Win" in m:
            return "win"
        return "other"

    per_market = {}
    df["mtype"] = df["market"].apply(_market_type)
    for mt, grp in df.groupby("mtype"):
        per_market[mt] = {
            "n": len(grp),
            "avg_predicted": round(float(grp["predicted_prob"].mean()), 4),
            "actual_hit_rate": round(float(grp["actual_int"].mean()), 4),
            "brier": round(float(((grp["predicted_prob"] -
                                   grp["actual_int"]) ** 2).mean()), 4),
        }

    return {
        "n": len(df),
        "unique_fixtures": int(df["fixture_id"].nunique()),
        "brier_score": round(brier, 4),
        "avg_predicted": round(float(df["predicted_prob"].mean()), 4),
        "overall_hit_rate": round(float(df["actual_int"].mean()), 4),
        "overconfidence": round(float(df["predicted_prob"].mean() -
                                      df["actual_int"].mean()), 4),
        "calibration_bins": bin_data,
        "per_model": per_model,
        "consensus": consensus_stats,
        "per_market": per_market,
    }


# ── Base rate comparison ───────────────────────────────────────────────

def compute_base_rates(fixtures: pd.DataFrame) -> dict:
    n = len(fixtures)
    if n == 0:
        return {}
    hg = fixtures["goals_home_ft"].astype(int)
    ag = fixtures["goals_away_ft"].astype(int)
    total = hg + ag
    return {
        "n_fixtures": n,
        "home_scores": round(float((hg >= 1).mean()), 4),
        "away_scores": round(float((ag >= 1).mean()), 4),
        "over_1.5": round(float((total > 1.5).mean()), 4),
        "over_2.5": round(float((total > 2.5).mean()), 4),
        "under_3.5": round(float((total < 3.5).mean()), 4),
        "bounds_1_4": round(float(((total >= 1) & (total <= 4)).mean()), 4),
    }


# ── Main walk-forward runner ──────────────────────────────────────────

def run_walk_forward(months: int = 6, fold_days: int = 14,
                     cal_frac: float = CAL_FRAC_DEFAULT,
                     dump_raw: str | None = None) -> dict:
    conn = _get_conn()
    try:
        print("=" * 60)
        print("  WALK-FORWARD CALIBRATION (no data leakage)")
        print(f"  Period: last {months} months | fold size: {fold_days} days")
        print(f"  Calibration maps fitted on first {cal_frac:.0%} of folds,")
        print(f"  evaluated on the remainder only")
        print("=" * 60)

        print("\nLoading all completed fixtures...")
        all_fixtures = load_all_fixtures(conn, months=months)
        all_fixtures["kickoff_utc"] = pd.to_datetime(
            all_fixtures["kickoff_utc"], utc=True)
        print(f"  {len(all_fixtures)} fixtures loaded")

        print("Loading ID maps...")
        league_map = _load_league_map(conn)

        # Build folds: first 50% of the period is min training data,
        # then test in fold_days blocks
        min_date = all_fixtures["kickoff_utc"].min()
        max_date = all_fixtures["kickoff_utc"].max()
        total_days = (max_date - min_date).days

        # Start testing after at least 60 days of training data
        min_train_days = 60
        test_start = min_date + timedelta(days=min_train_days)

        folds = []
        current = test_start
        while current < max_date:
            fold_end = min(current + timedelta(days=fold_days), max_date)
            folds.append((current, fold_end))
            current = fold_end

        print(f"  {len(folds)} test folds (training cutoff moves forward)")
        print(f"  Training starts: {min_date.date()}")
        print(f"  Test starts:     {test_start.date()}")
        print(f"  Test ends:       {max_date.date()}")

        all_predictions = []
        all_test_fixtures = pd.DataFrame()

        for i, (fold_start, fold_end) in enumerate(folds):
            test_mask = ((all_fixtures["kickoff_utc"] >= fold_start) &
                         (all_fixtures["kickoff_utc"] < fold_end))
            test_fix = all_fixtures[test_mask].copy()
            if test_fix.empty:
                continue

            cutoff_dt = fold_start
            n_train = len(all_fixtures[all_fixtures["kickoff_utc"] < cutoff_dt])

            print(f"\n  Fold {i+1}/{len(folds)}: "
                  f"{fold_start.date()} → {fold_end.date()} | "
                  f"{len(test_fix)} test fixtures | "
                  f"{n_train} training fixtures")

            fold_preds = []

            # Bayesian
            try:
                bp = _bayesian_fold(conn, test_fix, cutoff_dt)
                fold_preds.extend(bp)
                print(f"    Bayesian: {len(bp)} raw predictions")
            except Exception as e:
                print(f"    Bayesian failed: {e}")

            # Poisson
            try:
                pp = _poisson_fold(conn, test_fix, cutoff_dt, league_map)
                fold_preds.extend(pp)
                print(f"    Poisson:  {len(pp)} raw predictions")
            except Exception as e:
                print(f"    Poisson failed: {e}")

            # Mismatch
            try:
                mp = _mismatch_fold(conn, test_fix, cutoff_dt, league_map)
                fold_preds.extend(mp)
                print(f"    Mismatch: {len(mp)} raw predictions")
            except Exception as e:
                print(f"    Mismatch failed: {e}")

            # Dixon-Coles
            try:
                dcp = _dixon_coles_fold(conn, test_fix, cutoff_dt, league_map)
                fold_preds.extend(dcp)
                print(f"    DixonColes: {len(dcp)} raw predictions")
            except Exception as e:
                print(f"    DixonColes failed: {e}")

            # Attach fixture info
            fix_lookup = test_fix.set_index("fixture_id").to_dict("index")
            for pred in fold_preds:
                finfo = fix_lookup.get(pred["fixture_id"], {})
                pred["home"] = finfo.get("home_team_name", "")
                pred["away"] = finfo.get("away_team_name", "")
                hg = int(finfo.get("goals_home_ft", 0))
                ag = int(finfo.get("goals_away_ft", 0))
                pred["score"] = f"{hg}-{ag}"
                pred["kickoff"] = str(finfo.get("kickoff_utc", ""))[:16]
                pred["api_league_id"] = finfo.get("api_league_id")
                pred["league"] = league_map.get(finfo.get("api_league_id"), "")
                pred["fold"] = i + 1

            test_fix["fold"] = i + 1
            all_predictions.extend(fold_preds)
            all_test_fixtures = pd.concat([all_test_fixtures, test_fix],
                                          ignore_index=True)

        print(f"\n{'─' * 60}")
        print(f"  Total raw predictions: {len(all_predictions)}")
        if dump_raw:
            # Diagnostic only (2026-10-03): lets top-band calibration be
            # inspected offline, e.g. engine/eval/top_band_check.py.
            with open(dump_raw, "w") as fh:
                json.dump(all_predictions, fh, default=str)
            print(f"  Raw predictions written to {dump_raw}")

        # ── Fit/eval fold split — maps are never graded on their own data ──
        n_folds = len(folds)
        n_fit = min(max(1, int(round(n_folds * cal_frac))), n_folds - 1)
        fit_preds = [p for p in all_predictions if p["fold"] <= n_fit]
        eval_preds = [p for p in all_predictions if p["fold"] > n_fit]
        print(f"  Calibration-fit folds 1-{n_fit}: {len(fit_preds)} predictions")
        print(f"  Evaluation folds {n_fit + 1}-{n_folds}: "
              f"{len(eval_preds)} predictions")

        print("\nFitting calibration maps (fit folds only)...")
        fit_maps = {}
        for calibration_key in CALIBRATION_KEYS:
            mp = _rows_for_key(fit_preds, calibration_key)
            points = _fit_map(mp, calibration_key)
            fit_maps[calibration_key] = points
            print(f"  {calibration_key:<26s} {len(mp):>6d} preds → "
                  f"{f'{len(points)} map points' if points else 'insufficient data (identity)'}")

        fit_league_adjustments_by_key = {}
        for calibration_key in LEAGUE_ADJUSTED_CALIBRATION_KEYS:
            fit_rows = [p for p in fit_preds
                        if _calibration_key(p) == calibration_key]
            adjustments = fit_league_logit_adjustments(
                fit_rows,
                fit_maps.get(calibration_key) or [],
                min_n=LEAGUE_CALIBRATION_MIN_N,
                prior_strength=LEAGUE_CALIBRATION_PRIOR_STRENGTH,
            )
            fit_league_adjustments_by_key[calibration_key] = adjustments
            print(f"  {calibration_key:<26s} "
                  f"{len(adjustments):>6d} league offsets "
                  f"(n≥{LEAGUE_CALIBRATION_MIN_N}, "
                  f"prior={LEAGUE_CALIBRATION_PRIOR_STRENGTH})")

        # ── Apply maps + LIVE gates to eval-fold predictions ──
        # Gates mirror production: bayesian raw >= MIN_POSTERIOR_INTERESTING,
        # poisson calibrated >= CALIBRATED_THRESHOLD, mismatch raw >= MIN_CONFIDENCE.
        from engine.eval.predict_upcoming import CALIBRATED_THRESHOLD
        from engine.eval.predict_mismatch import MIN_CONFIDENCE as MM_MIN_CONFIDENCE

        gated = []
        for p in eval_preds:
            raw = p["predicted_prob_raw"]
            points = _points_for(fit_maps, _calibration_key(p))
            global_cal = apply_points(points, raw) if points else raw
            cal = global_cal
            calibration_key = _calibration_key(p)
            league_adjustments = fit_league_adjustments_by_key.get(
                calibration_key, {})
            if calibration_key in LEAGUE_ADJUSTED_CALIBRATION_KEYS:
                adjustment = league_adjustments.get(
                    str(int(p["api_league_id"]))) if p.get("api_league_id") is not None else None
                cal = apply_logit_adjustment(global_cal, adjustment)
            if p["model"] == "bayesian":
                # Live order: calibrate, then streak boost, then the 70% gate
                # on the final displayed value.
                cal = apply_streak_boost(cal, int(p.get("streak", 0)))
                if cal < MIN_POSTERIOR_INTERESTING:
                    continue
            if p["model"] == "poisson" and cal < CALIBRATED_THRESHOLD:
                continue
            if p["model"] == "mismatch" and raw < MM_MIN_CONFIDENCE:
                continue
            if p["model"] == "dixon_coles" and cal < CALIBRATED_THRESHOLD:
                continue
            q = dict(p)
            q["predicted_prob"] = round(cal, 4)
            q["global_calibrated_probability"] = round(global_cal, 4)
            q["calibration_scope"] = (
                "global+league"
                if (calibration_key in LEAGUE_ADJUSTED_CALIBRATION_KEYS
                    and p.get("api_league_id") is not None
                    and str(int(p["api_league_id"])) in league_adjustments)
                else "global"
            )
            gated.append(q)
        print(f"\n  Eval predictions passing live gates: {len(gated)}")

        # De-duplicate: one prediction per (fixture, market)
        deduped = deduplicate_predictions(gated)
        print(f"  After dedup: {len(deduped)} unique (fixture, market) predictions")
        print(f"  Unique fixtures with predictions: "
              f"{len(set(p['fixture_id'] for p in deduped))}")

        print("\nComputing calibration (eval folds only)...")
        report = compute_calibration(deduped)

        # Isolate the directly calibrated bounds market so the league layer can
        # be judged against the global map on the same held-out predictions.
        bounds_eval = [p for p in eval_preds
                       if _calibration_key(p) == DC_BOUNDS_1_4_CALIBRATION_KEY]
        if bounds_eval:
            rows = []
            bounds_points = fit_maps.get(DC_BOUNDS_1_4_CALIBRATION_KEY) or []
            fit_league_adjustments = fit_league_adjustments_by_key.get(
                DC_BOUNDS_1_4_CALIBRATION_KEY, {})
            for p in bounds_eval:
                global_prob = (apply_points(bounds_points, p["predicted_prob_raw"])
                               if bounds_points else p["predicted_prob_raw"])
                league_key = (str(int(p["api_league_id"]))
                              if p.get("api_league_id") is not None else "")
                league_prob = apply_logit_adjustment(
                    global_prob, fit_league_adjustments.get(league_key))
                rows.append({**p, "global_prob": global_prob,
                             "league_prob": league_prob,
                             "actual_int": int(p["actual"])})
            bdf = pd.DataFrame(rows)

            def _summary(prob_col: str) -> dict:
                return {
                    "n": len(bdf),
                    "avg_predicted": round(float(bdf[prob_col].mean()), 4),
                    "actual_hit_rate": round(float(bdf["actual_int"].mean()), 4),
                    "brier": round(float(((bdf[prob_col] - bdf["actual_int"]) ** 2).mean()), 4),
                    "mean_gap": round(float(bdf[prob_col].mean() - bdf["actual_int"].mean()), 4),
                }

            by_league = {}
            for league_id, grp in bdf.groupby("api_league_id"):
                if len(grp) < 10:
                    continue
                by_league[str(int(league_id))] = {
                    "league": grp["league"].iloc[0],
                    "n": len(grp),
                    "global_predicted": round(float(grp["global_prob"].mean()), 4),
                    "league_predicted": round(float(grp["league_prob"].mean()), 4),
                    "actual_hit_rate": round(float(grp["actual_int"].mean()), 4),
                    "global_brier": round(float(((grp["global_prob"] - grp["actual_int"]) ** 2).mean()), 4),
                    "league_brier": round(float(((grp["league_prob"] - grp["actual_int"]) ** 2).mean()), 4),
                    "adjustment_fitted": str(int(league_id)) in fit_league_adjustments,
                }
            report["bounds_1_4_league_calibration"] = {
                "global_only": _summary("global_prob"),
                "league_aware": _summary("league_prob"),
                "fitted_league_count": len(fit_league_adjustments),
                "min_n": LEAGUE_CALIBRATION_MIN_N,
                "prior_strength": LEAGUE_CALIBRATION_PRIOR_STRENGTH,
                "by_league": by_league,
            }

        # Same picks scored on RAW probabilities, to show the maps' effect
        raw_scored = [dict(p, predicted_prob=p["predicted_prob_raw"])
                      for p in gated]
        raw_deduped = deduplicate_predictions(raw_scored)
        rdf = pd.DataFrame(raw_deduped)
        if not rdf.empty:
            rdf["actual_int"] = rdf["actual"].astype(int)
            report["eval_raw_summary"] = {
                "n": len(rdf),
                "brier_score": round(float(((rdf["predicted_prob"] -
                                             rdf["actual_int"]) ** 2).mean()), 4),
                "avg_predicted": round(float(rdf["predicted_prob"].mean()), 4),
                "overall_hit_rate": round(float(rdf["actual_int"].mean()), 4),
                "overconfidence": round(float(rdf["predicted_prob"].mean() -
                                              rdf["actual_int"].mean()), 4),
            }

        # ── Shipped maps: method validated above, refit on ALL folds so the
        # live maps use every raw prediction, including the most recent ──
        shipped_models = {}
        for calibration_key in CALIBRATION_KEYS:
            mp = _rows_for_key(all_predictions, calibration_key)
            shipped_models[calibration_key] = _fit_map(mp, calibration_key)
        shipped_league_adjustments = {}
        for calibration_key in LEAGUE_ADJUSTED_CALIBRATION_KEYS:
            rows = [p for p in all_predictions
                    if _calibration_key(p) == calibration_key]
            shipped_league_adjustments[calibration_key] = (
                fit_league_logit_adjustments(
                    rows,
                    shipped_models.get(calibration_key) or [],
                    min_n=LEAGUE_CALIBRATION_MIN_N,
                    prior_strength=LEAGUE_CALIBRATION_PRIOR_STRENGTH,
                )
            )
        shipped_maps = {
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "method": "walk-forward equal-count bins + PAVA, refit on all folds",
            "months": months,
            "fold_days": fold_days,
            "n_folds": n_folds,
            "validation": "fit on folds 1-%d, evaluated on folds %d-%d" % (
                n_fit, n_fit + 1, n_folds),
            "models": shipped_models,
            "league_adjustments": shipped_league_adjustments,
            "league_adjustment_method": (
                "empirical-Bayes league logit intercept over global PAVA; "
                f"min_n={LEAGUE_CALIBRATION_MIN_N}, prior={LEAGUE_CALIBRATION_PRIOR_STRENGTH}"
            ),
        }

        eval_fixtures = all_test_fixtures[all_test_fixtures["fold"] > n_fit]
        report["raw_prediction_count"] = len(all_predictions)
        report["base_rates"] = compute_base_rates(eval_fixtures)
        report["predictions"] = deduped
        report["method"] = "walk-forward (calibration fit/eval fold split)"
        report["fold_days"] = fold_days
        report["n_folds"] = n_folds
        report["n_fit_folds"] = n_fit
        report["fit_maps"] = fit_maps
        report["shipped_maps"] = shipped_maps

        return report

    finally:
        conn.close()


def print_report(report: dict) -> None:
    if "error" in report:
        print(f"\nError: {report['error']}")
        return

    print(f"\n{'=' * 70}")
    print(f"  WALK-FORWARD CALIBRATION REPORT")
    print(f"  Method: {report.get('method', 'unknown')} | "
          f"{report.get('n_folds', '?')} folds of "
          f"{report.get('fold_days', '?')} days | "
          f"maps fit on folds 1-{report.get('n_fit_folds', '?')}")
    print(f"  Metrics below are EVAL FOLDS ONLY (never seen by the maps)")
    print(f"{'=' * 70}")
    print(f"  De-duplicated predictions: {report['n']}")
    print(f"  Unique fixtures:           {report.get('unique_fixtures', '?')}")
    print(f"  Raw predictions (all folds, pre-gate): "
          f"{report.get('raw_prediction_count', '?')}")
    print(f"  Brier score:       {report['brier_score']:.4f}  "
          f"(lower = better, 0 = perfect)")
    print(f"  Avg predicted:     {report['avg_predicted']:.1%}")
    print(f"  Actual hit rate:   {report['overall_hit_rate']:.1%}")
    oc = report['overconfidence']
    label = ("OVERCONFIDENT" if oc > 0.02 else
             "UNDERCONFIDENT" if oc < -0.02 else "WELL CALIBRATED")
    print(f"  Overconfidence:    {oc:+.1%}  ({label})")

    raw_sum = report.get("eval_raw_summary")
    if raw_sum:
        print(f"\n  Same picks, RAW (uncalibrated) probabilities:")
        print(f"    brier={raw_sum['brier_score']:.4f}  "
              f"predicted={raw_sum['avg_predicted']:.1%}  "
              f"actual={raw_sum['overall_hit_rate']:.1%}  "
              f"gap={raw_sum['overconfidence']:+.1%}")

    bounds_league = report.get("bounds_1_4_league_calibration")
    if bounds_league:
        g = bounds_league["global_only"]
        l = bounds_league["league_aware"]
        print(f"\n  Goal Bounds 1-4 league-aware validation:")
        print(f"    global-only: brier={g['brier']:.4f}  "
              f"predicted={g['avg_predicted']:.1%}  actual={g['actual_hit_rate']:.1%}")
        print(f"    league-aware: brier={l['brier']:.4f}  "
              f"predicted={l['avg_predicted']:.1%}  actual={l['actual_hit_rate']:.1%}")
        print(f"    fitted league offsets: {bounds_league['fitted_league_count']}")

    br = report.get("base_rates", {})
    if br:
        print(f"\n{'─' * 70}")
        print(f"  BASE RATES (what you'd get picking blind)")
        print(f"{'─' * 70}")
        print(f"  Home team scores ≥1:  {br.get('home_scores', 0):.1%}")
        print(f"  Away team scores ≥1:  {br.get('away_scores', 0):.1%}")
        print(f"  Match Over 1.5:       {br.get('over_1.5', 0):.1%}")
        print(f"  Match Over 2.5:       {br.get('over_2.5', 0):.1%}")
        print(f"  Match Under 3.5:      {br.get('under_3.5', 0):.1%}")
        print(f"  Match Goals 1-4:      {br.get('bounds_1_4', 0):.1%}")

    print(f"\n{'─' * 70}")
    print(f"  MODEL PARTICIPATION (which models contributed)")
    print(f"{'─' * 70}")
    for model, stats in report.get("per_model", {}).items():
        print(f"  {model:<12s} in {stats['n']:>6d} predictions  "
              f"brier={stats['brier']:.4f}  "
              f"pred={stats['avg_predicted']:.1%}  "
              f"actual={stats['actual_hit_rate']:.1%}  "
              f"gap={stats['overconfidence']:+.1%}")

    print(f"\n{'─' * 70}")
    print(f"  CONSENSUS TIERS")
    print(f"{'─' * 70}")
    for tier, stats in report.get("consensus", {}).items():
        print(f"  {tier:<6s} N={stats['n']:>5d}  "
              f"predicted={stats['avg_predicted']:.1%}  "
              f"actual={stats['actual_hit_rate']:.1%}  "
              f"brier={stats['brier']:.4f}")

    print(f"\n{'─' * 70}")
    print(f"  PER-MARKET BREAKDOWN")
    print(f"{'─' * 70}")
    print(f"  {'Market':<14s} {'N':>6s} {'Predicted':>10s} "
          f"{'Actual':>8s} {'Gap':>8s} {'vs Base':>10s}")
    base = report.get("base_rates", {})
    base_map = {
        "over_match": base.get("over_1.5", 0),
        "under": base.get("under_3.5", 0),
        "bounds_1_4": base.get("bounds_1_4", 0),
        "team_scores": (base.get("home_scores", 0) + base.get("away_scores", 0)) / 2,
    }
    for mt, stats in report.get("per_market", {}).items():
        gap = stats['avg_predicted'] - stats['actual_hit_rate']
        base_rate = base_map.get(mt, 0)
        lift = stats['actual_hit_rate'] - base_rate if base_rate else 0
        print(f"  {mt:<14s} {stats['n']:>6d} {stats['avg_predicted']:>9.1%} "
              f"{stats['actual_hit_rate']:>7.1%} {gap:>+7.1%}"
              f"  {f'lift:{lift:+.1%}' if base_rate else ''}")

    print(f"\n{'─' * 70}")
    print(f"  CALIBRATION CURVE (predicted vs actual)")
    print(f"{'─' * 70}")
    print(f"  {'Bin':>10s} {'N':>6s} {'Predicted':>10s} "
          f"{'Actual':>8s} {'Gap':>8s} {'Visual'}")
    for b in report.get("calibration_bins", []):
        bar_len = int(b["actual_rate"] * 40)
        bar = "█" * bar_len + "░" * (40 - bar_len)
        print(f"  {b['bin_lo']:.0%}-{b['bin_hi']:.0%}  {b['n']:>5d} "
              f"{b['predicted_avg']:>9.1%} {b['actual_rate']:>7.1%} "
              f"{b['gap']:>+7.1%}  {bar}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Walk-forward calibration backtest")
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--fold-days", type=int, default=14)
    parser.add_argument("--cal-frac", type=float, default=CAL_FRAC_DEFAULT,
                        help="Fraction of folds used to fit calibration maps")
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--dump-raw", type=str, default=None,
                        help="Write every raw walk-forward prediction to this JSON path")
    parser.add_argument("--no-save-maps", action="store_true",
                        help="Skip writing calibration_maps.json for live use")
    args = parser.parse_args()

    report = run_walk_forward(months=args.months, fold_days=args.fold_days,
                              cal_frac=args.cal_frac, dump_raw=args.dump_raw)
    print_report(report)

    out_path = Path(args.output) if args.output else (
        Path(__file__).parent / "calibration_report.json")
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport saved to {out_path}")

    if not args.no_save_maps and report.get("shipped_maps"):
        with open(MAPS_PATH, "w") as f:
            json.dump(report["shipped_maps"], f, indent=2)
        print(f"Live calibration maps saved to {MAPS_PATH}")
