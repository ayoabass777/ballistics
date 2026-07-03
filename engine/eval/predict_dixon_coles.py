"""
Dixon-Coles bivariate Poisson model with exponential time decay.

Fits per-league team attack/defence rates, home advantage, and the
Dixon-Coles low-score correlation correction (rho), weighting matches
by recency (Dixon & Coles, 1997). One fitted league model produces a
full scoreline distribution per fixture, from which every engine
market is priced: match over/unders, team totals, BTTS, and 1X2.

This replaces the total-goals Poisson GLM as the goals model candidate:
it models WHO scores, not just how many, and corrects the 0-0/1-1
dependence that plagues under markets.

Usage:
    python -m engine.eval.predict_dixon_coles           # picks next 7 days
    python -m engine.eval.predict_dixon_coles --days 10
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import poisson as poisson_dist

from engine.eval.calibration_maps import apply_points, load_points
from etl.src.logger import get_logger

logger = get_logger(__name__)

XI = 0.0065                # per-day decay → half-life ≈ 107 days
MAX_GOALS = 10             # scoreline grid truncation
MIN_LEAGUE_MATCHES = 50    # skip leagues with thinner history
RHO_BOUND = 0.15
CALIBRATED_THRESHOLD = 0.65

_DC_POINTS = load_points("dixon_coles")


def _calibrate(raw: float) -> float:
    """Fitted walk-forward map when available, identity otherwise."""
    if _DC_POINTS:
        return apply_points(_DC_POINTS, raw)
    return raw


def load_dc_historical(conn) -> pd.DataFrame:
    """Load historical matches for Dixon-Coles without Elo-dependent joins."""
    sql = """
        SELECT
            f.fixture_id,
            f.kickoff_utc,
            f.season,
            r.api_league_id,
            f.league_name AS league,
            f.team_id AS home_team_id,
            f.opponent_id AS away_team_id,
            f.team_name AS home_team,
            f.opponent_name AS away_team,
            f.goals_for AS home_goals,
            f.goals_against AS away_goals
        FROM raw.fact_head_to_head f
        JOIN raw.raw_fixtures r
          ON r.fixture_id = f.fixture_id
        WHERE f.is_played
          AND f.is_home
        ORDER BY f.kickoff_utc
    """
    return pd.read_sql(sql, conn)


# ── fitting ─────────────────────────────────────────────────────────────────

def fit_league_dc(matches: pd.DataFrame, ref_time, xi: float = XI) -> dict | None:
    """Fit one league's Dixon-Coles model on matches before ref_time.

    matches: home_team_id, away_team_id, home_goals, away_goals, kickoff_utc.
    Returns {"mu", "home_adv", "rho", "attack": {id: a}, "defence": {id: d}}
    or None when the league has too little history.
    """
    if len(matches) < MIN_LEAGUE_MATCHES:
        return None

    teams = sorted(set(matches["home_team_id"]) | set(matches["away_team_id"]))
    n = len(teams)
    if n < 4:
        return None
    idx = {t: i for i, t in enumerate(teams)}

    h = matches["home_team_id"].map(idx).values
    a = matches["away_team_id"].map(idx).values
    x = matches["home_goals"].values.astype(float)
    y = matches["away_goals"].values.astype(float)

    kickoffs = pd.to_datetime(matches["kickoff_utc"], utc=True)
    days_ago = np.clip((ref_time - kickoffs).dt.days.values, 0, None)
    w = np.exp(-xi * days_ago)

    m00 = (x == 0) & (y == 0)
    m01 = (x == 0) & (y == 1)
    m10 = (x == 1) & (y == 0)
    m11 = (x == 1) & (y == 1)

    def nll(params: np.ndarray) -> float:
        mu, ha, rho = params[0], params[1], params[2]
        att = params[3:3 + n]
        deff = params[3 + n:]
        # centre attack/defence: removes the additive gauge freedom
        att = att - att.mean()
        deff = deff - deff.mean()

        lam = np.exp(mu + ha + att[h] + deff[a])   # home scoring rate
        muv = np.exp(mu + att[a] + deff[h])        # away scoring rate

        tau = np.ones(len(x))
        tau[m00] = 1.0 - lam[m00] * muv[m00] * rho
        tau[m01] = 1.0 + lam[m01] * rho
        tau[m10] = 1.0 + muv[m10] * rho
        tau[m11] = 1.0 - rho

        ll = w * (np.log(np.clip(tau, 1e-10, None))
                  + x * np.log(lam) - lam
                  + y * np.log(muv) - muv)
        return -float(ll.sum())

    x0 = np.concatenate([[0.1, 0.25, 0.0], np.zeros(2 * n)])
    bounds = ([(-2.0, 2.0), (-1.0, 1.0), (-RHO_BOUND, RHO_BOUND)]
              + [(-3.0, 3.0)] * (2 * n))
    res = minimize(nll, x0, method="L-BFGS-B", bounds=bounds,
                   options={"maxiter": 300})
    if not np.isfinite(res.fun):
        logger.warning("DC fit diverged (%d matches)", len(matches))
        return None

    p = res.x
    att = p[3:3 + n] - p[3:3 + n].mean()
    deff = p[3 + n:] - p[3 + n:].mean()
    return {
        "mu": float(p[0]),
        "home_adv": float(p[1]),
        "rho": float(p[2]),
        "attack": {t: float(att[idx[t]]) for t in teams},
        "defence": {t: float(deff[idx[t]]) for t in teams},
        "n_matches": len(matches),
    }


# ── prediction ──────────────────────────────────────────────────────────────

def score_matrix(params: dict, home_id, away_id) -> tuple[np.ndarray, float, float] | None:
    """P[i, j] = P(home scores i, away scores j). None if a team is unknown."""
    att, deff = params["attack"], params["defence"]
    if home_id not in att or away_id not in att:
        return None
    lam = np.exp(params["mu"] + params["home_adv"] + att[home_id] + deff[away_id])
    muv = np.exp(params["mu"] + att[away_id] + deff[home_id])

    goals = np.arange(MAX_GOALS + 1)
    P = np.outer(poisson_dist.pmf(goals, lam), poisson_dist.pmf(goals, muv))

    rho = params["rho"]
    P[0, 0] *= max(1.0 - lam * muv * rho, 1e-10)
    P[0, 1] *= 1.0 + lam * rho
    P[1, 0] *= 1.0 + muv * rho
    P[1, 1] *= 1.0 - rho
    P /= P.sum()
    return P, float(lam), float(muv)


def fixture_markets(params: dict, home_id, away_id,
                    home_name: str, away_name: str) -> dict | None:
    """Price all engine markets from the scoreline distribution."""
    out = score_matrix(params, home_id, away_id)
    if out is None:
        return None
    P, lam, muv = out

    ii, jj = np.indices(P.shape)
    total = ii + jj
    markets = {
        "Match Over 1.5": float(P[total > 1.5].sum()),
        "Match Over 2.5": float(P[total > 2.5].sum()),
        "Match Under 2.5": float(P[total < 2.5].sum()),
        "Match Over 3.5": float(P[total > 3.5].sum()),
        "Match Under 3.5": float(P[total < 3.5].sum()),
        f"{home_name} Over 0.5": float(1.0 - P[0, :].sum()),
        f"{home_name} Over 1.5": float(P[2:, :].sum()),
        f"{away_name} Over 0.5": float(1.0 - P[:, 0].sum()),
        f"{away_name} Over 1.5": float(P[:, 2:].sum()),
        "BTTS Yes": float(P[1:, 1:].sum()),
        f"{home_name} Win": float(P[ii > jj].sum()),
        f"{away_name} Win": float(P[jj > ii].sum()),
    }
    return {"markets": markets, "lambda_home": lam, "lambda_away": muv}


# ── live prediction over upcoming fixtures ──────────────────────────────────

def predict_dixon_coles(days: int = 7,
                        threshold: float = CALIBRATED_THRESHOLD) -> pd.DataFrame:
    from engine.eval.predict_upcoming import (
        _get_conn, load_upcoming,
        _load_league_map, _build_team_id_map,
    )

    conn = _get_conn()
    try:
        print("Loading historical matches...")
        hist = load_dc_historical(conn)
        print(f"  {len(hist)} matches")

        print("Loading upcoming fixtures...")
        upcoming = load_upcoming(conn, days=days)
        if upcoming.empty:
            print("  No upcoming fixtures.")
            return pd.DataFrame()
        league_map = _load_league_map(conn)
        upcoming["league"] = upcoming["api_league_id"].map(league_map)
        upcoming = upcoming[upcoming["league"].notna()].copy()
        id_map = _build_team_id_map(conn)
    finally:
        conn.close()

    ref_time = pd.Timestamp(datetime.now(timezone.utc))
    hist["kickoff_utc"] = pd.to_datetime(hist["kickoff_utc"], utc=True)

    results = []
    for api_league_id, up_lg in upcoming.groupby("api_league_id"):
        lg_hist = hist[hist["api_league_id"] == api_league_id]
        params = fit_league_dc(lg_hist, ref_time)
        if params is None:
            continue
        for _, row in up_lg.iterrows():
            home_h2h = id_map.get(row["home_team_id"])
            away_h2h = id_map.get(row["away_team_id"])
            if home_h2h is None or away_h2h is None:
                continue
            fm = fixture_markets(params, home_h2h, away_h2h,
                                 row["home_team"], row["away_team"])
            if fm is None:
                continue
            for market, raw in fm["markets"].items():
                cal = _calibrate(raw)
                if cal < threshold:
                    continue
                results.append({
                    "fixture_id": int(row["fixture_id"]),
                    "kickoff": row["kickoff_utc"],
                    "home": row["home_team"],
                    "away": row["away_team"],
                    "league": row["league"],
                    "market": market,
                    "probability": round(cal, 4),
                    "raw_probability": round(raw, 4),
                    "lambda_home": round(fm["lambda_home"], 2),
                    "lambda_away": round(fm["lambda_away"], 2),
                })

    picks = pd.DataFrame(results)
    if not picks.empty:
        picks = picks.sort_values("probability", ascending=False)
    return picks


def print_picks(picks: pd.DataFrame) -> None:
    if picks.empty:
        print("No picks above threshold.")
        return
    print(f"\n{'=' * 110}")
    print(f"  DIXON-COLES PICKS  (calibrated >= {CALIBRATED_THRESHOLD:.0%}, "
          f"map {'loaded' if _DC_POINTS else 'ABSENT — raw probabilities'})")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'=' * 110}")
    print(f"  {'Conf':>5s}  {'λh':>4s}  {'λa':>4s}  {'Kickoff':>16s}  "
          f"{'Match':<40s}  {'Market':<24s}  League")
    for _, r in picks.iterrows():
        dt = pd.Timestamp(r["kickoff"]).strftime("%a %b %d %H:%M")
        match = f"{r['home']} vs {r['away']}"
        print(f"  {r['probability']:>4.0%}  {r['lambda_home']:>4.1f}  "
              f"{r['lambda_away']:>4.1f}  {dt:>16s}  {match:<40s}  "
              f"{r['market']:<24s}  {r['league']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dixon-Coles market picks")
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    picks = predict_dixon_coles(days=args.days)
    print_picks(picks)
