"""
Shared calibration-map utilities.

calibration_wf.py fits per-model monotonic calibration maps from raw
walk-forward predictions and writes them to calibration_maps.json.
Live predictors (bayesian_streaks, predict_upcoming, predict_mismatch)
load the maps here and apply them to raw model probabilities, falling
back to their legacy constants when the file is absent.

The fitting is equal-count binning followed by pool-adjacent-violators
(PAVA), which enforces a monotonic raw → calibrated relationship — a
stronger raw signal can never map to a lower calibrated probability.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np

MAPS_PATH = Path(__file__).parent / "calibration_maps.json"

# ── Dixon-Coles per-market calibration (2026-10-03) ─────────────────────
# One shared DC map pulled every market to the same top value (~83.9%):
# team-to-score and Over 1.5 were understated, Under 3.5 and the 2.5 lines
# overstated. Each market family now gets its own map; the pooled
# "dixon_coles" map remains the fallback when a family lacks data.
DC_POOLED_KEY = "dixon_coles"
DC_BOUNDS_1_4_KEY = "dixon_coles_bounds_1_4"
DC_MATCH_MARKET_KEYS = {
    "Match Over 1.5": "dixon_coles_over_1_5",
    "Match Over 2.5": "dixon_coles_over_2_5",
    "Match Over 3.5": "dixon_coles_over_3_5",
    "Match Under 2.5": "dixon_coles_under_2_5",
    "Match Under 3.5": "dixon_coles_under_3_5",
    "BTTS Yes": "dixon_coles_btts",
    "Match Goals 1-4": DC_BOUNDS_1_4_KEY,
}
DC_TEAM_SCORE_KEY = "dixon_coles_team_score"
DC_TEAM_OVER_1_5_KEY = "dixon_coles_team_over_1_5"
DC_TEAM_WIN_KEY = "dixon_coles_team_win"
DC_MARKET_KEYS = (
    *dict.fromkeys(DC_MATCH_MARKET_KEYS.values()),
    DC_TEAM_SCORE_KEY, DC_TEAM_OVER_1_5_KEY, DC_TEAM_WIN_KEY,
)


def dc_calibration_key(market: str) -> str:
    """Calibration-map key for one Dixon-Coles market name."""
    if market in DC_MATCH_MARKET_KEYS:
        return DC_MATCH_MARKET_KEYS[market]
    if market.endswith(" Over 0.5"):
        return DC_TEAM_SCORE_KEY
    if market.endswith(" Over 1.5"):
        return DC_TEAM_OVER_1_5_KEY
    if market.endswith(" Win"):
        return DC_TEAM_WIN_KEY
    return DC_POOLED_KEY


def load_points(model: str, path: Path = MAPS_PATH) -> list[list[float]] | None:
    """Return [[raw, calibrated], ...] for a model, or None if unavailable."""
    try:
        with open(path) as f:
            data = json.load(f)
        points = data.get("models", {}).get(model)
        return points if points else None
    except (OSError, ValueError):
        return None


def load_league_adjustments(model: str, path: Path = MAPS_PATH) -> dict[str, dict]:
    """Return league-level logit adjustments for one calibration key.

    Keys are stringified API league IDs so the JSON is portable.  Missing or
    older map files intentionally behave as a global-only calibration.
    """
    try:
        with open(path) as f:
            data = json.load(f)
        adjustments = data.get("league_adjustments", {}).get(model, {})
        return adjustments if isinstance(adjustments, dict) else {}
    except (OSError, ValueError):
        return {}


def apply_points(points: list[list[float]], raw: float) -> float:
    """Piecewise-linear interpolation through map points, clamped at the ends."""
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return float(np.interp(raw, xs, ys))


def apply_logit_adjustment(probability: float, adjustment: dict | None) -> float:
    """Apply a shrunk league intercept while preserving probability ranking."""
    if not adjustment:
        return float(probability)
    delta = float(adjustment.get("logit_delta", 0.0))
    p = min(max(float(probability), 1e-6), 1 - 1e-6)
    logit = math.log(p / (1 - p)) + delta
    return float(1 / (1 + math.exp(-logit)))


def fit_league_logit_adjustments(predictions: list[dict],
                                 global_points: list[list[float]],
                                 min_n: int = 30,
                                 prior_strength: int = 200) -> dict[str, dict]:
    """Fit empirical-Bayes league offsets on top of a global calibration map.

    A league's observed hit rate is shrunk toward the rate expected by the
    global map.  The resulting constant logit offset lets leagues differ in
    level without fitting fragile, non-monotonic curves to small samples.
    """
    grouped: dict[str, list[dict]] = {}
    for p in predictions:
        league_id = p.get("api_league_id")
        if league_id is None:
            continue
        grouped.setdefault(str(int(league_id)), []).append(p)

    adjustments = {}
    for league_id, rows in grouped.items():
        n = len(rows)
        if n < min_n:
            continue
        expected = np.asarray([
            apply_points(global_points, r["predicted_prob_raw"])
            if global_points else float(r["predicted_prob_raw"])
            for r in rows
        ], dtype=float)
        actual = np.asarray([float(r["actual"]) for r in rows], dtype=float)
        expected_rate = float(expected.mean())
        actual_rate = float(actual.mean())
        shrunk_rate = float(
            (actual.sum() + prior_strength * expected_rate) /
            (n + prior_strength)
        )
        e = min(max(expected_rate, 1e-6), 1 - 1e-6)
        s = min(max(shrunk_rate, 1e-6), 1 - 1e-6)
        delta = math.log(s / (1 - s)) - math.log(e / (1 - e))
        adjustments[league_id] = {
            "n": n,
            "expected_rate": round(expected_rate, 4),
            "actual_rate": round(actual_rate, 4),
            "shrunk_rate": round(shrunk_rate, 4),
            "logit_delta": round(float(delta), 6),
            "prior_strength": prior_strength,
        }
    return adjustments


def fit_pava_map(raw, actual, n_bins: int = 10,
                 min_bin_n: int = 50) -> list[list[float]]:
    """Fit a monotonic calibration map from raw probabilities and outcomes.

    Equal-count bins over sorted raw probabilities, then PAVA merges any
    bin whose actual hit rate dips below its left neighbour (weighted by
    bin size). Returns [[raw_bin_mean, calibrated_rate], ...], or [] when
    there is too little data to fit at least two bins.
    """
    raw = np.asarray(raw, dtype=float)
    actual = np.asarray(actual, dtype=float)
    n = len(raw)
    if n < 2 * min_bin_n:
        return []
    order = np.argsort(raw)
    raw_s, act_s = raw[order], actual[order]
    n_bins = max(2, min(n_bins, n // min_bin_n))
    edges = np.linspace(0, n, n_bins + 1).astype(int)

    xs, ys, ws = [], [], []
    for i in range(n_bins):
        seg_r = raw_s[edges[i]:edges[i + 1]]
        seg_a = act_s[edges[i]:edges[i + 1]]
        if len(seg_r) == 0:
            continue
        xs.append(float(seg_r.mean()))
        ys.append(float(seg_a.mean()))
        ws.append(len(seg_r))

    # Pool adjacent violators: merge until actual rates are non-decreasing
    merged: list[list] = []  # [rate, weight, [bin indices]]
    for i, (y, w) in enumerate(zip(ys, ws)):
        merged.append([y, w, [i]])
        while len(merged) >= 2 and merged[-2][0] > merged[-1][0]:
            y2, w2, i2 = merged.pop()
            y1, w1, i1 = merged.pop()
            w_tot = w1 + w2
            merged.append([(y1 * w1 + y2 * w2) / w_tot, w_tot, i1 + i2])

    cal = [0.0] * len(ys)
    for y, _, idxs in merged:
        for i in idxs:
            cal[i] = y
    return [[round(x, 4), round(c, 4)] for x, c in zip(xs, cal)]
