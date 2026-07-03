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
from pathlib import Path

import numpy as np

MAPS_PATH = Path(__file__).parent / "calibration_maps.json"


def load_points(model: str, path: Path = MAPS_PATH) -> list[list[float]] | None:
    """Return [[raw, calibrated], ...] for a model, or None if unavailable."""
    try:
        with open(path) as f:
            data = json.load(f)
        points = data.get("models", {}).get(model)
        return points if points else None
    except (OSError, ValueError):
        return None


def apply_points(points: list[list[float]], raw: float) -> float:
    """Piecewise-linear interpolation through map points, clamped at the ends."""
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return float(np.interp(raw, xs, ys))


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
