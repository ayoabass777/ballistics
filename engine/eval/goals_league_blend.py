"""League blend for Goals outlook probabilities (proposal 2026-10-17, owner approved).

Backtest finding: when a calibrated Dixon-Coles chance sits far above the league
rate for the same event, the event comes true less often than claimed. The
league level carries information the forecast under-uses. For the markets where
this holds, the shown chance is a logit-space blend of the calibrated chance and
the league rate:

    blended = sigmoid(a + b * logit(calibrated) + c * logit(league_rate))

Weights live in goals_league_blend.json, keyed by calibration-map key, and are
refitted from a raw-prediction dump (`calibration_wf --dump-raw`):

    python -m engine.eval.goals_league_blend data/raw_preds.json

Fitting is walk-forward: each fixture's chance comes from calibration maps fitted
on earlier folds and its league rate from earlier fixtures only, so the weights
learn what the production pipeline actually sees. Over 1.5 and Goals 1-4 are
left out on purpose: they were already honest and the blend over-corrected them.
See docs/product/experiments/gap-aware-goals-2026-10-17.
"""
from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from engine.eval.calibration_maps import (
    DC_TEAM_OVER_1_5_KEY,
    DC_TEAM_SCORE_KEY,
    apply_points,
    dc_calibration_key,
    fit_pava_map,
)

BLEND_PATH = Path(__file__).parent / "goals_league_blend.json"
BLENDED_KEYS = (
    DC_TEAM_OVER_1_5_KEY,          # team scores 2+
    DC_TEAM_SCORE_KEY,             # team scores 1+
    "dixon_coles_under_3_5",
    "dixon_coles_over_2_5",
)
FIT_MIN_PROB = 0.60       # weights are fitted on, and applied to, chances >= this
FIT_MIN_LEAGUE = 20       # league fixtures behind the rate (LEAGUE_RATE_MIN_MATCHES)
FIT_MIN_ROWS = 300        # fewer rows for a key -> no weights, no blend
L2 = 1.0                  # pull toward b=1, c=0 (no blend) when data is thin

_CACHE: dict | None = None


def _logit(p: float) -> float:
    p = min(max(p, 1e-4), 1 - 1e-4)
    return math.log(p / (1 - p))


def load_blend(path: Path = BLEND_PATH) -> dict:
    """{calibration_key: {"a","b","c",...}}; empty when the file is missing."""
    global _CACHE
    if _CACHE is not None and path == BLEND_PATH:
        return _CACHE
    try:
        weights = json.loads(path.read_text()).get("weights", {})
    except (OSError, ValueError):
        weights = {}
    if path == BLEND_PATH:
        _CACHE = weights
    return weights


def blend_probability(market: str, probability: float, league_rate: float | None,
                      weights: dict | None = None) -> float | None:
    """Blended chance, or None when the blend does not apply to this row."""
    if league_rate is None or probability is None or probability < FIT_MIN_PROB:
        return None
    key = dc_calibration_key(market)
    if key not in BLENDED_KEYS:
        return None
    w = (weights if weights is not None else load_blend()).get(key)
    if not w:
        return None
    z = w["a"] + w["b"] * _logit(float(probability)) + w["c"] * _logit(float(league_rate))
    return 1 / (1 + math.exp(-z))


# ── Fitting ─────────────────────────────────────────────────────────────

_MATCH_EVENTS = {"Match Over 2.5": lambda t: t > 2, "Match Under 3.5": lambda t: t < 4}


def walk_forward_rows(preds: list[dict]) -> list[dict]:
    """Rows of (key, calibrated chance, prior league rate, outcome), no look-ahead."""
    fixtures = {}
    for p in preds:
        if p.get("score") and p["fixture_id"] not in fixtures:
            h, a = map(int, p["score"].split("-"))
            fixtures[p["fixture_id"]] = dict(ko=p["kickoff"], lg=(p["api_league_id"], p["league"]),
                                             home=p["home"], away=p["away"], hg=h, ag=a)
    # league tallies as they stood before each fixture
    match_t = defaultdict(lambda: [0, 0])
    team_t = defaultdict(lambda: [0, 0])
    prior = {}
    for fid, f in sorted(fixtures.items(), key=lambda kv: kv[1]["ko"]):
        lg, total = f["lg"], f["hg"] + f["ag"]
        prior[fid] = {
            **{("match", m): tuple(match_t[(lg, m)]) for m in _MATCH_EVENTS},
            **{(ev, side): tuple(team_t[(lg, ev, side)])
               for ev in ("score_1", "score_2") for side in ("home", "away")},
        }
        for m, fn in _MATCH_EVENTS.items():
            c = match_t[(lg, m)]; c[0] += fn(total); c[1] += 1
        for side, gf in (("home", f["hg"]), ("away", f["ag"])):
            for ev, need in (("score_1", 1), ("score_2", 2)):
                c = team_t[(lg, ev, side)]; c[0] += gf >= need; c[1] += 1

    dc = [p for p in preds if p.get("model") == "dixon_coles" and p.get("actual") is not None
          and p["fixture_id"] in prior and dc_calibration_key(p["market"]) in BLENDED_KEYS]
    folds = sorted({p["fold"] for p in dc})
    rows = []
    for fold in folds[2:]:
        fit = defaultdict(lambda: ([], []))
        for p in dc:
            if p["fold"] < fold:
                k = dc_calibration_key(p["market"])
                fit[k][0].append(p["predicted_prob_raw"]); fit[k][1].append(float(p["actual"]))
        maps = {k: fit_pava_map(*v) for k, v in fit.items()}
        for p in dc:
            if p["fold"] != fold:
                continue
            key = dc_calibration_key(p["market"])
            if not maps.get(key):
                continue
            prob = apply_points(maps[key], p["predicted_prob_raw"])
            if prob < FIT_MIN_PROB:
                continue
            f, s = fixtures[p["fixture_id"]], prior[p["fixture_id"]]
            if p["market"] in _MATCH_EVENTS:
                tally = s[("match", p["market"])]
            else:
                team = p["market"][:-len(" Over 0.5")]
                side = "home" if team == f["home"] else "away" if team == f["away"] else None
                if side is None:
                    continue
                ev = "score_1" if key == DC_TEAM_SCORE_KEY else "score_2"
                tally = s[(ev, side)]
            if tally[1] < FIT_MIN_LEAGUE:
                continue
            rows.append(dict(key=key, prob=prob, league=tally[0] / tally[1],
                             y=float(p["actual"]), fold=fold))
    return rows


def fit_weights(rows: list[dict], l2: float = L2, iters: int = 60) -> dict | None:
    """Penalised logistic fit of y ~ logit(prob) + logit(league), prior b=1, c=0."""
    import numpy as np

    if len(rows) < FIT_MIN_ROWS:
        return None
    X = np.array([[1.0, _logit(r["prob"]), _logit(r["league"])] for r in rows])
    y = np.array([r["y"] for r in rows])
    w0 = np.array([0.0, 1.0, 0.0])
    pen = np.diag([0.0, l2, l2])
    w = w0.copy()
    for _ in range(iters):
        p = 1 / (1 + np.exp(-(X @ w)))
        grad = X.T @ (y - p) - pen @ (w - w0)
        hess = X.T @ (X * (p * (1 - p))[:, None]) + pen + 1e-9 * np.eye(3)
        w = w + np.linalg.solve(hess, grad)
    return {"a": round(float(w[0]), 4), "b": round(float(w[1]), 4), "c": round(float(w[2]), 4),
            "rows": len(rows)}


def main(path: str, out: Path = BLEND_PATH) -> None:
    preds = json.load(open(path))
    rows = walk_forward_rows(preds)
    weights = {}
    for key in BLENDED_KEYS:
        w = fit_weights([r for r in rows if r["key"] == key])
        if w:
            weights[key] = w
        print(f"  {key:28s} {w if w else 'too few rows: not blended'}")
    kickoffs = sorted(p["kickoff"] for p in preds if p.get("kickoff"))
    payload = {
        "fitted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": Path(path).name,
        "data_range": [kickoffs[0][:10], kickoffs[-1][:10]] if kickoffs else None,
        "min_probability": FIT_MIN_PROB,
        "formula": "sigmoid(a + b*logit(calibrated) + c*logit(league_rate))",
        "weights": weights,
    }
    out.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data/raw_preds.json")
