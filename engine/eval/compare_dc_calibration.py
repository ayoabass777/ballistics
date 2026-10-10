"""Compare one shared Dixon-Coles calibration map against per-market maps.

Diagnostic (2026-10-03). Uses a raw-prediction dump from
`calibration_wf --dump-raw`. Maps are fitted on the first 40% of folds and
scored only on later folds, mirroring calibration_wf (no leakage).

    python -m engine.eval.compare_dc_calibration data/raw_preds.json
"""
import json
import math
import sys
from collections import defaultdict

from engine.eval.calibration_maps import apply_points, fit_pava_map

BOUNDS = "Match Goals 1-4"
SHOWN_FROM = 0.65  # predict_upcoming.CALIBRATED_THRESHOLD


def family(market: str) -> str:
    if market == BOUNDS:
        return "goals_1_4"
    if market.endswith(" Over 0.5") and not market.startswith("Match"):
        return "team_score"
    if market == "Match Over 1.5":
        return "over_1_5"
    if market == "Match Under 3.5":
        return "under_3_5"
    return "other"


FAMILIES = ["team_score", "over_1_5", "under_3_5", "other"]


def fit(rows, **kw):
    return fit_pava_map([r["predicted_prob_raw"] for r in rows],
                        [float(r["actual"]) for r in rows], **kw)


def score(rows, key):
    n = len(rows)
    if not n:
        return None
    brier = sum((r[key] - r["actual"]) ** 2 for r in rows) / n
    ll = -sum(math.log(max(1e-6, r[key] if r["actual"] else 1 - r[key]))
              for r in rows) / n
    # expected calibration error over 5-point bins
    bins = defaultdict(list)
    for r in rows:
        bins[min(int(r[key] * 20), 19)].append(r)
    ece = sum(len(b) / n * abs(sum(x[key] for x in b) / len(b)
                               - sum(x["actual"] for x in b) / len(b))
              for b in bins.values())
    return brier, ll, ece


def main(path: str, cal_frac: float = 0.4) -> None:
    rows = [p for p in json.load(open(path))
            if p.get("model") == "dixon_coles" and p.get("actual") is not None]
    rows = [dict(r, actual=float(r["actual"])) for r in rows]
    n_folds = max(r["fold"] for r in rows)
    n_fit = min(max(1, int(round(n_folds * cal_frac))), n_folds - 1)
    fit_rows = [r for r in rows if r["fold"] <= n_fit and family(r["market"]) != "goals_1_4"]
    ev = [r for r in rows if r["fold"] > n_fit and family(r["market"]) != "goals_1_4"]
    print(f"DC predictions: fit folds 1-{n_fit} = {len(fit_rows)}, "
          f"eval folds {n_fit + 1}-{n_folds} = {len(ev)} (Goals 1-4 excluded: own map in both)\n")

    shared = fit(fit_rows)
    per = {}
    for fam in FAMILIES:
        fr = [r for r in fit_rows if family(r["market"]) == fam]
        per[fam] = fit(fr) or shared
        print(f"  {fam:11s} fit n={len(fr):5d}  map top point {per[fam][-1]}")
    print(f"  {'shared':11s} fit n={len(fit_rows):5d}  map top point {shared[-1]}\n")

    for r in ev:
        r["shared"] = apply_points(shared, r["predicted_prob_raw"])
        r["per_market"] = apply_points(per[family(r["market"])], r["predicted_prob_raw"])

    def report(label, subset):
        print(label)
        print(f"  {'':11s} {'n':>6s}   {'Brier':>14s}   {'log loss':>14s}   {'calib error':>14s}")
        for fam in FAMILIES + ["ALL"]:
            s = subset if fam == "ALL" else [r for r in subset if family(r["market"]) == fam]
            a, b = score(s, "shared"), score(s, "per_market")
            if not a:
                continue
            print(f"  {fam:11s} {len(s):6d}   {a[0]:.4f}→{b[0]:.4f}   {a[1]:.4f}→{b[1]:.4f}   "
                  f"{100*a[2]:4.1f}→{100*b[2]:4.1f} pts")
        print()

    report("ALL eval predictions (shared → per-market; lower is better)", ev)
    for key in ("shared", "per_market"):
        shown = [r for r in ev if r[key] >= SHOWN_FROM]
        print(f"{key:10s}: {len(shown)} predictions would be shown (≥ {SHOWN_FROM:.0%})")
    print()

    print("Top of the range: predictions shown at ≥ 80% (shown avg vs actual)")
    for key in ("shared", "per_market"):
        print(f"  {key}")
        for fam in FAMILIES:
            s = [r for r in ev if family(r["market"]) == fam and r[key] >= 0.80]
            if s:
                shown = sum(r[key] for r in s) / len(s)
                act = sum(r["actual"] for r in s) / len(s)
                distinct = len({round(r[key], 4) for r in s})
                print(f"    {fam:11s} n={len(s):5d}  shown {shown:5.1%}  actual {act:5.1%}  "
                      f"gap {100*(shown-act):+5.1f}  distinct values {distinct}")


if __name__ == "__main__":
    main(sys.argv[1])
