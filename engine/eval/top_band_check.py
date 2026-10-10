"""Top-band calibration check (diagnostic, 2026-10-03).

Question: within raw Dixon-Coles probabilities above the calibration map's
last point (~0.91), do higher raw values actually hit more often? If yes, the
clamp in calibration_maps.apply_points hides real information and the top of
the map needs finer bins. If no, the ceiling is honest and only ordering
within the band is lost.

Usage:
    python -m engine.eval.calibration_wf --no-save-maps --output /tmp/cal_report.json --dump-raw /tmp/raw_preds.json
    python -m engine.eval.top_band_check /tmp/raw_preds.json
"""
import json
import sys
from collections import defaultdict

BANDS = [(0.80, 0.85), (0.85, 0.88), (0.88, 0.91), (0.91, 0.94),
         (0.94, 0.97), (0.97, 1.01)]


def market_type(m: str) -> str:
    if m.startswith("Match Goals"):
        return "Goals 1-4"
    if m.startswith("Match"):
        return " ".join(m.split()[:3])
    return "Team " + " ".join(m.split()[-2:])


def main(path: str, model: str = "dixon_coles") -> None:
    preds = [p for p in json.load(open(path)) if p.get("model") == model]
    print(f"{model}: {len(preds)} raw predictions with outcomes\n")
    for label, rows in [("ALL MARKETS", preds)] + sorted(
            ((t, [p for p in preds if market_type(p["market"]) == t])
             for t in {market_type(p["market"]) for p in preds}),
            key=lambda x: -len(x[1])):
        cells = []
        for lo, hi in BANDS:
            sub = [p for p in rows if lo <= p["predicted_prob_raw"] < hi]
            if sub:
                hit = sum(1 for p in sub if p["actual"]) / len(sub)
                cells.append(f"{lo:.2f}-{min(hi,1):.2f}: {hit:5.1%} (n={len(sub)})")
        if cells:
            print(f"{label}\n  " + "\n  ".join(cells) + "\n")


if __name__ == "__main__":
    main(sys.argv[1], *(sys.argv[2:3]))
