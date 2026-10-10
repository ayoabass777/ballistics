"""League sensitivity: league-level calibration error vs error expected from chance alone.

Diagnostic for docs/product/numbers-audit-2026-10-03.md (§4 league test).
Input: raw walk-forward dump from
    python -m engine.eval.calibration_wf --no-save-maps --dump-raw data/raw_preds.json
Run:
    python -m engine.eval.diagnostics.league_sensitivity [data/raw_preds.json]
"""
import sys
RAW = sys.argv[1] if len(sys.argv) > 1 else "data/raw_preds.json"
import json, random
from collections import defaultdict
from engine.eval.calibration_maps import apply_points, fit_pava_map
from engine.eval.compare_dc_calibration import family, FAMILIES
rows=[dict(p,actual=float(p['actual'])) for p in json.load(open(RAW)) if p.get('model')=='dixon_coles' and p.get('actual') is not None]
random.seed(7)
for n_fit in (4,6):
    fit=[r for r in rows if r['fold']<=n_fit]; ev=[r for r in rows if r['fold']>n_fit]
    print(f"fit folds 1-{n_fit}")
    for fam in FAMILIES:
        fr=[r for r in fit if family(r['market'])==fam]; er=[r for r in ev if family(r['market'])==fam]
        pts=fit_pava_map([r['predicted_prob_raw'] for r in fr],[r['actual'] for r in fr])
        for r in er: r['pm']=apply_points(pts,r['predicted_prob_raw'])
        g=defaultdict(list)
        for r in er: g[r.get('api_league_id')].append(r)
        big=[v for v in g.values() if len(v)>=40]; n=sum(len(v) for v in big)
        obs=sum(len(v)/n*abs(sum(x['pm'] for x in v)/len(v)-sum(x['actual'] for x in v)/len(v)) for v in big)
        noise=sum(sum(len(v)/n*abs(sum(x['pm'] for x in v)/len(v)-sum(1.0 if random.random()<x['pm'] else 0.0 for x in v)/len(v)) for v in big) for _ in range(300))/300
        print(f"  {fam:11s} leagues={len(big):3d} league error {100*obs:4.1f}pt  chance {100*noise:4.1f}pt  excess {100*(obs-noise):+4.1f}")
