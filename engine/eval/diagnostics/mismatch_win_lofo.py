"""Mismatch win: leave-one-fold-out test of a win-specific calibration map.

Diagnostic for docs/product/numbers-audit-2026-10-03.md (§5).
Input: raw walk-forward dump from
    python -m engine.eval.calibration_wf --no-save-maps --dump-raw data/raw_preds.json
Run:
    python -m engine.eval.diagnostics.mismatch_win_lofo [data/raw_preds.json]
"""
import sys
RAW = sys.argv[1] if len(sys.argv) > 1 else "data/raw_preds.json"
import json
from engine.eval.calibration_maps import apply_points, fit_pava_map, fit_league_logit_adjustments, apply_logit_adjustment
P=[dict(p,actual=float(p['actual'])) for p in json.load(open(RAW)) if p.get('actual') is not None and p['model']=='mismatch']
win=lambda p:p['market'].endswith(' Win')
fp=lambda rows,mb: fit_pava_map([r['predicted_prob_raw'] for r in rows],[r['actual'] for r in rows],min_bin_n=mb)
out={'live':[], 'win_map':[]}
for k in range(1,10):
    tr=[p for p in P if p['fold']!=k]; te=[p for p in P if p['fold']==k and win(p) and p['predicted_prob_raw']>=0.70]
    if not te: continue
    pooled=fp(tr,50); adj=fit_league_logit_adjustments(tr,pooled,min_n=30,prior_strength=200)
    wm=fp([p for p in tr if win(p)],150) or pooled
    for p in te:
        lg=str(int(p['api_league_id'])) if p.get('api_league_id') is not None else ''
        out['live'].append((apply_logit_adjustment(apply_points(pooled,p['predicted_prob_raw']),adj.get(lg)),p['actual']))
        out['win_map'].append((apply_points(wm,p['predicted_prob_raw']),p['actual']))
for k,v in out.items():
    n=len(v); s=sum(a for a,_ in v)/n; a=sum(b for _,b in v)/n; br=sum((x-y)**2 for x,y in v)/n
    print(f"  {k:8s} n={n} shown {s:5.1%} actual {a:5.1%} gap {100*(s-a):+5.1f} Brier {br:.4f}")
print('win-specific map from all mismatch win preds:', fp([p for p in P if win(p)],150))
